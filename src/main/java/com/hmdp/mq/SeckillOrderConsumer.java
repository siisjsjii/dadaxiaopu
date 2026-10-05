package com.hmdp.mq;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.constant.RedisConstants;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.service.ISeckillOrderFailService;
import com.hmdp.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * <p>
 * 秒杀订单消费者：手动 ACK + 界内重试 + 唯一索引幂等
 * </p>
 *
 * <h3>为什么重试和拒绝都写在消费者里，而不是靠 Spring Retry 的自动拒绝</h3>
 * <p>
 * 这是实测踩出来的坑。配置成 {@code acknowledge-mode: manual} 后，容器<b>不会</b>替消费者
 * 做 ack 或 nack —— 这本就是手动模式的定义。于是：
 * </p>
 * <pre>
 * 监听器抛异常 → Spring Retry 在容器层重试 3 次
 *              → 重试耗尽，RejectAndDontRequeueRecoverer 抛 AmqpRejectAndDontRequeueException
 *              → 但 MANUAL 模式下容器【不会】因此调用 basicNack
 *              → 消息一直 unacked → channel 关闭时重回队列 → 无限循环重投
 * </pre>
 * <p>
 * 实测现象：消息既进不了死信队列，也丢不掉，在 {@code seckill.order.queue} 里反复重投，
 * 把库存扣减 SQL 空跑了 5 次。<b>这比丢消息更糟</b>，是持续的资源浪费。
 * </p>
 * <p>
 * 所以这里显式做两件事：自己重试（{@link #MAX_ATTEMPTS} 次，指数退避），
 * 失败后自己 {@code basicNack(tag, false, false)} 拒绝且不重回队列 —— 这样才会按队列上声明的
 * {@code x-dead-letter-exchange} 进入 {@code seckill.order.dlx.queue}。
 * </p>
 *
 * <h3>异常分流</h3>
 * <ul>
 *   <li><b>DuplicateKeyException</b>：业务异常（一人一单/重复投递），<b>不重试</b>，直接 ACK；</li>
 *   <li><b>其它异常</b>：可重试，重试 {@link #MAX_ATTEMPTS} 次后拒绝进死信。</li>
 * </ul>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Component
@Slf4j
public class SeckillOrderConsumer {

    /**
     * 建单重试次数上限。与 application.yaml 里 listener.simple.retry 的配置保持一致，
     * 但重试动作实际由本类执行（原因见类注释）。
     */
    private static final int MAX_ATTEMPTS = 3;

    /**
     * 重试退避基数（毫秒）：第 n 次重试前等 2^(n-1) 秒
     */
    private static final long BACKOFF_BASE_MS = 1000L;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillOrderFailService seckillOrderFailService;

    @Resource
    private SeckillMessageRelay seckillMessageRelay;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @RabbitListener(queues = RabbitMQConfig.SECKILL_ORDER_QUEUE)
    public void onSeckillOrder(SeckillOrderMessage message, Channel channel,
                               @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        try {
            createOrderWithRetry(message);
        } catch (DuplicateKeyException e) {
            // 唯一索引挡下了重复下单 —— 预期内的正常分支，不重试，直接 ACK
            handleDuplicate(message);
            channel.basicAck(deliveryTag, false);
            return;
        } catch (Exception e) {
            log.error("🚨【告警】建单重试 {} 次仍失败，拒绝消息转入死信队列。orderId={}, userId={}, voucherId={}",
                    MAX_ATTEMPTS, message.getOrderId(), message.getUserId(), message.getVoucherId(), e);
            // requeue=false：不重回原队列，按 x-dead-letter-exchange 进 seckill.order.dlx.queue
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        // 建单成功：重试成功的记录要收口，避免失败表里留僵尸行
        try {
            seckillOrderFailService.markResolved(message.getOrderId());
        } catch (Exception e) {
            log.warn("收口建单失败记录时出错（不影响下单）orderId={}", message.getOrderId(), e);
        }

        // 投递延迟消息，用于「15 分钟未支付自动取消」。
        // 放在建单成功之后，且异常不外抛：延迟消息发失败的最坏结果是这笔订单不会被自动取消，
        // 不应该因此让整条订单消息重投、导致重复建单。
        try {
            seckillMessageRelay.sendDelayAsync(message);
        } catch (Exception e) {
            log.error("🚨【告警】延迟取消消息投递失败，该订单不会被自动取消。orderId={}", message.getOrderId(), e);
        }

        channel.basicAck(deliveryTag, false);
    }

    /**
     * 界内重试建单。DuplicateKeyException 属于业务结果而非故障，直接向上抛、不消耗重试次数。
     */
    private void createOrderWithRetry(SeckillOrderMessage message) {
        for (int attempt = 1; ; attempt++) {
            try {
                voucherOrderService.createOrderFromMq(message);
                return;
            } catch (DuplicateKeyException e) {
                throw e;
            } catch (Exception e) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
                }
                long backoffMs = BACKOFF_BASE_MS * (1L << (attempt - 1));
                log.warn("建单失败（第 {}/{} 次），{}ms 后重试。orderId={}",
                        attempt, MAX_ATTEMPTS, backoffMs, message.getOrderId(), e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("重试被中断", ie);
                }
            }
        }
    }

    /**
     * 区分两种都表现为 DuplicateKeyException、但处理方式完全相反的情况：
     * <ol>
     *   <li><b>同一条消息被重复投递</b>（撞主键 id）：Redis 侧只扣过一次库存，
     *       <b>绝不能</b>回滚，否则库存会被凭空放大；</li>
     *   <li><b>同一用户对同一张券生成了新的 orderId</b>（撞 uk_user_voucher）：
     *       说明 Redis 的「已购 Set」失效过（例如 Redis 重启丢数据）、库存被多扣了一次，
     *       <b>必须</b>回滚库存，并把该用户重新标记为已购。</li>
     * </ol>
     * 判据：订单表里有没有这个 orderId —— 有就是情况 1，没有就是情况 2。
     */
    private void handleDuplicate(SeckillOrderMessage message) {
        Long orderId = message.getOrderId();
        if (voucherOrderService.getById(orderId) != null) {
            log.info("秒杀订单消息重复投递，已幂等丢弃。orderId={}", orderId);
            return;
        }
        log.warn("检测到同一用户重复下单，回滚多扣的库存并恢复已购标记。orderId={}, userId={}, voucherId={}",
                orderId, message.getUserId(), message.getVoucherId());
        String voucherId = message.getVoucherId().toString();
        // 库存被多扣了一次，补回来
        stringRedisTemplate.opsForValue().increment(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        // 这里必须是 SADD 而不是 SREM：DB 里这个人确实有订单，
        // 重新标记为「已购」才能让 Redis 与数据库的事实一致，否则他会一直抢、一直被拒。
        stringRedisTemplate.opsForSet()
                .add(RedisConstants.SECKILL_ORDER_KEY + voucherId, message.getUserId().toString());
    }
}
