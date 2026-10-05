package com.cinfly.dadaxiaopu.mq;

import com.cinfly.dadaxiaopu.config.RabbitMQConfig;
import com.cinfly.dadaxiaopu.dto.SeckillOrderMessage;
import com.cinfly.dadaxiaopu.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * <p>
 * 超时取消消费者：消费延迟消息，取消未支付订单并回滚库存
 * </p>
 * <p>
 * 消息来源：{@code seckill.delay.queue} 里的消息 TTL 到期 → 死信到
 * {@code seckill.dlx.exchange/seckill.dlx} → 落到 {@code seckill.dlx.queue} → 本消费者。
 * </p>
 * <p>
 * <b>幂等</b>：取消动作是「带状态条件的乐观更新」（只有 status=1 未支付才更新），
 * 重复收到同一条取消消息时第二次影响行数为 0，直接返回，不会重复回滚库存。
 * </p>
 * <p>
 * 重试与拒绝同样显式处理，原因见 {@link SeckillOrderConsumer} 的类注释 ——
 * 手动 ACK 模式下容器不会替消费者 nack。
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Component
@Slf4j
public class SeckillTimeoutConsumer {

    private static final int MAX_ATTEMPTS = 3;

    private static final long BACKOFF_BASE_MS = 1000L;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = RabbitMQConfig.SECKILL_DLX_QUEUE)
    public void onTimeoutMessage(SeckillOrderMessage message, Channel channel,
                                 @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                voucherOrderService.cancelUnpaidOrder(message);
                channel.basicAck(deliveryTag, false);
                return;
            } catch (Exception e) {
                if (attempt >= MAX_ATTEMPTS) {
                    // 本队列没有再声明下一级死信，requeue=false 意味着消息就此丢弃。
                    // 所以这里必须是 error 级告警，靠人工/对账兜底，不能只记 info。
                    log.error("🚨【告警】超时取消订单重试 {} 次仍失败，消息将被丢弃，需人工核对。orderId={}, userId={}, voucherId={}",
                            MAX_ATTEMPTS, message.getOrderId(), message.getUserId(), message.getVoucherId(), e);
                    channel.basicNack(deliveryTag, false, false);
                    return;
                }
                long backoffMs = BACKOFF_BASE_MS * (1L << (attempt - 1));
                log.warn("超时取消失败（第 {}/{} 次），{}ms 后重试。orderId={}",
                        attempt, MAX_ATTEMPTS, backoffMs, message.getOrderId(), e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("🚨【告警】超时取消重试被中断，消息将被丢弃。orderId={}", message.getOrderId(), ie);
                    channel.basicNack(deliveryTag, false, false);
                    return;
                }
            }
        }
    }
}
