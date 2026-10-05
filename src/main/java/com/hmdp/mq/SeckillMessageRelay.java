package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.config.RabbitMQConfig;
import com.hmdp.constant.RedisConstants;
import com.hmdp.dto.SeckillOrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 秒杀消息中转：把 Redis 待投递队列里的消息搬到 RabbitMQ
 * </p>
 *
 * <h3>为什么要有这一层</h3>
 * <p>
 * 秒杀热路径必须在 Redis 内闭环，绝不能碰数据库。所以「扣库存」和「记下这条待投递消息」
 * 被合进 {@code seckill.lua} 的同一条原子脚本：扣库存成功的同时把消息 LPUSH 进
 * {@code seckill:pending}。既然消息已经在 Redis 里了，就需要一个后台线程把它送进 MQ —— 就是本类。
 * </p>
 *
 * <h3>可靠队列模式（BRPOPLPUSH + 确认后删除）</h3>
 * <pre>
 *   seckill:pending ──BRPOPLPUSH──> seckill:pending:processing
 *                                          │
 *                                     convertAndSend + 等 broker 确认
 *                                          │
 *                                    确认成功 → LREM processing（真正删除）
 *                                    确认失败 → 留在 processing，下轮/重启再试
 * </pre>
 * <p>
 * 直接用 {@code BRPOP} 是错的：它「取走即删除」，若进程在取走之后、MQ 确认之前崩溃，
 * 这条消息就真的没了（而库存已经扣了）。BRPOPLPUSH 先把消息挪到 processing，
 * 只有确认投递成功才删除，崩溃时消息还留在 processing 里。
 * </p>
 *
 * <h3>怎么发现 processing 里卡住的消息</h3>
 * <p>
 * List 没有时间戳，没法判断哪条「超时」。这里用了一个更简单也更准确的判据：
 * <b>应用启动时把整个 processing 搬回 pending</b>。启动那一刻不可能有在途消息，
 * 所以里面残留的必然是上次崩溃留下的。
 * </p>
 *
 * <h3>为什么这里可以阻塞</h3>
 * <p>
 * relay 跑在自己的守护线程上，不在请求线程里。所以它可以放心地用
 * {@link CorrelationData#getFuture()} <b>同步等待 broker 确认</b> ——
 * 这正是把投递搬出热路径换来的好处：不必再维护异步回调 + 在途消息表那套复杂机制。
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Component
@Slf4j
public class SeckillMessageRelay implements RabbitTemplate.ReturnCallback {

    /**
     * 单轮投递内的重试次数
     */
    private static final int MAX_SEND_ATTEMPTS = 3;

    /**
     * 同一进程生命周期内对同一条消息的总尝试上限。
     * 超过即移入兜底队列并告警 —— 避免对一条永远发不出去的坏消息无限重试
     * （原 Redis Stream 版 handlePendingList 的 while(true) 就是这么死循环的）。
     */
    private static final int MAX_TOTAL_ATTEMPTS = 5;

    /**
     * 单轮投递内的退避基数（毫秒）：第 n 次重试前等 2^(n-1) 秒
     */
    private static final long BACKOFF_BASE_MS = 1000L;

    /**
     * BRPOPLPUSH 的阻塞超时（秒）。设小一点，便于线程及时响应停机信号。
     */
    private static final long POP_TIMEOUT_SECONDS = 1L;

    /**
     * 等待 broker 确认的超时（秒）
     */
    private static final long CONFIRM_TIMEOUT_SECONDS = 5L;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * orderId -> 本进程内已尝试投递的总次数
     */
    private final Map<Long, Integer> totalAttempts = new ConcurrentHashMap<>();

    /**
     * 被 broker 退回（路由不到任何队列）的 correlationId。
     * 这类消息 broker 是 <b>ack</b> 的，不单独标记就会被当成投递成功而误删。
     */
    private final Set<String> returnedIds = ConcurrentHashMap.newKeySet();

    private volatile boolean running = true;

    private Thread worker;

    /* ==================== 生命周期 ==================== */

    @PostConstruct
    public void init() {
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnCallback(this);
        // 先把上次崩溃遗留的在途消息搬回来，再开始消费
        recoverInFlight();
        worker = new Thread(this::loop, "seckill-mq-relay");
        worker.setDaemon(true);
        worker.start();
        log.info("秒杀消息 relay 已启动");
    }

    @PreDestroy
    public void destroy() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }

    /* ==================== 对外入口 ==================== */

    /**
     * 把消息放回待投递队列。供建单失败重试任务使用 ——
     * 这样所有「要发 MQ」的路径都统一经过 relay，MQ 的生产者就只有本类一个。
     */
    public void enqueue(SeckillOrderMessage message) {
        stringRedisTemplate.opsForList()
                .leftPush(RedisConstants.SECKILL_PENDING_KEY, JSONUtil.toJsonStr(message));
    }

    /**
     * 投递「15 分钟未支付自动取消」的延迟消息。
     * <p>
     * 这条消息走 best-effort：不进待投递队列、不等确认。丢了最坏的结果是这笔订单不会被
     * 自动取消，不影响订单本身和库存，所以不值得为它再引入一套可靠投递。
     * </p>
     */
    public void sendDelayAsync(SeckillOrderMessage message) {
        try {
            rabbitTemplate.convertAndSend("", RabbitMQConfig.SECKILL_DELAY_QUEUE, message, m -> {
                m.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                return m;
            });
        } catch (Exception e) {
            log.error("🚨【告警】延迟取消消息发送失败，该订单不会被自动取消。orderId={}", message.getOrderId(), e);
        }
    }

    /* ==================== 核心循环 ==================== */

    private void loop() {
        while (running) {
            try {
                String payload = stringRedisTemplate.opsForList().rightPopAndLeftPush(
                        RedisConstants.SECKILL_PENDING_KEY,
                        RedisConstants.SECKILL_PENDING_PROCESSING_KEY,
                        POP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (payload == null) {
                    // 队列空，超时返回，继续下一轮
                    continue;
                }
                deliver(payload);
            } catch (Exception e) {
                if (!running) {
                    // 停机时阻塞中的 BRPOPLPUSH 会被打断，这里不该打错误日志
                    break;
                }
                // Redis 抖动之类：不要把线程打挂，记日志后歇一下重来
                log.error("relay 主循环异常，1 秒后继续", e);
                sleep(1000L);
            }
        }
        log.info("秒杀消息 relay 已停止");
    }

    /**
     * 投递单条消息。成功则从 processing 删除，失败则留在 processing 等下一轮或重启恢复。
     */
    private void deliver(String payload) {
        SeckillOrderMessage message;
        try {
            message = JSONUtil.toBean(payload, SeckillOrderMessage.class);
        } catch (Exception e) {
            // 消息体是我们自己序列化进去的，正常不会解析失败。
            // 真失败了说明数据被外力改坏，重试多少次也没用 —— 直接进兜底队列。
            log.error("🚨【告警】待投递消息体无法解析，移入兜底队列。payload={}", payload, e);
            moveToDead(payload);
            return;
        }

        Long orderId = message.getOrderId();
        int total = totalAttempts.merge(orderId, 1, Integer::sum);
        if (total > MAX_TOTAL_ATTEMPTS) {
            log.error("🚨【告警】消息累计投递 {} 次仍失败，移入兜底队列等待人工处理。orderId={}", total, orderId);
            totalAttempts.remove(orderId);
            moveToDead(payload);
            return;
        }

        for (int i = 1; i <= MAX_SEND_ATTEMPTS; i++) {
            try {
                sendWithConfirm(message, orderId);
                totalAttempts.remove(orderId);
                ackProcessing(payload);
                log.debug("秒杀下单消息已投递并确认 orderId={}", orderId);
                return;
            } catch (Exception e) {
                log.warn("秒杀下单消息投递失败（第 {}/{} 次）orderId={}", i, MAX_SEND_ATTEMPTS, orderId, e);
                if (i < MAX_SEND_ATTEMPTS) {
                    sleep(BACKOFF_BASE_MS * (1L << (i - 1)));
                }
            }
        }
        // 本轮没送出去：消息留在 processing，下次应用启动时会被 recoverInFlight 搬回 pending
        log.error("🚨【告警】秒杀下单消息本轮投递未成功，保留在 processing 队列，待下次重启恢复。orderId={}", orderId);
    }

    /**
     * 投递并<b>同步等待</b> broker 确认。
     */
    private void sendWithConfirm(SeckillOrderMessage message, Long orderId) throws Exception {
        String correlationId = String.valueOf(orderId);
        CorrelationData correlationData = new CorrelationData(correlationId);
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.SECKILL_ORDER_EXCHANGE,
                RabbitMQConfig.SECKILL_ORDER_ROUTING_KEY,
                message,
                m -> {
                    m.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    m.getMessageProperties().setCorrelationId(correlationId);
                    return m;
                },
                correlationData);

        CorrelationData.Confirm confirm =
                correlationData.getFuture().get(CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!confirm.isAck()) {
            throw new IllegalStateException("broker nack: " + confirm.getReason());
        }
        // broker ack 只代表消息到达交换机；若路由不到队列，消息会被退回（而 broker 仍是 ack 的）。
        // 这种消息绝不能算成功，否则会被 LREM 掉、真的丢失。
        if (returnedIds.remove(correlationId)) {
            throw new IllegalStateException("消息被退回（路由不到任何队列），exchange="
                    + RabbitMQConfig.SECKILL_ORDER_EXCHANGE
                    + ", routingKey=" + RabbitMQConfig.SECKILL_ORDER_ROUTING_KEY);
        }
    }

    /* ==================== 队列搬运 ==================== */

    /**
     * 应用启动时把 processing 里残留的消息整体搬回 pending。
     */
    private void recoverInFlight() {
        String processingKey = RedisConstants.SECKILL_PENDING_PROCESSING_KEY;
        Long size = stringRedisTemplate.opsForList().size(processingKey);
        if (size == null || size == 0) {
            return;
        }
        long recovered = 0;
        while (recovered < size) {
            String moved = stringRedisTemplate.opsForList().rightPopAndLeftPush(
                    processingKey, RedisConstants.SECKILL_PENDING_KEY);
            if (moved == null) {
                break;
            }
            recovered++;
        }
        log.warn("🚨【告警】检测到上次运行有 {} 条正在投递的消息未完成，已全部搬回待投递队列", recovered);
    }

    private void ackProcessing(String payload) {
        stringRedisTemplate.opsForList()
                .remove(RedisConstants.SECKILL_PENDING_PROCESSING_KEY, 1, payload);
    }

    private void moveToDead(String payload) {
        stringRedisTemplate.opsForList()
                .leftPush(RedisConstants.SECKILL_PENDING_DEAD_KEY, payload);
        ackProcessing(payload);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /* ==================== 回调 ==================== */

    @Override
    public void returnedMessage(Message message, int replyCode, String replyText,
                                String exchange, String routingKey) {
        String correlationId = message.getMessageProperties().getCorrelationId();
        if (correlationId != null) {
            // 交给 sendWithConfirm 判定失败，不能让它被当成投递成功
            returnedIds.add(correlationId);
        }
        log.error("🚨【告警】秒杀下单消息路由失败（拓扑可能被破坏）：exchange={}, routingKey={}, replyCode={}, reply={}",
                exchange, routingKey, replyCode, replyText);
    }
}
