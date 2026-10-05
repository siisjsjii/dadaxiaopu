package com.cinfly.dadaxiaopu.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <p>
 * RabbitMQ 拓扑配置：秒杀下单异步化
 * </p>
 *
 * <pre>
 *  ┌──────────────────────────────────────────────────────────────────┐
 *  │ 正常下单链路                                                      │
 *  │                                                                  │
 *  │  seckill.order.exchange (Direct)                                 │
 *  │        │ rk = seckill.order                                      │
 *  │        ▼                                                         │
 *  │  seckill.order.queue ──[建单失败/重试耗尽]──┐                     │
 *  └────────────────────────────────────────────┼─────────────────────┘
 *                                               ▼
 *                          seckill.order.dlx.exchange (Direct)
 *                                               │ rk = seckill.order.dlx
 *                                               ▼
 *                          seckill.order.dlx.queue ──> 建单失败兜底 + 告警
 *
 *  ┌──────────────────────────────────────────────────────────────────┐
 *  │ 15 分钟未支付自动取消链路                                          │
 *  │                                                                  │
 *  │  seckill.delay.queue  (TTL=15min，故意不设消费者，消息滞留至过期)   │
 *  │        │ 队头消息 TTL 到期                                        │
 *  │        ▼                                                         │
 *  │  seckill.dlx.exchange (Direct)                                   │
 *  │        │ rk = seckill.dlx                                        │
 *  │        ▼                                                         │
 *  │  seckill.dlx.queue ──> 超时取消消费者（未支付→取消+回滚库存）        │
 *  └──────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>
 * 两条链路用**两组独立的死信交换机/队列**，语义不混：
 * 建单失败是「出问题了，要告警要人工介入」，超时取消是「正常业务流程」。
 * 如果共用一个死信队列，消费者就得靠 x-death header 去猜消息从哪来。
 * </p>
 *
 * <p>
 * <b>TTL 方案的已知限制</b>：RabbitMQ 的队列级 TTL 是「队头过期才投递死信」，
 * 所以如果将来需要多种延迟时长（比如 15 分钟 / 30 分钟），必须一档 TTL 一个队列，
 * 或者改用延迟插件。当前场景所有延迟消息 TTL 一致，队列级 TTL 是安全的。
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Configuration
public class RabbitMQConfig {

    /* ==================== 交换机 ==================== */

    /**
     * 秒杀订单交换机（Direct）
     */
    public static final String SECKILL_ORDER_EXCHANGE = "seckill.order.exchange";

    /**
     * 建单失败死信交换机（Direct）
     */
    public static final String SECKILL_ORDER_DLX_EXCHANGE = "seckill.order.dlx.exchange";

    /**
     * 超时取消死信交换机（Direct）
     */
    public static final String SECKILL_DLX_EXCHANGE = "seckill.dlx.exchange";

    /* ==================== 队列 ==================== */

    /**
     * 秒杀订单队列
     */
    public static final String SECKILL_ORDER_QUEUE = "seckill.order.queue";

    /**
     * 建单失败死信队列
     */
    public static final String SECKILL_ORDER_DLX_QUEUE = "seckill.order.dlx.queue";

    /**
     * 超时取消死信队列
     */
    public static final String SECKILL_DLX_QUEUE = "seckill.dlx.queue";

    /**
     * 延迟队列（无消费者，消息在此滞留到 TTL 到期后转投超时取消死信交换机）
     */
    public static final String SECKILL_DELAY_QUEUE = "seckill.delay.queue";

    /* ==================== routingKey ==================== */

    public static final String SECKILL_ORDER_ROUTING_KEY = "seckill.order";

    public static final String SECKILL_ORDER_DLX_ROUTING_KEY = "seckill.order.dlx";

    public static final String SECKILL_DLX_ROUTING_KEY = "seckill.dlx";

    /**
     * 延迟队列 TTL：15 分钟。
     * <p>
     * 端到端验证时可临时改成 30000（30 秒）观察死信链路是否联通，
     * 验完必须改回 900000。<b>注意</b>：改 TTL 后必须先把
     * seckill.delay.queue 删掉再重启应用，否则 RabbitMQ 会以
     * PRECONDITION_FAILED 拒绝声明参数不一致的队列。
     * </p>
     */
    public static final int SECKILL_DELAY_TTL = 15 * 60 * 1000;

    /* ==================== 交换机 Bean ==================== */

    @Bean
    public DirectExchange seckillOrderExchange() {
        return new DirectExchange(SECKILL_ORDER_EXCHANGE, true, false);
    }

    @Bean
    public DirectExchange seckillOrderDlxExchange() {
        return new DirectExchange(SECKILL_ORDER_DLX_EXCHANGE, true, false);
    }

    @Bean
    public DirectExchange seckillDlxExchange() {
        return new DirectExchange(SECKILL_DLX_EXCHANGE, true, false);
    }

    /* ==================== 队列 Bean ==================== */

    /**
     * 秒杀订单队列：建单失败/被拒绝/队列满时，消息转入建单失败死信交换机。
     */
    @Bean
    public Queue seckillOrderQueue() {
        return QueueBuilder.durable(SECKILL_ORDER_QUEUE)
                .deadLetterExchange(SECKILL_ORDER_DLX_EXCHANGE)
                .deadLetterRoutingKey(SECKILL_ORDER_DLX_ROUTING_KEY)
                .build();
    }

    /**
     * 建单失败死信队列：不设 TTL，消息一直留到被消费。
     */
    @Bean
    public Queue seckillOrderDlxQueue() {
        return QueueBuilder.durable(SECKILL_ORDER_DLX_QUEUE).build();
    }

    /**
     * 超时取消死信队列：15 分钟延迟消息过期后落到这里被消费。
     */
    @Bean
    public Queue seckillDlxQueue() {
        return QueueBuilder.durable(SECKILL_DLX_QUEUE).build();
    }

    /**
     * 延迟队列：<b>故意不声明任何消费者</b>，消息进来后原地滞留，
     * 队头消息 TTL 到期即被投递到超时取消死信交换机。
     */
    @Bean
    public Queue seckillDelayQueue() {
        return QueueBuilder.durable(SECKILL_DELAY_QUEUE)
                .ttl(SECKILL_DELAY_TTL)
                .deadLetterExchange(SECKILL_DLX_EXCHANGE)
                .deadLetterRoutingKey(SECKILL_DLX_ROUTING_KEY)
                .build();
    }

    /* ==================== 绑定 ==================== */

    @Bean
    public Binding seckillOrderBinding() {
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(SECKILL_ORDER_ROUTING_KEY);
    }

    @Bean
    public Binding seckillOrderDlxBinding() {
        return BindingBuilder.bind(seckillOrderDlxQueue())
                .to(seckillOrderDlxExchange())
                .with(SECKILL_ORDER_DLX_ROUTING_KEY);
    }

    @Bean
    public Binding seckillDlxBinding() {
        return BindingBuilder.bind(seckillDlxQueue())
                .to(seckillDlxExchange())
                .with(SECKILL_DLX_ROUTING_KEY);
    }

    /* ==================== 消息转换器 ==================== */

    /**
     * JSON 序列化。Spring Boot 的自动配置会把容器里唯一的 MessageConverter
     * 同时装配到 RabbitTemplate 和 @RabbitListener 上，所以这一处声明两边都生效。
     */
    @Bean
    public MessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
