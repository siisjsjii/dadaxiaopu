package com.cinfly.dadaxiaopu.mq;

import com.cinfly.dadaxiaopu.config.RabbitMQConfig;
import com.cinfly.dadaxiaopu.dto.SeckillOrderMessage;
import com.cinfly.dadaxiaopu.service.ISeckillOrderFailService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 建单失败死信消费者：把失败的订单登记到 tb_seckill_order_fail 并告警
 * </p>
 * <p>
 * 这个队列与超时取消队列是<b>分开的</b>：建单失败代表「出问题了，要告警、要人工介入」，
 * 超时取消代表「正常业务流程」。混在一个队列里就得靠 x-death header 猜来源。
 * </p>
 * <p>
 * 登记完成后必须 ACK —— 消息已经落到表里了，重投只会让表里多一行
 * （而且 order_id 唯一索引也会挡）。
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Component
@Slf4j
public class SeckillOrderFailConsumer {

    @Resource
    private ISeckillOrderFailService seckillOrderFailService;

    @RabbitListener(queues = RabbitMQConfig.SECKILL_ORDER_DLX_QUEUE)
    public void onOrderCreateFailed(SeckillOrderMessage message, Message rawMessage, Channel channel,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        String reason = extractDeathReason(rawMessage);
        try {
            seckillOrderFailService.recordFail(message, reason);
        } catch (Exception e) {
            // 落表都失败了，说明数据库大概率有问题 —— 这里不能 ACK，让消息重回队列再试
            log.error("🚨【告警】建单失败消息落表失败，将重回队列。orderId={}", message.getOrderId(), e);
            channel.basicNack(deliveryTag, false, true);
            return;
        }
        channel.basicAck(deliveryTag, false);
    }

    /**
     * 从 RabbitMQ 自动维护的 x-death header 里挖出「死在哪个队列、什么原因、第几次」，
     * 这样失败表里存的原因才有排查价值，而不是一句笼统的「建单失败」。
     */
    private String extractDeathReason(Message rawMessage) {
        List<Map<String, ?>> xDeath = rawMessage.getMessageProperties().getXDeathHeader();
        if (xDeath == null || xDeath.isEmpty()) {
            return "建单失败（无 x-death 信息）";
        }
        Map<String, ?> first = xDeath.get(0);
        return String.format("建单失败：死在队列=%s, reason=%s, count=%s",
                first.get("queue"), first.get("reason"), first.get("count"));
    }
}
