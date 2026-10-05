package com.hmdp.mq;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.SeckillOrderFail;
import com.hmdp.service.ISeckillOrderFailService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 * 建单失败自动重试任务
 * </p>
 * <p>
 * 把 tb_seckill_order_fail 里「待重试」的记录重新投回订单交换机，再走一遍建单。
 * 每次重投都会累加 retry_count，累计到
 * {@link SeckillOrderFail#MAX_RETRY} 次后 status 翻转为
 * 「待人工审核」，不再自动重试 —— 避免对一条永远失败的坏消息无限重投
 * （这正是原来 Redis Stream 版本 handlePendingList 死循环的教训）。
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Component
@Slf4j
public class SeckillOrderFailRetryJob {

    private static final int BATCH_SIZE = 50;

    @Resource
    private ISeckillOrderFailService seckillOrderFailService;

    @Resource
    private SeckillMessageRelay seckillMessageRelay;

    @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
    public void retryFailedOrders() {
        List<SeckillOrderFail> retryable = seckillOrderFailService.listRetryable(BATCH_SIZE);
        if (CollUtil.isEmpty(retryable)) {
            return;
        }
        log.info("建单失败自动重试：本轮 {} 条", retryable.size());
        for (SeckillOrderFail fail : retryable) {
            try {
                SeckillOrderMessage message =
                        JSONUtil.toBean(fail.getPayload(), SeckillOrderMessage.class);
                // 先累加重试次数；返回 false 说明这一轮用尽了额度，已翻转为待人工审核tb_blog_comments
                boolean stillAutoRetry = seckillOrderFailService.recordRetry(fail, null);
                // 走 relay 统一入队，而不是直接发 MQ —— 这样「往 MQ 发消息」这件事
                // 全项目只有一个出口，投递可靠性只需要在 SeckillMessageRelay 一处保证
                seckillMessageRelay.enqueue(message);
                if (!stillAutoRetry) {
                    log.error("🚨【告警】orderId={} 建单自动重试已达上限，已转为待人工审核，本轮为最后一次投递",
                            fail.getOrderId());
                }
            } catch (Exception e) {
                log.error("🚨【告警】建单失败重投出错。orderId={}", fail.getOrderId(), e);
                seckillOrderFailService.recordRetry(fail, e.getMessage());
            }
        }
    }
}
