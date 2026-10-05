package com.cinfly.dadaxiaopu.service;

import com.cinfly.dadaxiaopu.dto.SeckillOrderMessage;
import com.cinfly.dadaxiaopu.entity.SeckillOrderFail;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  秒杀建单失败表 服务类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
public interface ISeckillOrderFailService extends IService<SeckillOrderFail> {

    /**
     * 死信消费者调用：记录一条建单失败，并打告警日志。
     * 靠 order_id 唯一索引保证同一条消息重复进死信时只登记一次。
     *
     * @param message  原始消息
     * @param errorMsg 失败原因
     * @return true 表示本次新登记，false 表示这条 orderId 之前已登记过
     */
    boolean recordFail(SeckillOrderMessage message, String errorMsg);

    /**
     * 查询待重试且未超重试上限的失败记录。
     */
    List<SeckillOrderFail> listRetryable(int limit);

    /**
     * 记录一次自动重试；若已达上限则翻转为待人工审核。
     *
     * @return true 表示仍在自动重试，false 表示已转人工审核
     */
    boolean recordRetry(SeckillOrderFail fail, String errorMsg);

    /**
     * 建单终于成功时收口：把该 orderId 的失败记录标记为已解决。
     * 否则重试成功后记录会一直挂在「待重试/待人工审核」里造成误告警。
     */
    void markResolved(Long orderId);
}
