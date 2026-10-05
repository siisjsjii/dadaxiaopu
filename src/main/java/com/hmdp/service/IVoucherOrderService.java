package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result createOrder(VoucherOrder voucherOrder);

    /**
     * 由 MQ 消费者调用：真正把订单落库（乐观扣减 DB 库存 + insert，靠唯一索引保证一人一单）。
     */
    void createOrderFromMq(SeckillOrderMessage message);

    /**
     * 15 分钟未支付自动取消：取消未支付订单并回滚 Redis + DB 库存。
     */
    void cancelUnpaidOrder(SeckillOrderMessage message);

    /**
     * 支付订单。
     */
    Result payOrder(Long orderId);
}
