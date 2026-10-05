package com.cinfly.dadaxiaopu.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * <p>
 * 秒杀下单消息体 —— 在 MQ 上传输的就是这个对象。
 * </p>
 * <p>
 * 三个字段都必须是「业务上可重复投递而不出错」的：orderId 是 RedisWorker 生成的全局唯一 id，
 * 它同时也是 tb_voucher_order 的主键，所以同一条消息被重复投递时会撞主键
 * （DuplicateKeyException），这就是消费端的第一道幂等防线；
 * 而「一人一单」这条业务规则靠 tb_voucher_order 的 uk_user_voucher 唯一索引兜底。
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class SeckillOrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 订单id（RedisWorker 生成的全局唯一 id，即 tb_voucher_order.id）
     */
    private Long orderId;

    /**
     * 下单用户id
     */
    private Long userId;

    /**
     * 优惠券id
     */
    private Long voucherId;
}
