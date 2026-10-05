package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * <p>
 * 秒杀建单失败表（消费者侧）
 * </p>
 * <p>
 * 消费者建单重试耗尽后，消息被拒绝并进入 seckill.order.dlx.queue，
 * 死信消费者把消息落到这张表并打告警日志。
 * 定时任务会重试 status=0 的行，retry_count 超过上限
 * （{@link #MAX_RETRY}）后把 status 翻转为 STATUS_MANUAL（待人工审核），
 * 不再自动重试，等人工介入。
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_seckill_order_fail")
public class SeckillOrderFail implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待重试
     */
    public static final int STATUS_PENDING = 0;
    /**
     * 待人工审核（自动重试次数已用尽）
     */
    public static final int STATUS_MANUAL = 1;
    /**
     * 已解决
     */
    public static final int STATUS_RESOLVED = 2;

    /**
     * 自动重试次数上限，超过即转人工审核
     */
    public static final int MAX_RETRY = 3;

    /**
     * 主键
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 订单id（唯一索引，防同一条消息重复落失败表）
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

    /**
     * 原始消息体 JSON
     */
    private String payload;

    /**
     * 已重试次数
     */
    private Integer retryCount;

    /**
     * 0待重试 1待人工审核 2已解决
     */
    private Integer status;

    /**
     * 最后一次失败原因
     */
    private String errorMsg;

    /**
     * 下次可重试时间（退避用）
     */
    private LocalDateTime nextRetryTime;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
}
