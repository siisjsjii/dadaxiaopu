package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.SeckillOrderFail;
import com.hmdp.mapper.SeckillOrderFailMapper;
import com.hmdp.service.ISeckillOrderFailService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <p>
 *  秒杀建单失败表 服务实现类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Service
@Slf4j
public class SeckillOrderFailServiceImpl extends ServiceImpl<SeckillOrderFailMapper, SeckillOrderFail>
        implements ISeckillOrderFailService {

    /**
     * error_msg 列是 varchar(1024)
     */
    private static final int ERROR_MSG_MAX_LEN = 1000;

    /**
     * 注意：本方法<b>刻意不加 @Transactional</b>。
     * 这里要捕获 DuplicateKeyException 并当正常分支处理，
     * 如果包在事务里，异常会把事务标记为 rollback-only，把外层调用一起带崩。
     */
    @Override
    public boolean recordFail(SeckillOrderMessage message, String errorMsg) {
        SeckillOrderFail fail = new SeckillOrderFail()
                .setOrderId(message.getOrderId())
                .setUserId(message.getUserId())
                .setVoucherId(message.getVoucherId())
                .setPayload(JSONUtil.toJsonStr(message))
                .setRetryCount(0)
                .setStatus(SeckillOrderFail.STATUS_PENDING)
                .setErrorMsg(truncate(errorMsg));
        try {
            save(fail);
        } catch (DuplicateKeyException e) {
            // 同一条消息重复进死信（比如 nack 后又重投），order_id 唯一索引挡下，不重复登记
            log.warn("[秒杀建单失败] orderId={} 已登记过，跳过重复登记", message.getOrderId());
            return false;
        }
        // 告警：这里是「需要人知道」的级别，不是普通业务日志
        log.error("🚨【告警】秒杀建单失败已进入死信兜底。orderId={}, userId={}, voucherId={}, 原因={}",
                message.getOrderId(), message.getUserId(), message.getVoucherId(), errorMsg);
        return true;
    }

    @Override
    public List<SeckillOrderFail> listRetryable(int limit) {
        return lambdaQuery()
                .eq(SeckillOrderFail::getStatus, SeckillOrderFail.STATUS_PENDING)
                .lt(SeckillOrderFail::getRetryCount, SeckillOrderFail.MAX_RETRY)
                .and(w -> w.isNull(SeckillOrderFail::getNextRetryTime)
                        .or().le(SeckillOrderFail::getNextRetryTime, LocalDateTime.now()))
                .last("LIMIT " + limit)
                .list();
    }

    @Override
    public boolean recordRetry(SeckillOrderFail fail, String errorMsg) {
        int next = fail.getRetryCount() + 1;
        boolean toManual = next >= SeckillOrderFail.MAX_RETRY;
        lambdaUpdate()
                .set(SeckillOrderFail::getRetryCount, next)
                .set(SeckillOrderFail::getErrorMsg, truncate(errorMsg))
                // 达到上限就翻转为「待人工审核」，不再自动重试
                .set(toManual, SeckillOrderFail::getStatus, SeckillOrderFail.STATUS_MANUAL)
                .set(SeckillOrderFail::getNextRetryTime,
                        toManual ? null : LocalDateTime.now().plusSeconds(1L << (next - 1)))
                .eq(SeckillOrderFail::getId, fail.getId())
                .update();
        if (toManual) {
            log.error("🚨【告警】秒杀建单失败自动重试已用尽（{} 次），转为待人工审核。orderId={}, userId={}, voucherId={}",
                    next, fail.getOrderId(), fail.getUserId(), fail.getVoucherId());
        }
        return !toManual;
    }

    @Override
    public void markResolved(Long orderId) {
        lambdaUpdate()
                .set(SeckillOrderFail::getStatus, SeckillOrderFail.STATUS_RESOLVED)
                .eq(SeckillOrderFail::getOrderId, orderId)
                .ne(SeckillOrderFail::getStatus, SeckillOrderFail.STATUS_RESOLVED)
                .update();
    }

    private String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= ERROR_MSG_MAX_LEN ? s : s.substring(0, ERROR_MSG_MAX_LEN);
    }
}
