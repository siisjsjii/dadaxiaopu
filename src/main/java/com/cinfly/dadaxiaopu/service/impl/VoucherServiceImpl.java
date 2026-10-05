package com.cinfly.dadaxiaopu.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.cinfly.dadaxiaopu.constant.RedisConstants;
import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.entity.Voucher;
import com.cinfly.dadaxiaopu.mapper.VoucherMapper;
import com.cinfly.dadaxiaopu.entity.SeckillVoucher;
import com.cinfly.dadaxiaopu.service.ISeckillVoucherService;
import com.cinfly.dadaxiaopu.service.IVoucherService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        // 保存优惠券
        save(voucher);
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);
        //将优惠卷信息存入到redis
        stringRedisTemplate.opsForValue().set(RedisConstants.SECKILL_STOCK_KEY+voucher.getId(), voucher.getStock().toString());

    }
}
