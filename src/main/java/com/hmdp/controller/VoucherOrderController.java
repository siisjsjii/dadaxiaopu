package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {
    @Resource
    private IVoucherOrderService voucherOrderService;
    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 支付订单。最小实现，只为让「15 分钟未支付自动取消」这条链路能闭环验证。
     */
    @PostMapping("pay/{orderId}")
    public Result payOrder(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.payOrder(orderId);
    }
}
