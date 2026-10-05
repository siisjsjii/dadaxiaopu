package com.cinfly.dadaxiaopu.controller;


import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.entity.ShopType;
import com.cinfly.dadaxiaopu.service.IShopTypeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/shop-type")
public class ShopTypeController {
    @Resource
    private IShopTypeService typeService;

    @GetMapping("list")
    public Result queryTypeList() {

        return typeService.queryTypeList();
    }
}
