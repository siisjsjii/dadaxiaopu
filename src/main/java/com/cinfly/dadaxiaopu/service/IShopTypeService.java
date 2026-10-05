package com.cinfly.dadaxiaopu.service;

import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.entity.ShopType;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
public interface IShopTypeService extends IService<ShopType> {

    Result queryTypeList();
}
