package com.cinfly.dadaxiaopu.service;

import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.entity.Shop;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
public interface IShopService extends IService<Shop> {

    Result queryShopById(Long id);

    Result update(Shop shop);


    void saveShop2Redis(Long id, Long expireSeconds);

    Result queryShopByType(Integer typeId, Integer current, Double x, Double y);
}
