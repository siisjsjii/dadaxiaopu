package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
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
