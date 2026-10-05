package com.hmdp.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.constant.RedisConstants;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        String key= RedisConstants.CACHR_SHOP_TYPE_KEY;
        //查询redis缓存
        List<String> shopTypes = stringRedisTemplate.opsForList().range(key, 0, -1);
        if(CollUtil.isEmpty(shopTypes))
        {
            List<ShopType> typeList = query().orderByAsc("sort").list();
            if(CollUtil.isEmpty(typeList))
            {
                return Result.fail("店铺类型信息不存在");
            }
            List<String> shops = typeList.stream().map(JSONUtil::toJsonStr).collect(Collectors.toList());
            stringRedisTemplate.opsForList().rightPushAll(key,shops);
            return Result.ok(typeList);
        }
        List<ShopType>typeList = shopTypes.stream().map(type -> JSONUtil.toBean(type, ShopType.class)).collect(Collectors.toList());
        return Result.ok(typeList);
    }
}
