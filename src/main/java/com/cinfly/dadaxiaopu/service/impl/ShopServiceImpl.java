package com.cinfly.dadaxiaopu.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.cinfly.dadaxiaopu.constant.RedisConstants;
import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.entity.Shop;
import com.cinfly.dadaxiaopu.mapper.ShopMapper;
import com.cinfly.dadaxiaopu.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.cinfly.dadaxiaopu.utils.RedisClient;
import com.cinfly.dadaxiaopu.utils.RedisData;
import com.cinfly.dadaxiaopu.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisClient redisClient;


    @Override
    public Result queryShopById(Long id) {
        //互斥锁解决缓存击穿
       // Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
       // Shop shop = queryWithLogicalExpire(id);
        //工具类解决缓存击穿
        Shop shop = redisClient.queryWithPathThroughAndLogicExpire(RedisConstants.CACHE_SHOP_KEY, id,
                Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop==null)
        {
            return Result.fail("商户不存在");
        }
        return Result.ok(shop);
       /* Shop shop = redisClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop==null)
        {
            return Result.fail("商户不存在");
        }
        return Result.ok(shop);*/
    }
    //线程池
 /*   private static final ExecutorService CACHE_EXECUTOR_SERVICE= Executors.newFixedThreadPool(10);
    public Shop queryWithLogicalExpire(Long id)
    {
        String key= RedisConstants.CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(shopJson))
        {
            //未命中,直接返回
            return null;
        }
        //命中,判断是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data =(JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期直接返回
        if(expireTime.isAfter(LocalDateTime.now()))
        {
            return shop;
        }
        //过期,,重建缓存
        String lockKey=RedisConstants.LOCK_KEY+id;
        if(tryLock(lockKey))
        {
            //获取锁,重建缓存
            CACHE_EXECUTOR_SERVICE.submit(()->
            {
                try {
                    this.saveShop2Redis(id,20L);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }
        return shop;
    }*/
 /*   public Shop queryWithPassThrough(Long id)
    {
        String key= RedisConstants.CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson))
        {
            //缓存命中,直接返回
            return JSONUtil.toBean(shopJson,Shop.class);
        }
        //缓存命中获取到空直接返回
        if(shopJson!=null)
        {
            return null;
        }
        Shop shop = getById(id);
        if(shop==null)
        {
            //商户不存在,将空值存入redis,避免缓存穿透
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        String jsonStr = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(key,jsonStr,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;

    }*/
/*    public Shop queryWithMutex(Long id)
    {
        //redis缓存商户数据
        String key= RedisConstants.CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(shopJson))
        {
            //缓存命中空值,直接返回
            if(shopJson!=null)
            {
                return null;
            }
            Shop shop = null;
            String lockKey=RedisConstants.LOCK_KEY+id;
            try {
                //缓存未命中,查询数据库前先获取锁,获取到就查询,没获取到就休眠一段时间再重试
                if(!tryLock(lockKey))
                {
                    Thread.sleep(50);
                    return queryWithMutex(id);
                }
                shop = getById(id);
                if(shop==null)
                {
                    //商户不存在,将空值存入redis,避免缓存穿透
                    stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                    return null;
                }
                String jsonStr = JSONUtil.toJsonStr(shop);
                //如果存在那么写入redis,设置缓存时间保证数据一致性
                stringRedisTemplate.opsForValue().set(key,jsonStr,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
                //已经写入redis,释放锁

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                unlock(lockKey);
            }
            return shop;
        }
        //缓存命中,直接返回数据
        return JSONUtil.toBean(shopJson,Shop.class);
    }*/
    //互斥锁
/*
    private boolean tryLock(String key)
    {
        Boolean islook = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(islook);
    }
    //释放锁
    private void unlock(String key)
    {
        stringRedisTemplate.delete(key);
    }
*/

    @Override
    public Result update(Shop shop) {

        Long id = shop.getId();
        if(id==null)
        {
            return Result.fail("商户id不能为空");
        }
        //先更新数据库,再删除缓存
        updateById(shop);
        String key=RedisConstants.CACHE_SHOP_KEY+id;
        stringRedisTemplate.delete(key);
        return Result.ok();
    }
    @Override
    public void saveShop2Redis(Long id, Long expireSeconds)
    {
        Shop shop = getById(id);
        String key=RedisConstants.CACHE_SHOP_KEY+id;
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.是否需要根据坐标查询
        if(x==null||y==null)
        {
            // 1. 构建查询条件（根据type_id字段查询）
            QueryWrapper<Shop> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("type_id", typeId);
            // 可选：按排序字段倒序，例如按创建时间
            // queryWrapper.orderByDesc("create_time");

            // 2. 执行分页查询（current为当前页，DEFAULT_PAGE_SIZE为每页大小）
            Page<Shop> page = new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE);
            Page<Shop> resultPage = page(page, queryWrapper);
        }
        //2.计算分页参数
        int from = (current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current*SystemConstants.DEFAULT_PAGE_SIZE;

        //查询redis,按照距离排序,分页
        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);


    }
}
