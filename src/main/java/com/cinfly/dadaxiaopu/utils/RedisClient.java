package com.cinfly.dadaxiaopu.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.cinfly.dadaxiaopu.constant.RedisConstants;
import com.cinfly.dadaxiaopu.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class RedisClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    public void set(String key, Object value, Long time, TimeUnit timeUnit)
    {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),timeUnit.toSeconds(time));
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit)
    {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }
    public  <ID,R> R queryWithPassThrough(String keyPrefix, ID id, Class<R>type, Function<ID,R> query, Long time, TimeUnit timeUnit)
    {

        String key=keyPrefix+id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(Json))
        {
            //缓存命中,直接返回
            return JSONUtil.toBean(Json,type);
        }
        //缓存命中获取到空直接返回
        if(Json!=null)
        {
            return null;
        }
        R r = query.apply(id);
        if(r==null)
        {
            //商户不存在,将空值存入redis,避免缓存穿透
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        String jsonStr = JSONUtil.toJsonStr(r);
        stringRedisTemplate.opsForValue().set(key,jsonStr,timeUnit.toSeconds(time));
        return r;
    }
    //线程池
    private static final ExecutorService CACHE_EXECUTOR_SERVICE= Executors.newFixedThreadPool(10);
    public  <ID,R> R queryWithPathThroughAndLogicExpire(String keyPrefix, ID id, Class<R>type, Function<ID,R> query, Long time, TimeUnit timeUnit)
    {
        String key= keyPrefix+id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(Json))
        {
            //命中空值,说明数据库不存在,直接返回
           if("".equals(Json))
           {
               return null;
           }
            //未命中,查数据库有没有,没有就缓存空值
            /*R r = query.apply(id);
            if(r==null)
            {
                //数据库中不存在,缓存空值
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //数据库有那么就更新缓存,设置逻辑过期时间
            this.setWithLogicalExpire(key,r,time,timeUnit);
            return r;*/
            //未命中重建缓存
            return loadAndCache(key,id,type,query,time,timeUnit);



        }
        //命中,判断是否过期
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        JSONObject data =(JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期直接返回
        if(expireTime.isAfter(LocalDateTime.now()))
        {
            return r;
        }
        //过期,,重建缓存
        String lockKey=RedisConstants.LOCK_KEY+id;
        //异步重建缓存
        rebuildWithLockAsync(lockKey,id,key,type,query,time,timeUnit);
        return r;

    }
    private <ID,R> R loadAndCache(String key,ID id, Class<R>type, Function<ID,R> query, Long time, TimeUnit timeUnit) {
        R newR = query.apply(id);
        if (newR == null) {
            stringRedisTemplate.opsForValue()
                    .set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        this.setWithLogicalExpire(key, newR, time, timeUnit);
        return newR;
    }
    private <ID,R>void rebuildWithLockAsync(String lockKey,ID id, String key, Class<R>type, Function<ID,R> query, Long time, TimeUnit timeUnit) {
        if (!tryLock(lockKey)) {
            return;   // 没抢到锁，别人在重建，直接走
        }
        try {
            CACHE_EXECUTOR_SERVICE.submit(() -> {
                try {
                    //查询数据库,并且将逻辑过期时间加入
                    loadAndCache(key, id, type, query, time, timeUnit);
                } catch (Exception e) {
                    log.error("异步重建缓存失败, key={}", key, e);
                } finally {
                    unlock(lockKey);   // 任务跑完才解锁
                }
            });
        } catch (RejectedExecutionException e) {
            unlock(lockKey);           // 提交失败，补解锁，防止锁泄漏
            log.error("线程池拒绝提交任务, key={}", key, e);
        }
    }
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
}


