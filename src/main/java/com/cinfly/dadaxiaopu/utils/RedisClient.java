package com.cinfly.dadaxiaopu.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.cinfly.dadaxiaopu.constant.RedisConstants;
import com.cinfly.dadaxiaopu.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
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

    /**
     * 缓存重建互斥锁的持有时间（秒）。
     * <p>
     * 重建一旦超过这个时间，锁会自动过期、被别的请求重新持有 —— 这是 TTL 固有的，
     * 光靠令牌比对消不掉，只能保证「不误删别人的锁」，不能保证「同一时刻只有一个人在重建」。
     */
    private static final long LOCK_TTL_SECONDS = 10L;

    /**
     * 解锁脚本：比对令牌与锁中的值，一致才 DEL，在 Redis 内一步完成。
     * 与 {@code SimpleRedisLock} 共用同一份 {@code unlock.lua}。
     */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

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
        // 令牌在【请求线程】上生成，随闭包带进异步任务。
        // 刻意不像 SimpleRedisLock 那样用「UUID + 线程ID」：这里是跨线程解锁 ——
        // 加锁在请求线程、解锁在线程池线程，两者线程 ID 不同，
        // 解锁时会算出另一个令牌、永远比对不成功，结果锁只能等 TTL 自然过期。
        // 可靠的标识是「这一次加锁」，而不是「这一个线程」，所以随闭包传递而不是在解锁时重算。
        String token = tryLock(lockKey);
        if (token == null) {
            return;   // 没抢到锁，别人在重建，直接走
        }
        try {
            CACHE_EXECUTOR_SERVICE.submit(() -> {
                try {
                    //查询数据库,并且将逻辑过期时间加入redis
                    loadAndCache(key, id, type, query, time, timeUnit);
                } catch (Exception e) {
                    log.error("异步重建缓存失败, key={}", key, e);
                } finally {
                    unlock(lockKey, token);   // 任务跑完才解锁
                }
            });
        } catch (RejectedExecutionException e) {
            unlock(lockKey, token);           // 提交失败，补解锁，防止锁泄漏
            log.error("线程池拒绝提交任务, key={}", key, e);
        }
    }

    /**
     * 加锁，成功返回本次持有的令牌，失败返回 null。
     */
    private String tryLock(String key)
    {
        String token = UUID.randomUUID().toString(true);
        Boolean islock = stringRedisTemplate.opsForValue().setIfAbsent(key, token, LOCK_TTL_SECONDS, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(islock) ? token : null;
    }

    /**
     * 释放锁：用 Lua 把「比对令牌」和「DEL」合成一步。
     * <p>
     * 不能写成裸 {@code delete(key)}。重建耗时超过 {@link #LOCK_TTL_SECONDS} 时锁会自动过期
     * 并被别人重新持有，此时无条件 DEL 删掉的是<b>别人的锁</b>，于是又一个请求拿到锁开始重建，
     * 互斥形同虚设。令牌比对还必须是原子的 —— 先 GET 再 DEL 中间仍有窗口：
     * GET 到 DEL 之间锁可能刚好过期易主，照样删错。
     */
    private void unlock(String key, String token)
    {
        Long released = stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(key), token);
        if (released != null && released == 0L) {
            // 走到这里 = 锁已经易主或过期，说明本次重建超过了 LOCK_TTL_SECONDS
            log.warn("缓存重建锁已易主或过期，本次未释放（重建耗时超过 {}s）。key={}", LOCK_TTL_SECONDS, key);
        }
    }
}


