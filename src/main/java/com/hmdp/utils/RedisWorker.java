package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisWorker {
    private static final long BEGIN_TIMESTAMP=1767225600L;
    private final StringRedisTemplate stringRedisTemplate;
    private static final long COUNT_BITS=32;

    public RedisWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    //全局唯一id生成器
    public  long nextId(String keyPrefix)
    {
        //计算当前时间到begin时间戳的差
        LocalDateTime now = LocalDateTime.now();
        long nowseconds= now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp=nowseconds-BEGIN_TIMESTAMP;
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        String key="icr:"+keyPrefix+date;
        long count = stringRedisTemplate.opsForValue().increment(key);
        return timeStamp<<COUNT_BITS|count;
    }


    public static void main(String[] args) {
        LocalDateTime localDateTime = LocalDateTime.of(2026, 1, 1, 0, 0, 0);
        long seconds = localDateTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println(seconds);
    }
}
