package com.cinfly.dadaxiaopu;

import com.cinfly.dadaxiaopu.constant.RedisConstants;
import com.cinfly.dadaxiaopu.entity.Shop;
import com.cinfly.dadaxiaopu.service.IShopService;
import com.cinfly.dadaxiaopu.service.impl.ShopServiceImpl;
import com.cinfly.dadaxiaopu.utils.RedisClient;
import com.cinfly.dadaxiaopu.utils.RedisWorker;
import org.apache.ibatis.annotations.Delete;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
public class RedisDataTest {
    @Resource
    private IShopService shopService;
    @Resource
    private RedisClient redisClient;
    @Resource
    private RedisWorker redisWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    //开启线程池
    private ExecutorService es= Executors.newFixedThreadPool(500);
    @Test
    public void redisTest()
    {
        Shop shop = shopService.getById(1L);
        shop.setName("测试");
        redisClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);
    }

    @Test
    public void workerTest() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);
        Runnable task=()->
        {
            for (int i = 0; i < 100; i++) {
                long order = redisWorker.nextId("order");
                System.out.println(order);
            }
            countDownLatch.countDown();
        };
        long begin=System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        countDownLatch.await();
        long end=System.currentTimeMillis();
        System.out.println("耗时:"+(end-begin));

    }
    @Test
    public void loadShoptest()
    {
        //查询所有店铺信息
        List<Shop> list = shopService.list();

        //把店铺分组
        Map<Long, List<Shop>>map=list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            Long typeId = entry.getKey();
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> geoLocations = value.stream().map(shop -> new RedisGeoCommands.GeoLocation<String>(shop.getId().toString(), new Point(shop.getX(), shop.getY())))
                    .collect(Collectors.toList());
            String key=RedisConstants.SHOP_GEO_KEY+typeId;
            //分批导入redis
            stringRedisTemplate.opsForGeo().add(key,geoLocations);
        }



    }
}
