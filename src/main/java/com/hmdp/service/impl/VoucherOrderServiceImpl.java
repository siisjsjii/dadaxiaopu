package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.constant.RedisConstants;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.print.attribute.standard.PrinterURI;
import javax.xml.bind.PrintConversionEvent;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * <h3>秒杀下单链路的演进（每一代都保留在下面，可对照）</h3>
 * <ol>
 *   <li>同步 + synchronized / Redisson 分布式锁 —— 见文末注释块</li>
 *   <li>Lua 原子扣减 + 阻塞队列异步落库 —— 见文末注释块</li>
 *   <li>Lua 原子扣减 + <b>Redis Stream</b> 异步落库 —— 见文末 // 注释块（已被取代）</li>
 *   <li><b>当前生效</b>：Lua 原子扣减 + 本地消息表(outbox) + <b>RabbitMQ</b> 异步落库，
 *       配发布确认、手动 ACK、两级死信、15 分钟延迟取消</li>
 * </ol>
 *
 * @author cinfly
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisWorker redisWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    /* ==================== 订单状态（对应 tb_voucher_order.status） ==================== */

    /**
     * 未支付
     */
    private static final int ORDER_STATUS_UNPAID = 1;
    /**
     * 已支付
     */
    private static final int ORDER_STATUS_PAID = 2;
    /**
     * 已取消
     */
    private static final int ORDER_STATUS_CANCELLED = 4;

    /* ==================== Lua 脚本 ==================== */

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /* ==================== 秒杀入口（当前生效：Lua + outbox + RabbitMQ） ==================== */

    /**
     * 秒杀下单。
     * <p>
     * 热路径只有 <b>一次 Redis 往返</b>：校验时间窗 → 生成订单号 → Lua（扣库存 + 一人一单 + 消息入队）
     * → 立即返回。<b>全程不碰数据库、不发 MQ。</b>
     * </p>
     * <p>
     * 消息入队和扣库存同处一条 Lua，所以「库存扣了但消息没记下」这个窗口根本不存在，
     * 也就不需要任何补偿逻辑。消息由 {@code SeckillMessageRelay} 后台线程搬到 RabbitMQ，
     * 真正落库由 {@code SeckillOrderConsumer} 异步完成，用户侧零等待。
     * </p>
     * <p>
     * 与 Stream 版本相比，最大的变化是 <b>不再需要 AopContext.currentProxy()</b>：
     * MQ 消费者是一个独立的 Bean，它注入的 {@code IVoucherOrderService} 本身就是代理对象，
     * 自调用问题自然消失（原来那个「proxy 赋值在 XADD 之后导致首单 NPE」的坑也随之不存在了）。
     * </p>
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 查询优惠券，校验秒杀时间窗
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券不存在");
        }
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束");
        }

        // 2. 生成全局唯一订单号，并构造待投递消息体。
        //    JSON 在 Java 侧序列化（Lua 里手工拼字符串易错），脚本只负责把它入队。
        Long userId = UserHolder.getUser().getId();
        long orderId = redisWorker.nextId("order:");
        String payload = JSONUtil.toJsonStr(new SeckillOrderMessage(orderId, userId, voucherId));

        // 3. 执行 Lua —— 判库存 → 一人一单 → 扣库存 → 记录已购 → 消息 LPUSH 进待投递队列，
        //    五件事在一条脚本里原子完成。
        int result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
                        voucherId.toString(), userId.toString(), String.valueOf(orderId), payload)
                .intValue();
        if (result != 0) {
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }

        // 4. 立即返回订单号，用户侧零等待
        return Result.ok(orderId);
    }

    /* ==================== 消费者回调 ==================== */

    /**
     * 由 {@code SeckillOrderConsumer} 调用：真正把订单落库。
     * <p>
     * 与旧的 {@link #createOrder(VoucherOrder)} 最大的区别：<b>不再先 count() 查一遍</b>。
     * 那个「先查再插」是非原子的，在 MQ 并发消费下会漏。
     * 现在直接插，让 {@code uk_user_voucher} 唯一索引在数据库层原子兜底，
     * 冲突就抛 {@code DuplicateKeyException}，由消费者按业务异常处理（不重试、直接 ACK）。
     * </p>
     * <p>
     * 抛异常语义约定：
     * <ul>
     *   <li>{@code DuplicateKeyException} —— 重复下单/重复投递，业务异常，不重试</li>
     *   <li>其它异常 —— 可重试，交给 Spring Retry</li>
     * </ul>
     * </p>
     */
    @Override
    @Transactional
    public void createOrderFromMq(SeckillOrderMessage message) {
        // 乐观条件扣减：只有 stock > 0 才扣得动
        boolean deducted = seckillVoucherService.lambdaUpdate()
                .setSql("stock = stock - 1")
                .eq(SeckillVoucher::getVoucherId, message.getVoucherId())
                .gt(SeckillVoucher::getStock, 0)
                .update();
        if (!deducted) {
            // Redis 说有库存、DB 说没有 —— 两边不一致，属于必须人工介入的严重情况，
            // 抛出去让消息进死信并告警，绝不能静默 ACK 丢掉。
            throw new IllegalStateException(
                    "DB 库存不足，与 Redis 不一致。voucherId=" + message.getVoucherId());
        }

        VoucherOrder order = new VoucherOrder()
                .setId(message.getOrderId())
                .setUserId(message.getUserId())
                .setVoucherId(message.getVoucherId())
                .setPayType(1)
                .setStatus(ORDER_STATUS_UNPAID);
        // 撞主键(同一条消息重复投递) 或 撞 uk_user_voucher(一人一单) 都会抛 DuplicateKeyException，
        // 事务整体回滚，上面的库存扣减一并撤销
        save(order);
    }

    /**
     * 15 分钟未支付自动取消。由 {@code SeckillTimeoutConsumer} 调用。
     * <p>
     * <b>幂等</b>：取消是「带状态条件的乐观更新」，只有 status=1（未支付）才会被改成 4（已取消）。
     * 重复收到同一条取消消息时，第二次影响行数为 0，直接返回，不会重复回滚库存。
     * </p>
     */
    @Override
    @Transactional
    public void cancelUnpaidOrder(SeckillOrderMessage message) {
        boolean cancelled = lambdaUpdate()
                .set(VoucherOrder::getStatus, ORDER_STATUS_CANCELLED)
                .eq(VoucherOrder::getId, message.getOrderId())
                .eq(VoucherOrder::getStatus, ORDER_STATUS_UNPAID)
                .update();
        if (!cancelled) {
            // 已支付 / 已取消 / 订单不存在 —— 都是正常情况，直接返回
            log.info("订单无需取消（可能已支付或已取消）。orderId={}", message.getOrderId());
            return;
        }

        // 回滚 DB 库存
        seckillVoucherService.lambdaUpdate()
                .setSql("stock = stock + 1")
                .eq(SeckillVoucher::getVoucherId, message.getVoucherId())
                .update();
        // 只回滚库存，**不**清「已购 Set」—— 让别的用户能抢到退回的库存，
        // 但下过单的这个用户不能再抢（与 uk_user_voucher 的语义保持一致）
        rollbackRedisStockOnly(message.getVoucherId());

        log.info("订单超时未支付，已取消并回滚库存。orderId={}, userId={}, voucherId={}",
                message.getOrderId(), message.getUserId(), message.getVoucherId());
    }

    /**
     * 支付订单（最小实现，为了让「15 分钟未支付自动取消」这条链路可验证、可闭环）。
     */
    @Override
    public Result payOrder(Long orderId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return Result.fail("订单不存在");
        }
        if (!order.getUserId().equals(userId)) {
            return Result.fail("无权操作该订单");
        }
        if (ORDER_STATUS_PAID == order.getStatus()) {
            // 幂等：重复支付直接当成功
            return Result.ok();
        }
        boolean paid = lambdaUpdate()
                .set(VoucherOrder::getStatus, ORDER_STATUS_PAID)
                .set(VoucherOrder::getPayTime, LocalDateTime.now())
                .eq(VoucherOrder::getId, orderId)
                .eq(VoucherOrder::getStatus, ORDER_STATUS_UNPAID)
                .update();
        if (!paid) {
            return Result.fail("订单当前状态不可支付");
        }
        return Result.ok();
    }

    /* ==================== 内部工具 ==================== */

    /**
     * 只回滚 Redis 库存，<b>保留</b>「已购 Set」里的标记。
     * <p>
     * 用于「订单确实已经存在」的场景（超时取消、重复请求）。
     * 之所以不清已购标记：一人一单是<b>终身</b>的 —— 订单被取消时行还在表里，
     * {@code uk_user_voucher} 索引位仍然被占着，DB 侧不可能再接受同一 (user, voucher)。
     * 如果这里把标记清了，就会出现「Redis 放行、DB 必然拒绝」的两边打架，
     * 表现为用户反复抢、库存被反复扣又回滚，最终什么也拿不到。
     * </p>
     */
    private void rollbackRedisStockOnly(Long voucherId) {
        stringRedisTemplate.opsForValue()
                .increment(RedisConstants.SECKILL_STOCK_KEY + voucherId.toString());
    }

    /* ==================== 以下为旧的同步建单方法，供文末注释块对照 ==================== */

    @Transactional
    public Result createOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        if (lambdaQuery().eq(VoucherOrder::getUserId, userId).eq(VoucherOrder::getVoucherId, voucherId).count() > 0) {
            return Result.fail("该用户已经买过一次");
        }
        //扣减库存,生成订单
        boolean update = seckillVoucherService.lambdaUpdate().
                setSql("stock = stock - 1").eq(SeckillVoucher::getVoucherId, voucherId).
                gt(SeckillVoucher::getStock, 0).update();
        if (!update) {
            return Result.fail("库存不足");
        }
        //订单id,用户id,优惠卷id
        save(voucherOrder);
        return Result.ok(voucherOrder.getId());
    }


// ============================================================================================
// 【方案对照 · 已停用】以下为「Lua + Redis Stream 异步落库」的完整实现，已被 RabbitMQ 方案取代。
// 刻意保留，用于对照两代消息队列方案的差异，请勿删除。
//
// 保留原因：
//   1. Stream 是 Redis 自带的轻量队列，无需额外中间件；RabbitMQ 则是独立的专业消息中间件，
//      换来的是发布确认、死信、延迟、手动 ACK 这类生产能力。
//   2. Stream 版本有两个真实缺陷，正好是这次改造要解决的：
//      - proxy = AopContext.currentProxy() 赋值在 XADD 之后，消费者可能抢先拿到消息导致首单 NPE；
//      - handlePendingList 的 while(true) 在 createOrder 持续失败时会无限重投同一条 pending 消息。
// ============================================================================================

//    //代理对象
//    private IVoucherOrderService proxy;
//
//    //异步处理线程池
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//
//    private volatile boolean running = true;
//
//    @PostConstruct
//    public void init() {
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//
//    @PreDestroy
//    public void destroy() {
//        log.info("正在关闭秒杀订单处理线程...");
//        running = false;
//        SECKILL_ORDER_EXECUTOR.shutdown();
//        try {
//            if (!SECKILL_ORDER_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
//                log.warn("等待超时，强制关闭线程池");
//                SECKILL_ORDER_EXECUTOR.shutdownNow();
//            }
//        } catch (InterruptedException e) {
//            log.error("关闭线程池异常", e);
//            SECKILL_ORDER_EXECUTOR.shutdownNow();
//            Thread.currentThread().interrupt();
//        }
//        log.info("秒杀订单处理线程已关闭");
//    }
//
//    //编写内部类
//    private class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//            while (running) {
//                try {
//                   //获取消息队列的订单信息
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
//                    );
//
//                    //判断消息获取是否成功
//                    //失败,说明没有消息,继续下一个循环
//                    if(CollUtil.isEmpty(list))
//                    {
//                        continue;
//                    }
//                    //解析list封装
//                    //创建订单
//                    MapRecord<String, Object, Object> map = list.get(0);
//                    Map<Object, Object> values = map.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                    //下单,使用代理对象
//                    proxy.createOrder(voucherOrder);
//
//                    //ack确认
//                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",map.getId());
//
//
//                } catch (Exception e) {
//                    //没有处理,处理pendinglist里面的消息
//                    log.error("获取订单失败，错误信息：{}", e.getMessage(), e);
//                    handlePendingList();
//                }
//
//            }
//        }
//
//        private void handlePendingList() {
//            while (true) {
//                try {
//                    //获取pendinglist的订单信息
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1),
//                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
//                    );
//
//                    //判断消息获取是否成功
//                    //失败,说明没有消息,结束循环
//                    if(CollUtil.isEmpty(list))
//                    {
//                        break;
//                    }
//                    //解析list封装
//                    //创建订单
//                    MapRecord<String, Object, Object> map = list.get(0);
//                    Map<Object, Object> values = map.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                    //下单
//                    proxy.createOrder(voucherOrder);
//
//                    //ack确认
//                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",map.getId());
//
//
//                } catch (Exception e) {
//                    //没有处理,处理pending list里面的订单
//                    log.error("处理pending list订单失败，错误信息：{}", e.getMessage(), e);
//                    try {
//                        Thread.sleep(2000);
//                    } catch (InterruptedException ex) {
//                        log.error("处理订单异常", ex);
//                        Thread.currentThread().interrupt();
//                    }
//                }
//
//            }
//        }
//        //阻塞队列
//    /*    private  BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//
//        private void handleVoucherOrder(VoucherOrder voucherOrder) {
//            //1.获取用户
//            Long userId = voucherOrder.getUserId();
//            // 2.创建锁对象
//            RLock redisLock = redissonClient.getLock("lock:order:" + userId);
//            // 3.尝试获取锁
//            boolean isLock = redisLock.tryLock();
//            // 4.判断是否获得锁成功
//            if (!isLock) {
//                // 获取锁失败，直接返回失败或者重试
//                log.error("不允许重复下单！");
//                return;
//            }
//            try {
//                // ✅ 检查代理是否存在
//                if (proxy == null) {
//                    log.error("代理对象为空，无法创建订单");
//                    // 重新放入队列，稍后重试
//                    orderTasks.add(voucherOrder);
//                    return;
//                }
//                proxy.createOrder(voucherOrder);
//            } finally {
//                // 释放锁
//                redisLock.unlock();
//            }
//        }*/
//    }
//
//    //基于stream消息队列实现异步秒杀
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //查询优惠卷信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //查询秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已结束");
//        }
//        //执行lua脚本
//        Long userId = UserHolder.getUser().getId();
//        long orderId = redisWorker.nextId("order:");
//        int result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(),
//                        userId.toString(),String.valueOf(orderId))
//                .intValue();
//        if (result != 0) {
//            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
//        }
//        //已经加入到stream队列了
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//
//        //返回订单id
//
//        return Result.ok(orderId);
//    }

        //基于阻塞队列实现异步秒杀
      /*  @Override
        public Result seckillVoucher(Long voucherId) {
            //查询优惠卷信息
            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
            //查询秒杀是否开始
            if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
                return Result.fail("秒杀尚未开始");
            }
            if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
                return Result.fail("秒杀已结束");
            }
            //执行lua脚本
            Long userId = UserHolder.getUser().getId();
            long orderId = redisWorker.nextId("order:");
            int result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(),
                            userId.toString())
                    .intValue();
            if (result != 0) {
                return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
            }

            //如果结果为0有购买资格,把下单信息加入到阻塞队列
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setVoucherId(voucherId);
            voucherOrder.setId(orderId);
            voucherOrder.setUserId(userId);
            //放入阻塞队列
            orderTasks.add(voucherOrder);
            proxy = (IVoucherOrderService) AopContext.currentProxy();

            //返回订单id

            return Result.ok(orderId);
        }*/

        /* @Override
         public Result seckillVoucher(Long voucherId) {
             //查询优惠卷信息
             SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
             //查询秒杀是否开始

             if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
                 return Result.fail("秒杀尚未开始");
             }
             if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
                 return Result.fail("秒杀已结束");
             }
             //判断有无库存,无库存返回错误信息
             Integer stock = voucher.getStock();
             if (stock < 1) {
                 return Result.fail("库存不足");
             }
             //一人一单逻辑需要加锁
           *//*  Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()) {
            //获取代理对象
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createOrder(voucherId);
        }*//*
        //redis分布式锁实现
      *//*  Long userId = UserHolder.getUser().getId();
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
        boolean success = simpleRedisLock.tryLock(1200L);*//*
        //基于redisson实现分布式锁
        Long userId = UserHolder.getUser().getId();
        RLock lock = redissonClient.getLock("lock:order:"+userId);
        try {
            boolean success = lock.tryLock();
            if(!success) {
                return Result.fail("一人一单");
            }

            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createOrder(voucherId);

        } finally {
            lock.unlock();
        }

    }*/

}
