package com.cinfly.dadaxiaopu.constant;

import com.sun.corba.se.impl.oa.poa.POAPolicyMediatorImpl_NR_UDS;

public class RedisConstants {
    public static final String CODE_KEY="phone:";
    public static final String USER_KEY="token:";
    public static final Long CODE_TTL=2L;
    public static final Long USER_TTL=360000L;

    public static final Long CACHE_SHOP_TTL=30L;
    public static final String CACHE_SHOP_KEY="cache:shop:";
    public static final String CACHR_SHOP_TYPE_KEY="cache:shop:type";
    public static final Long CACHE_NULL_TTL =2L ;

    public static final String LOCK_KEY="lock:";

    public static final String SECKILL_STOCK_KEY="seckill:stock:";

    /**
     * 「已下单用户」集合（Set），一人一单的 Redis 侧防线。
     * <p>
     * 注意：seckill.lua 里这个 key 是<b>硬编码的同名字面量</b>，
     * 改动这里必须同步改 seckill.lua，否则两边对不上。
     * </p>
     */
    public static final String SECKILL_ORDER_KEY="seckill:order:";

    /**
     * 秒杀待投递队列（List）。
     * <p>
     * 由 seckill.lua 在「扣库存 + 记录已购」的同一条原子脚本里 LPUSH 进来，
     * 再由 SeckillMessageRelay 后台线程用 BRPOPLPUSH 取出投递到 RabbitMQ。
     * 这是热路径上唯一的「消息落盘」动作，且完全发生在 Redis 内，不碰数据库。
     * </p>
     */
    public static final String SECKILL_PENDING_KEY = "seckill:pending";

    /**
     * 投递中队列：relay 用 {@code BRPOPLPUSH} 把消息搬到这里，
     * 等 RabbitMQ 确认后才 {@code LREM} 删除。
     * <p>
     * 应用启动时会把这里残留的消息（上一次进程崩溃时正在投递的那些）整体搬回待投递队列，
     * 这就是「可靠队列」模式里判断消息是否卡住的办法 —— 启动那一刻不可能有在途消息。
     * </p>
     */
    public static final String SECKILL_PENDING_PROCESSING_KEY = "seckill:pending:processing";

    /**
     * 兜底队列：同一进程内重试次数超过上限仍投不出去的消息会移到这里并告警，
     * 避免对一条坏消息无限重试（这是原 Redis Stream 版 handlePendingList 死循环的教训）。
     */
    public static final String SECKILL_PENDING_DEAD_KEY = "seckill:pending:dead";

    public static final String FOLLOW_KEY="follow:";

    public static final String FEEDS_KEY="feeds:";

    public static final String SHOP_GEO_KEY="shop:geo:";
    public static final String USER_SIGN_KEY = "user:sign:";
}
