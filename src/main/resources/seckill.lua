-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]
-- 1.4.待投递消息体（JSON），由 Java 侧构造。
--     刻意不在 Lua 里拼 JSON：手工拼字符串既易错又难读，
--     Java 侧用 Hutool 序列化更稳妥，脚本只负责「入队」这一个动作。
local payload = ARGV[4]

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key
local orderKey = 'seckill:order:' .. voucherId
-- 2.3.待投递队列key
local pendingKey = 'seckill:pending'

-- 3.脚本业务
-- 3.1.判断库存是否充足 get stockKey
--     ⚠️ 必须先判 nil 再比较：库存 key 不存在时（Redis 被清空、或该券没有预热过库存），
--     redis.call('get') 返回 false，tonumber(false) 得到 nil，
--     直接写 nil <= 0 会让【整个脚本】报错 "attempt to compare nil with number"，
--     表现为秒杀接口 500「服务器异常」而不是「库存不足」。
local stock = tonumber(redis.call('get', stockKey))
if(stock == nil or stock <= 0) then
    -- 3.2.库存不足，返回1
    return 1
end
-- 3.3.判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 3.4.存在，说明是重复下单，返回2
    return 2
end
-- 3.5.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.6.下单（保存用户）sadd orderKey userId
redis.call('sadd', orderKey, userId)
-- 3.7.【关键】把待投递消息入队，与上面三步同处一条原子脚本。
--
--    这一步取代了原先「Lua 之后再写一张 MySQL 本地消息表」的两段式做法：
--      旧做法：扣库存(原子) ──[JVM 可能崩在这个空隙]──> INSERT 消息表
--      新做法：扣库存 + 入队(原子)
--    没有中间窗口，因此也不需要「写表失败就回滚库存」那种本身也会失败的补偿逻辑。
--
--    消息由 SeckillMessageRelay 后台线程用 BRPOPLPUSH 取出并投递到 RabbitMQ，
--    投递成功（broker 确认）后才从 processing 队列里删除。
redis.call('lpush', pendingKey, payload)
return 0
