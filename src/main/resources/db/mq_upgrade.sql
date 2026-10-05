-- ============================================================
-- 秒杀下单 Redis Stream -> RabbitMQ 改造：数据库变更
-- 执行方式：mysql -h127.0.0.1 -P3306 -uroot -p123456 hmdp < mq_upgrade.sql
--
-- 注意：ALTER TABLE 加唯一索引这一步不是幂等的，重复执行会报
--       "Duplicate key name 'uk_user_voucher'"，属正常现象，忽略即可。
-- ============================================================

-- ------------------------------------------------------------
-- 1. 一人一单唯一索引（本次改造最重要的防线）
--    PK(id) 只能挡住「同一条消息重复投递」（orderId 相同）；
--    挡不住「同一用户对同一张券生成两个不同 orderId」的场景，
--    例如 Redis 里 seckill:order:{id} 这个 Set 丢失后重新下单。
--    唯一索引是唯一由数据库原子保证的「一人一单」约束。
--    注意：这个索引是「终身」的 —— 订单被取消时行还在表里、索引位仍被占用，
--    所以取消后该用户不能再买同一张券。这与 Redis 侧的行为必须保持一致。
-- ------------------------------------------------------------
ALTER TABLE tb_voucher_order
    ADD UNIQUE KEY uk_user_voucher (user_id, voucher_id);


-- ------------------------------------------------------------
-- 2. 建单失败表
--    订单创建在消费者里重试耗尽后进入死信队列，
--    死信消费者把消息落到这张表并告警。
--    定时任务重试，retry_count 超过 3 次后 status 翻转为 1（待人工审核），
--    不再自动重试，等人工介入。
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tb_seckill_order_fail
(
    id              bigint        NOT NULL AUTO_INCREMENT COMMENT '主键',
    order_id        bigint        NOT NULL COMMENT '订单id',
    user_id         bigint        NOT NULL COMMENT '下单用户id',
    voucher_id      bigint        NOT NULL COMMENT '优惠券id',
    payload         varchar(512)           DEFAULT NULL COMMENT '原始消息体JSON',
    retry_count     int           NOT NULL DEFAULT 0 COMMENT '已重试次数',
    status          tinyint       NOT NULL DEFAULT 0 COMMENT '0待重试 1待人工审核 2已解决',
    error_msg       varchar(1024)          DEFAULT NULL COMMENT '最后一次失败原因',
    next_retry_time datetime               DEFAULT NULL COMMENT '下次可重试时间（退避用）',
    create_time     timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time     timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_id (order_id),
    KEY idx_status_next (status, next_retry_time)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '秒杀建单失败表：重试3次后转人工审核';


-- ------------------------------------------------------------
-- 3. 删除 tb_local_message（本地消息表）
--
--    这张表是「生产者 outbox」方案留下的：原本在秒杀热路径上先写这张表再发 MQ，
--    以堵住「Lua 扣了库存、JVM 在发送前崩溃」的窗口。
--
--    但它有两个问题：
--      a) 热路径上多一次 MySQL insert，违背本项目「秒杀热路径完全不碰数据库」的设计；
--      b) 它和 Lua 的扣库存是两段独立操作，中间仍有窗口，
--         只能靠「写表失败就回滚 Redis 库存」来补偿 —— 而补偿本身也会失败。
--
--    现改为：把 LPUSH 待投递队列直接写进 seckill.lua，与扣库存同处一条原子脚本，
--    窗口彻底消失，也不需要任何补偿逻辑。消息由 SeckillMessageRelay 后台线程
--    用 BRPOPLPUSH 搬到 RabbitMQ（确认成功才删除），相关状态全在 Redis：
--      seckill:pending            待投递
--      seckill:pending:processing 投递中（应用启动时搬回 pending）
--      seckill:pending:dead       重试超限的兜底
-- ------------------------------------------------------------
DROP TABLE IF EXISTS tb_local_message;
