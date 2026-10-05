# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概览

达达小铺（hmdp）——本地生活点评类后端 API（商户/探店笔记/关注/优惠券秒杀）。
技术栈：Spring Boot 2.3.12 + JDK 8 + MyBatis-Plus 3.4.3 + Redis(Lettuce) + RabbitMQ(spring-amqp 2.2.18) + Redisson + Hutool + Knife4j。

> spring-amqp 版本由 Boot 2.3.12 管理为 **2.2.18**，只有旧的 `RabbitTemplate.ReturnCallback`；
> `ReturnsCallback`/`ReturnedMessage` 是 Spring AMQP 2.3 才引入的，用不了。

**纯 JSON API 后端，无前端静态资源。** 前端与图片由独立 nginx 提供（`D:\nginx\nginx-1.18.0\html\hmdp`，见 `SystemConstants.IMAGE_UPLOAD_DIR`），Spring 只跑在 8081。

外部依赖（缺一不可，否则启动失败）：
- MySQL `127.0.0.1:3306/hmdp`，root/123456（`application.yaml`）
- Redis `127.0.0.1:6379`。容器名 `redis`，**已开 AOF**：`redis-server --appendonly yes --appendfsync everysec`，
  数据挂在具名卷 `hmdp-redis-data` 上。**秒杀待投递队列（`seckill:pending`）的可靠性直接依赖 AOF，别把这两个参数去掉** ——
  没有 AOF 时它只有 RDB 定时快照，崩一次就可能丢掉那段窗口里的待投递消息。
  重建容器的命令：`docker run -d --name redis -p 6379:6379 -v hmdp-redis-data:/data redis redis-server --appendonly yes --appendfsync everysec`
- RabbitMQ `127.0.0.1:5672`（guest/guest），管理台 15672。本地可用
  `docker run -d --name hmdp-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management` 起

## 构建与运行

**本项目必须用 JDK 8 编译。** 默认 shell 里的 `java` 是 JDK 24，直接 `mvn` 会失败；JDK 8 在 `C:\Users\mike\.jdks\corretto-1.8.0_492`（IDEA 中也用这个）。

还有一个隐蔽的坑：全局 Maven 配置 `D:/kit/apache-maven-3.9.4/conf/settings.xml` 里有个 `jdk-17` profile，带 `<activeByDefault>true</activeByDefault>`，会把 `maven.compiler.source/target=17` 强行注入**每一个** Maven 构建，覆盖 pom 里的 `<java.version>1.8</java.version>`，报错 `无效的目标发行版: 17`。必须显式停用该 profile：

```bash
export JAVA_HOME="C:/Users/mike/.jdks/corretto-1.8.0_492"

mvn clean compile -P '!jdk-17'              # 编译
mvn package -DskipTests -P '!jdk-17'        # 打包 -> target/hm-dianping-0.0.1-SNAPSHOT.jar
java -jar target/hm-dianping-0.0.1-SNAPSHOT.jar
mvn spring-boot:run -P '!jdk-17'            # 或直接跑
```

接口文档（Knife4j）：http://localhost:8081/doc.html

初始化数据库：`mysql -uroot -p123456 < src/main/resources/db/hmdp.sql`

### 测试

`src/test` 下只有两个 `@SpringBootTest`，**都是连真实 MySQL/Redis 的手动冒烟测试，没有任何断言**，只是跑通不抛异常即通过。跑测试前必须先把 Redis 起起来。

```bash
mvn test -P '!jdk-17' -Dtest=RedisDataTest#workerTest   # 单个方法
mvn test -P '!jdk-17'                                    # 全部（会启动完整 Spring 上下文）
```

- `RedisDataTest.workerTest` — 300 线程压 `RedisWorker.nextId`，打印吞吐
- `RedisDataTest.redisTest` — 往 `cache:shop:1` 写逻辑过期缓存
- `RedisDataTest.loadShoptest` — 把 `tb_shop` 按 typeId 批量灌进 Redis GEO（跑附近商户功能前必须先跑这个）
- `HmDianPingApplicationTests` — 空壳，只会启动上下文

### 多节点测试

IDEA 里有个 `HmDianPingApplication2` 运行配置，带 `-Dserver.port=8082`，用于模拟多节点验证分布式锁/秒杀。

## 架构

标准分层：`controller` → `service`(`I*Service`/`impl`) → `mapper`（MyBatis-Plus `BaseMapper`），`entity`/`dto`/`utils`/`constant`/`config` 辅助。

关键横切机制：
- `Result<T>` 统一响应体；`WebExceptionAdvice` 只兜 `RuntimeException` → `Result.fail("服务器异常")`（所以很多错误最后表现为 500 而不是 401，见下方陷阱）
- `HmDianPingApplication` 上的 `@EnableAspectJAutoProxy(exposeProxy = true)` 是为了 `AopContext.currentProxy()` —— 秒杀里 `@Transactional` 自调用必须走代理对象，不能改成 `this.`
- `UserHolder` 用 ThreadLocal 存当前用户，`JwtLoginInterceptor` 写入、`afterCompletion` 清理

业务模块：用户/登录（JWT）、商户（缓存 + GEO 附近）、店铺分类（List 缓存）、探店笔记（ZSet 点赞 + 推模式 Feed）、关注（Set）、签到（Bitmap）、优惠券秒杀。

## 缓存体系（重点）

`utils/RedisClient.java` 是**手写的通用缓存模板**（`@Component`），把缓存策略抽象成了可复用方法，`ShopServiceImpl` 直接调它：

| 方法 | 解决的问题 |
|---|---|
| `queryWithPassThrough` | 缓存穿透 —— 未命中查库，库中不存在则写空值哨兵 `""`（`CACHE_NULL_TTL`） |
| `queryWithPathThroughAndLogicExpire` | 缓存击穿 —— 逻辑过期 + 互斥锁 + 异步重建（`CACHE_EXECUTOR_SERVICE`，10 线程） |

注意方法名是 `queryWith**Path**Through`（拼写错误，非 Pass），改名会波及调用方。

其他缓存点：
- `ShopServiceImpl.update` 用**先更新数据库、再删除缓存**（Cache-Aside），不是更新缓存
- `ShopTypeServiceImpl` 把分类列表缓存成 Redis **List**（`cache:shop:type`），无 TTL、无失效
- 店铺缓存 key 是 `cache:shop:{id}`，`RedisData` 是逻辑过期信封（`expireTime` + `data`）

> **重要约定：各 ServiceImpl 中的大量注释块是刻意保留的。**
> `ShopServiceImpl`（缓存三种方案：pass-through / 互斥锁 / 逻辑过期）和 `VoucherOrderServiceImpl`（秒杀四代方案：synchronized / Redisson 锁 / 阻塞队列 / Lua+Stream）里各留了几套演进中的实现。**不要当作死代码清理掉**；修改这些方法前先看清楚当前真正生效的是哪一行。
>
> 注意 `VoucherOrderServiceImpl` 里 Stream 那一代用的是 **`//` 逐行注释**而不是 `/* */` 块 —— 因为文件里已有嵌套的 `/* */` 方案对照块，而 Java 块注释**不支持嵌套**，再用块注释包裹会被内层的 `*/` 提前截断。

## 秒杀链路（重点）

**当前生效的是 RabbitMQ 方案**（2026-09 从 Redis Stream 改造而来）。

入口 `POST /voucher-order/seckill/{id}` → `VoucherOrderServiceImpl.seckillVoucher`：

1. `RedisWorker.nextId("order:")` 生成全局订单号（Redis `INCR`，`时间戳 << 32 | 当日计数`，key `icr:order:yyyy:MM:dd`）；消息体 JSON 在 Java 侧序列化
2. 执行 `seckill.lua` —— **单条 Lua 保证原子性**：判库存 → 一人一单 → 扣库存 → 记录已购 → **`LPUSH` 待投递队列**，五件事一步完成。返回 `0` 成功 / `1` 库存不足 / `2` 重复下单
3. 直接 `Result.ok(orderId)` 返回，用户侧零等待
4. `SeckillMessageRelay` 后台线程 `BRPOPLPUSH pending → processing` 取出消息，投递 RabbitMQ 并**同步等 broker 确认**，确认成功才 `LREM` 删除
5. `SeckillOrderConsumer` 手动 ACK 消费，`createOrderFromMq`（`@Transactional`）乐观扣减 `stock = stock - 1 AND stock > 0` 后直接 `save`，**靠 `uk_user_voucher` 唯一索引兜底一人一单**（不再先 count 再插）
6. 建单成功后投递一条延迟消息到 `seckill.delay.queue`（TTL 15 分钟）
7. 延迟消息过期 → 死信 → `SeckillTimeoutConsumer` 取消未支付订单并回滚 Redis + DB 库存

**热路径完全在 Redis 内闭环，不碰数据库、不发 MQ。** 整个下单请求只有一次 Redis 往返。

### 消息投递的可靠性（`mq/SeckillMessageRelay.java`）

```
seckill:pending ──BRPOPLPUSH──> seckill:pending:processing
                                       │ convertAndSend + 等确认
                                 确认成功 → LREM（真正删除）
                                 确认失败 → 留在 processing
```

- **为什么不用 `BRPOP`**：它「取走即删」，进程在取走之后、MQ 确认之前崩溃，消息就真没了（而库存已扣）。`BRPOPLPUSH` 先挪到 processing，确认成功才删。
- **怎么发现 processing 里卡住的**：List 没有时间戳。判据取巧但准确 —— **应用启动时把整个 processing 搬回 pending**，启动那一刻不可能有在途消息。
- **为什么这里可以阻塞**：relay 在自己的守护线程上，不在请求线程里。所以可以放心用 `CorrelationData.getFuture()` **同步等确认**，不必维护异步回调那套。
- **重试上限**：单轮 3 次（退避 1s×2），进程内累计超过 5 次移入 `seckill:pending:dead` 并告警 —— 避免原 Stream 版 `handlePendingList` 那种死循环。
- **`returnedMessage` 必须处理**：路由不到队列的消息 broker 仍是 **ack** 的，不单独判定就会被当成投递成功而误删。relay 用 `returnedIds` 集合标记，让 `sendWithConfirm` 判定为失败。

> 历史：这里原本是「先写 MySQL 本地消息表 `tb_local_message` 再发 MQ」的 outbox 方案。
> 它的问题是和 Lua 的扣库存分属两段独立操作、中间仍有窗口，只能靠「写表失败就回滚库存」补偿，
> 而补偿本身也会失败。改成 `LPUSH` 进 Lua 后窗口彻底消失，表也已删除（见 `db/mq_upgrade.sql`）。

### MQ 拓扑（`config/RabbitMQConfig.java`）

| 名称 | 作用 |
|---|---|
| `seckill.order.exchange` / `seckill.order.queue` | 下单主链路，rk=`seckill.order` |
| `seckill.order.dlx.exchange` / `seckill.order.dlx.queue` | **建单失败**死信（要告警、要人工介入） |
| `seckill.dlx.exchange` / `seckill.dlx.queue` | **超时取消**死信（正常业务流程） |
| `seckill.delay.queue` | 延迟队列，`x-message-ttl=900000`，**故意不设消费者** |

两组死信刻意分开：建单失败是「出问题了」，超时取消是「正常流程」，混在一起就得靠 `x-death` 猜来源。

### 两个必须知道的坑（都是实测踩出来的）

- **TTL 是队列级的，且队头过期才投递死信。** 所以改 `SECKILL_DELAY_TTL`（`RabbitMQConfig`）之后必须先把 `seckill.delay.queue` 删掉再重启应用，否则 RabbitMQ 会以 `PRECONDITION_FAILED` 拒绝参数不一致的队列声明。
- **`seckill.lua` 里库存的判空不能省。** `tonumber(redis.call('get', stockKey))` 在 key 不存在时得到 `nil`，直接写 `nil <= 0` 会让**整个脚本**报错 `attempt to compare nil with number`，接口返回 500 而不是「库存不足」。只要 Redis 被清空、或某张券没预热过库存，就会踩到。已改成先判 nil（`if(stock == nil or stock <= 0) then return 1`），**别改回去**。
- **手动 ACK 模式下，容器不会替你 nack。** 所以 `spring.rabbitmq.listener.simple.retry` 那套「重试耗尽自动拒绝」是**失效**的 —— 消息会一直 unacked、channel 关闭时重回队列、无限重投（实测：既进不了死信也丢不掉，把扣库存 SQL 空跑了 5 次）。因此重试和拒绝都由消费者显式执行，`listener.simple.retry.enabled` 已设为 `false`，**别改回 true**。

**热路径的代价**：把消息落点从 MySQL 挪到 Redis List 换来了热路径零 DB，但也就把消息的可靠性绑到了 Redis 上 —— **那个 list 有多可靠，取决于 Redis 有没有开持久化（AOF）**。好消息是库存预占和待投递消息在同一条 Lua 里，Redis 挂了会一起没，不会出现「库存扣了但消息没了」的错账。

Redisson 依赖和 `RedissonClient` bean（`RedisConfig`）都在，但**活路径上没用到** —— `RLock` 的调用全在注释块里。别以为有 Redisson 锁在生效。

## Redis Key 约定

| Key | 用途 |
|---|---|
| `phone:{phone}` | 登录验证码，2 分钟 |
| `cache:shop:{id}` | 店铺缓存（逻辑过期信封） |
| `cache:shop:type` | 分类列表（List） |
| `lock:{id}` | 缓存重建互斥锁 |
| `seckill:stock:{voucherId}` | 秒杀库存（lua 内硬编码，Java 侧用 `RedisConstants.SECKILL_STOCK_KEY`） |
| `seckill:order:{voucherId}` | 已下单用户 Set（lua 内硬编码，Java 侧用 `RedisConstants.SECKILL_ORDER_KEY`） |
| `stream.orders` | ~~秒杀订单 Stream~~ **已停用**，仅存在于注释代码 |
| `seckill:pending` | 待投递 MQ 队列（List），由 `seckill.lua` LPUSH |
| `seckill:pending:processing` | 投递中队列，`BRPOPLPUSH` 进来，MQ 确认后 LREM |
| `seckill:pending:dead` | 重试超限的兜底队列，需人工处理 |
| `icr:{prefix}yyyy:MM:dd` | 全局 ID 自增 |
| `blog:liked:{blogId}` | 点赞 ZSet |
| `feeds:{userId}` | 关注 Feed 收件箱 ZSet |
| `follow:{userId}` | 关注 Set |
| `shop:geo:{typeId}` | 店铺 GEO |
| `sign:{userId}:yyyyMM` | 签到 Bitmap |

秒杀那两个 key **不在常量类里**，只存在于 `seckill.lua`；改 key 名要同时改 lua。

`RedisConstants` 有两份：`com.hmdp.constant.RedisConstants` 是生效的；`com.hmdp.utils.RedisConstants` **整个文件被注释掉了，是死文件**。常量类里 `CACHR_SHOP_TYPE_KEY` 是拼写错误（且没有尾部冒号）。

## 认证

JWT **无状态**，token 不存 Redis。`JwtUtil`（Hutool 实现，非 jjwt）签发，`hmdp.jwt.secret=cinfly`、`ttl-minutes=36000`（约 25 天，固定不滑动）。

`MvcConfig` 全局注册 `JwtLoginInterceptor`，只排除 `/user/login`、`/user/code`、`/doc.html`、`/webjars/**`、`/swagger-resources/**`、`/v2/api-docs` —— 其余**所有**路径（含 `/shop/**`、`/blog/hot`）都要带 `authorization` 头，且是**裸 token、不带 `Bearer ` 前缀**。

`LoginInteceptor` 从未被注册；`RefreshTokenInteceptor` 整个文件被注释 —— 所以**没有续期机制**。

## 已知陷阱

这些都是现有代码里的真实缺陷，改动相关逻辑时留意（不要顺手"修"无关的）：

- **秒杀首单 NPE（已随 Stream 方案停用）**：原来 `proxy = AopContext.currentProxy()` 赋值在 `XADD` **之后**，消费者线程可能抢先拿到消息导致 `proxy` 为 null。改用 RabbitMQ 后消费者是独立 Bean、注入的本来就是代理对象，**这个问题自然消失**，只留在注释代码里。
- **一人一单是「终身」的，取消不释放资格**：`cancelUnpaidOrder` 只回滚 Redis 库存，**不清** `seckill:order:{id}` 里的用户标记。因为订单被取消时行还在表里，`uk_user_voucher` 索引位仍被占用，DB 侧不可能再接受同一 `(user, voucher)`；清标记会造成「Redis 放行、DB 必然拒绝」的两边打架。
- **建单失败进入待人工审核后，Redis 侧的预留不会自动释放**：库存仍处于已扣状态、用户仍被标记为已购。这是刻意的——人工重试若成功，库存早已退回就会产生「无单却已退库存」的错账。需要人工在处理失败记录时一并决定是否回滚 Redis。
- **`RedisClient` 的锁不安全**：`tryLock`/`unlock` 是裸 `SETNX` + `DEL`，没有 token 校验，可能释放别人的锁（`SimpleRedisLock` 用了 `unlock.lua` 做原子校验，但那条路径没被 `RedisClient` 复用）。异步重建时若查库返回 `null`，会用 `null` 覆盖空值哨兵，穿透防护失效。
- **`handlePendingList` 可能死循环**：`while(true)` 里若 `createOrder` 持续失败，同一条 pending 消息会被无限重投。
- **`ShopTypeServiceImpl` 并发 miss 会写入重复元素**：miss 判定是 `isEmpty` + `rightPushAll` 追加，不是原子 `SET`，两个并发请求会各推一遍。
- **`VoucherServiceImpl.addSeckillVoucher` 在 `@Transactional` 内写 Redis 预热库存**：事务回滚不会撤销 Redis 写入，且该 key 无 TTL。
- **`tb_voucher_order` 没有 `(user_id, voucher_id)` 唯一索引**，`tb_follow`、`tb_sign` 同样缺唯一约束。幂等完全靠 Redis + 应用层，任何绕过 `seckill.lua` 的路径只剩「先 count 再 insert」这种非原子保护。
- **无效 token 会被放行**：`JwtLoginInterceptor` 里 `parseToken` 返回 null 被忽略，下游 `UserHolder.getUser().getId()` 直接 NPE → 被全局异常处理器变成 500"服务器异常"；而空 token 虽然 `return false` 却没设 401，客户端拿到空 200。
- **项目只能用 JDK 8 编译**：多个文件有 IDE 误导入的 JDK 内部类（`RedisConstants` 的 `com.sun.corba.*`、`RedisConfig` 的 `sun.misc.*`、`LoginInteceptor` 的 `javax.jws.soap.*`、`BlogServiceImpl` 的 `javax.swing.text.*` 等）。`com.sun.corba` 在 JDK 11 已移除，升 JDK 会直接编译失败——升级 JDK 时得先清这些 import。
- **依赖被强行改版本**：`pom.xml` 把 `spring-data-redis`(2.6.2) 和 `lettuce-core`(6.1.6) 从 starter 里排除后按 Boot 2.6 的版本重新声明，与 Boot 2.3.12 不匹配；MySQL 驱动是老的 5.1.47（`com.mysql.jdbc.Driver`）。

## 数据库

单库 `hmdp`，`resources/db/hmdp.sql`（原始 dump，不要就地改）。核心表：`tb_user`、`tb_shop`（含 `x`/`y` 经纬度）、`tb_shop_type`、`tb_voucher`（`type` 0=普通券 1=秒杀券）、`tb_seckill_voucher`（PK 就是 `voucher_id`，与 `tb_voucher` 一对一，`stock` 是普通 `int(8)` 可为负）、`tb_voucher_order`、`tb_blog`、`tb_blog_comments`、`tb_follow`、`tb_sign`、`tb_user_info`。全库**没有任何外键**。

MQ 改造新增的变更在 **`resources/db/mq_upgrade.sql`**（单独迁移文件，与 dump 分开）：
- `ALTER TABLE tb_voucher_order ADD UNIQUE KEY uk_user_voucher (user_id, voucher_id)` —— 一人一单的 DB 层防线
- `tb_seckill_order_fail` —— 建单失败兜底，`status` 0=待重试 / 1=待人工审核（自动重试 3 次后用尽）
- `DROP TABLE tb_local_message` —— 生产者 outbox 方案已被 Redis List 取代，见上方「消息投递的可靠性」

注意该文件里的 `ALTER TABLE` **不是幂等的**，重复执行会报 `Duplicate key name 'uk_user_voucher'`，属正常现象。

## 代码约定

- 实体/DTO 用 Lombok `@Data` + `@Accessors(chain = true)`，`@TableName` 映射 `tb_` 表名
- Controller 用 Swagger 注解（`@Api`/`@ApiOperation`）生成文档；各类 javadoc 署名为 `@author cinfly`
- MyBatis-Plus 只注册了分页插件（`MybatisConfig`），**没有**乐观锁、防全表更新等插件；XML 只有 `resources/mapper/VoucherMapper.xml` 一个
- `BlogController` 的 `ORDER BY FIELD(id, ...)` 是字符串拼接进 `last()` 的，不走参数绑定
