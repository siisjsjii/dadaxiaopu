# 达达小铺（Dadaxiaopu）

本地生活点评类平台的后端服务，提供商户浏览、探店笔记、优惠券秒杀等能力。纯 JSON API，前端与静态资源由 Nginx 独立承载。

项目主要解决两类高并发问题：

- **秒杀场景下的超卖与重复下单** —— Redis + Lua 原子扣减，RabbitMQ 异步落库
- **热点数据的缓存穿透与击穿** —— 通用缓存模板 + 逻辑过期 + 互斥锁异步重建

---

## 技术栈

| 分类 | 选型 |
| --- | --- |
| 语言 / 框架 | Java 8、Spring Boot 2.3.12、MyBatis-Plus 3.4.3 |
| 数据存储 | MySQL、Redis（Lettuce） |
| 消息队列 | RabbitMQ（spring-amqp） |
| 分布式组件 | Redisson |
| 鉴权 | JWT（Hutool 实现，无状态） |
| 接口文档 | Knife4j / Swagger |
| 构建 | Maven |

> ⚠️ **必须使用 JDK 8 编译。** 代码中存在若干 JDK 内部类的 `import`（如 `com.sun.corba.*`、`sun.misc.*`），其中 `com.sun.corba` 在 JDK 11 已被移除，用更高版本的 JDK 会直接编译失败。

---

## 项目亮点

### 一、秒杀：Lua 原子扣减 + MQ 异步落库

**1. Lua 原子化：扣减链路全在 Redis 内闭环**

「校验库存 → 一人一单 → 扣减库存 → 记录已购 → 消息入队」五步合并进**一条 Lua 脚本**，由 Redis 保证原子性。

下单接口的实际开销是 **1 次数据库读**（校验券的秒杀时间窗）+ **2 次 Redis 往返**（生成全局订单号、执行 Lua），**不写库、不发 MQ**。真正耗时的库存扣减与限购判断全部留在 Redis 内完成。

把消息入队也放进 Lua 是有意为之：它和扣库存处于同一个原子单元，因此**不存在「库存扣了但消息没记下」的窗口**，也就不需要任何补偿逻辑。

**2. 可靠投递 + 崩溃自愈**

消息由后台线程用 `BRPOPLPUSH` 从待投递队列搬到 `processing` 队列，**等 RabbitMQ 确认后才删除**。进程崩溃时消息仍留在 `processing`，应用启动时再整体搬回待投递队列。

> 相比 `BRPOP`（取走即删），这套模式保证「取走后、确认前」崩溃不丢消息。实测 `kill -9` 应用后重启，卡在投递中的订单仍能成功落库。

**3. 三层幂等防线**

| 层次 | 机制 | 挡住什么 |
| --- | --- | --- |
| Redis | Lua 中 `SISMEMBER` | 用户重复点击（快筛） |
| DB | 主键 `id` | 同一条消息被重复投递 |
| DB | `uk_user_voucher` 唯一索引 | 「一人一单」的权威保证 |

Lua 那道是快筛而非事实源，Redis 数据丢失或绕过脚本的路径都可能漏，真正兜底的是唯一索引。据此对两种 `DuplicateKeyException` 做**相反处理**：撞主键说明是同消息重投（Redis 只扣过一次，绝不回滚）；撞唯一索引说明库存被多扣（必须补回并恢复「已购」标记）。

**4. 手动 ACK + 两级死信**

- **手动 ACK**：处理成功才确认，避免消费中途进程挂掉丢消息
- **两组独立死信队列**：建单失败（需告警、需人工介入）与超时取消（正常业务流程）分开，语义不混淆
- **失败兜底**：建单重试耗尽后落失败表并告警，定时任务再重试，仍失败则转「待人工审核」，避免对坏消息无限重投

> 踩坑记录：`acknowledge-mode: manual` 下**容器不会替消费者 nack**，Spring Retry 重试耗尽后消息不会被拒绝，而是一直 unacked、channel 关闭时重回队列、无限重投。最终改为由消费者显式重试 + 显式 `basicNack(requeue=false)`。

**5. 延迟队列实现未支付自动取消**

死信 + 队列级 TTL：消息先进入无消费者的延迟队列，TTL 到期后死信到取消队列，由消费者取消未支付订单并回滚 Redis / DB 库存。取消动作是**带状态条件的乐观更新**，重复消费时影响行数为 0，天然幂等。

### 二、缓存：穿透、击穿与一致性

**1. 把缓存策略抽象成可复用模板**

`RedisClient` 是一个泛型缓存模板，把策略沉淀为方法，业务侧一行调用：

| 方法 | 解决的问题 |
| --- | --- |
| `queryWithPassThrough` | **缓存穿透**：未命中查库，库中不存在则写空值哨兵 + 短 TTL，防止恶意 ID 反复打库 |
| `queryWithPathThroughAndLogicExpire` | **缓存击穿**：逻辑过期 + 互斥锁 + 异步重建 |

**2. 逻辑过期，热点 key 不会同时失效**

缓存不设物理 TTL，而是把过期时间包进 value（`RedisData{expireTime, data}`）。命中后判断逻辑过期：未过期直接返回；已过期则抢互斥锁并交给线程池**异步重建**，当前请求仍返回旧数据 —— 既不阻塞请求，也不会出现大批请求同时打库。

**3. 一致性采用 Cache-Aside**

更新时**先更新数据库、再删除缓存**（而非更新缓存），避免并发写导致脏数据。

**4. 其他缓存实践**

- 店铺分类整表用 Redis **List** 缓存
- 附近商户用 **GEO** 按距离排序 + 分页
- 笔记点赞用 **ZSet**（score 存时间戳，天然支持「最新点赞的人」）
- 每日签到用 **Bitmap**，一位一天，位运算统计连续签到天数

---

## 快速启动

### 1. 环境要求

| 依赖 | 版本 |
| --- | --- |
| JDK | **8**（必须） |
| Maven | 3.6+ |
| MySQL | 5.7+ |
| Redis | 6+ |
| RabbitMQ | 3.x |

### 2. 启动基础设施

```bash
# Redis（建议开启 AOF，秒杀待投递队列的可靠性依赖它）
docker run -d --name redis -p 6379:6379 \
  -v redis-data:/data \
  redis redis-server --appendonly yes --appendfsync everysec

# RabbitMQ（带管理台）
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
  rabbitmq:3.13-management
```

管理台：<http://localhost:15672>（guest / guest）

### 3. 初始化数据库

```bash
mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS hmdp DEFAULT CHARSET utf8mb4;"

# 基础表结构与初始数据
mysql -uroot -p123456 hmdp < src/main/resources/db/hmdp.sql

# 秒杀模块的增量变更：一人一单唯一索引 + 建单失败表
mysql -uroot -p123456 hmdp < src/main/resources/db/mq_upgrade.sql
```

> `mq_upgrade.sql` 中的 `ALTER TABLE` 不是幂等的，重复执行会报 `Duplicate key name 'uk_user_voucher'`，属正常现象。

### 4. 修改配置

按需修改 `src/main/resources/application.yaml` 中的 MySQL / Redis / RabbitMQ 连接信息：

```yaml
spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/hmdp?useSSL=false&serverTimezone=UTC
    username: root
    password: 123456
  redis:
    host: 127.0.0.1
    port: 6379
  rabbitmq:
    host: 127.0.0.1
    port: 5672
    username: guest
    password: guest
```

### 5. 构建并运行

```bash
mvn clean package -DskipTests

java -jar target/cinfly-dadaxiaopu-0.0.1-SNAPSHOT.jar
```

或直接以开发模式启动：

```bash
mvn spring-boot:run
```

> **构建报 `无效的目标发行版: 17`？**
> 多半是全局 Maven `settings.xml` 里有个 `<activeByDefault>true</activeByDefault>` 的 JDK 版本 profile，
> 把 `maven.compiler.source/target` 强注成了 17，覆盖了 pom 里的 `<java.version>1.8</java.version>`。
> 用 `mvn ... -P '!<profile-id>'` 停用它，或直接删掉/修正该 profile。

### 6. 访问接口文档

<http://localhost:8081/doc.html>

> 除 `/user/login`、`/user/code` 外，所有接口都需要在请求头携带 `authorization`，
> 值为登录返回的 **裸 token**（不带 `Bearer ` 前缀）。

---

## 目录结构

```
src/main/java/com/cinfly/dadaxiaopu/
├── config/          # MQ 拓扑、Redis、MVC、Swagger 等配置
├── constant/        # Redis Key 等常量
├── controller/      # REST 接口层
├── dto/             # 传输对象（Result、SeckillOrderMessage 等）
├── entity/          # 数据库实体
├── mapper/          # MyBatis-Plus Mapper
├── mq/              # 消息生产（relay）、消费者、补偿定时任务
├── service/         # 业务接口与实现
└── utils/           # JWT、缓存模板、全局 ID、用户上下文等工具
```

`src/main/resources/`

```
├── application.yaml
├── seckill.lua                 # 秒杀原子脚本（判库存/一人一单/扣减/入队）
├── unlock.lua                  # 分布式锁释放脚本
├── db/hmdp.sql                 # 基础表结构与数据
├── db/mq_upgrade.sql           # 秒杀模块增量变更
└── mapper/VoucherMapper.xml
```
