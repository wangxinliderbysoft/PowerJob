# [English](./README.md) | 简体中文

<p align="center">
<img src="https://raw.githubusercontent.com/KFCFans/PowerJob/master/others/images/logo.png" alt="PowerJob" title="PowerJob" width="557"/>
</p>

<p align="center">
<a href="https://github.com/PowerJob/PowerJob/actions"><img src="https://github.com/PowerJob/PowerJob/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master" alt="actions"></a>
<a href="https://search.maven.org/search?q=tech.powerjob"><img alt="Maven Central" src="https://img.shields.io/maven-central/v/tech.powerjob/powerjob-worker"></a>
<a href="https://github.com/PowerJob/PowerJob/releases"><img alt="GitHub release (latest SemVer)" src="https://img.shields.io/github/v/release/kfcfans/powerjob?color=%23E59866"></a>
<a href="https://github.com/PowerJob/PowerJob/blob/master/LICENSE"><img src="https://img.shields.io/github/license/KFCFans/PowerJob" alt="LICENSE"></a>
</p>

PowerJob（原OhMyScheduler）是全新一代分布式调度与计算框架，能让您轻松完成作业的调度与繁杂任务的分布式计算。

# Java 21 / Spring Boot 4

所有模块最低要求 **JDK 21**，应用和 Worker Starter 使用 **Spring Boot 4.0.9**。
Starter 不再兼容 Boot 2/3；接入项目和动态处理器项目需要使用 Java 21 重新构建。
Servlet、WebSocket、持久化和资源注入接口迁移到 `jakarta.*`。
Server 的嵌入式容器改为 **Tomcat 11**；独立的 HTTP、AKKA、MU 通讯协议均保留。

Jackson 集成已统一到 Boot 管理的 **Jackson 3.1.7**，MVC 使用原生 Jackson 3 转换器。
已移除 Jackson 2 兼容模块及转换器偏好配置，MVC 配置使用 `spring.jackson.*`。
`com.fasterxml.jackson.annotation` 是 Jackson 3 官方共享依赖，保留这些注解不代表保留 Jackson 2 实现。
`JsonUtils` 泛型重载改用 `tools.jackson.core.type.TypeReference`，接入方需要修改导入并重新编译；
这是源码和二进制兼容性变更。原有空值、日期、DAG 属性名和字符串实例 ID 约定保留。
自有 Jackson 3 mapper 使用 `configureForJackson2()`，MVC 配置
`spring.jackson.use-jackson2-defaults=true`，保留枚举名称等原有默认行为。
这些是 Jackson 3 的兼容设置，并非使用 Jackson 2 实现或转换器。

优先检查了组件升级，但 Vert.x 5、JJWT Jackson 扩展、springdoc 和 AWS SDK v2 仍依赖
Jackson 2 实现（AWS 包含 shaded 副本）。因此保留 Vert.x、JJWT，通过其官方序列化扩展接入
Jackson 3。HTTP 启动必须选中 PowerJob 注册的 Vert.x `JsonFactory`；嵌入其他应用时需保留
SPI 资源，不能由其他 JSON 编解码器抢占。S3 存储改用 **jclouds 2.7.0**，
由 SDK 负责 SigV4 签名，保留 `oms.storage.dfs.minio.*` 配置和路径式访问，区分对象不存在与鉴权失败。
Swagger Core 3 根据实际 MVC 映射生成文档，保留 Swagger UI、`oms.swagger.enable`、
`/v3/api-docs/PowerJob-ALL`、`/v3/api-docs.yaml/PowerJob-ALL` 及上下文路径支持。

安全筛选的全仓测试、真实 Java 8/21 协议与 JWT 双向互通、嵌套构件 Jackson 审计已通过。
独立本地 S3 兼容服务验证了签名请求、文件/元数据往返和错误签名拒绝；
桶策略已通过 HTTP 夹具验证，但实际 MinIO 验收仍待完成。
Docker 和 Oracle/SQL Server/DB2 验收继续保持待完成状态。

## 构建与测试

将 `JAVA_HOME` 指向 JDK 21，并把 `$JAVA_HOME/bin` 放到 `PATH` 前部，使用 **mvnd 1.0.3**：

```bash
java -version
mvnd --version
bash .github/scripts/verify-java21.sh
```

[验证脚本](.github/scripts/verify-java21.sh)构建全部模块，运行显式选择的本地契约测试，
包括真实 Tomcat HTTP/WebSocket 测试和 HTTP/AKKA/MU 回环通讯测试。
它不会运行依赖共享数据库、对象存储或公网 HTTP 服务的旧测试。
隔离这些测试资源之前，不要直接运行未筛选的 `mvnd test` 或 `mvnd verify`。
[Java CI](.github/workflows/maven_build.yml)还会在一次性 MySQL/PostgreSQL 服务上运行
`PersistenceMigrationIT`。该测试只接受本机回环地址、数据库名为 `powerjob_upgrade_test` 的连接，
使用升级前冻结的 DDL 初始化数据库，然后验证映射，不通过更新表结构消除错误。

下载的处理器模板默认使用 Java 21 和 Spring 7，需使用 JDK 21 构建；
仍保留请求中显式指定的旧处理器字节码目标，但这不代表旧 JVM 可以加载 Java 21 PowerJob 构件。

## 数据库与部署兼容性

| 数据库 | 保留的 JDBC 驱动 | Java 21 迁移验证 |
|---|---|---|
| MySQL | Connector/J 8.0.33 | MySQL 8.4：旧表结构、自增 ID、大文本和事务路由 |
| PostgreSQL | 42.6.0 | PostgreSQL 18：旧表结构、共享 `native` 序列、文本 LOB 和事务路由 |
| Oracle | ojdbc8 19.7.0.0 | 仍需真实数据库验证 |
| SQL Server | 7.4.1.jre8 | 仍需驱动运行时和真实数据库验证 |
| DB2 | 11.5.0.0 | 仍需驱动运行时和真实数据库验证 |

本次不包含表结构迁移。保留序列步长 1、表前缀和命名规则，MySQL LOB 保留原有 `longtext` 容量。
两个原有 PostgreSQL 方言类名均保留，使用 Hibernate 社区方言维持原 PostgreSQL 10 时代的 SQL 基线，
但这不代表所有历史数据库版本都已通过验证。
发布前应使用实际数据库的一次性副本，并设置
`spring.datasource.remote.hibernate.properties.hibernate.hbm2ddl.auto=validate` 检查兼容性；
不要依靠自动修改表结构掩盖问题。

应用 Dockerfile 固定使用支持多架构的 Temurin 21 JDK 镜像。
Agent 中的 `python` 命令改为 **Python 3**，Python 2 处理器必须先迁移。
开发环境 GC 参数已改为 Java 21 统一日志格式；镜像启动和架构检查仍需在支持 Docker 的环境中执行。

Java 8 应用无法加载新的 Java 21 构件。冻结的 Java 8 JSON/Kryo 数据兼容测试，以及不同 JVM 中
Java 8/Java 21 双向 HTTP、AKKA、MU 请求响应测试已通过；升级后的 MU 还验证了请求超时线程的释放。
这些检查不能替代实际 Server/Worker 拓扑和处理器的滚动升级验证。
请保留旧构件和数据库备份，不能假设应用回滚会撤销数据或 DDL 变更。
项目发布坐标未修改，正式发布前需确定新版本号，不要覆盖已经发布的 `5.1.7`。

# 简介
### 主要特性
* 使用简单：提供前端Web界面，允许开发者可视化地完成调度任务的管理（增、删、改、查）、任务运行状态监控和运行日志查看等功能。
* 定时策略完善：支持CRON表达式、固定频率、固定延迟和API四种定时调度策略。
* 执行模式丰富：支持单机、广播、Map、MapReduce四种执行模式，其中Map/MapReduce处理器能使开发者寥寥数行代码便获得集群分布式计算的能力。
* DAG工作流支持：支持在线配置任务依赖关系，可视化得对任务进行编排，同时还支持上下游任务间的数据传递
* 执行器支持广泛：支持Spring Bean、内置/外置Java类、Shell、Python等处理器，应用范围广。
* 运维便捷：支持在线日志功能，执行器产生的日志可以在前端控制台页面实时显示，降低debug成本，极大地提高开发效率。
* 依赖精简：最小仅依赖关系型数据库（MySQL/Oracle/MS SQLServer...）。
* 高可用&高性能：调度服务器经过精心设计，一改其他调度框架基于数据库锁的策略，实现了无锁化调度。部署多个调度服务器可以同时实现高可用和性能的提升（支持无限的水平扩展）。
* 故障转移与恢复：任务执行失败后，可根据配置的重试策略完成重试，只要执行器集群有足够的计算节点，任务就能顺利完成。

### 适用场景
* 有定时执行需求的业务场景：如每天凌晨全量同步数据、生成业务报表等。
* 有需要全部机器一同执行的业务场景：如使用广播执行模式清理集群日志。
* 有需要分布式处理的业务场景：比如需要更新一大批数据，单机执行耗时非常长，可以使用Map/MapReduce处理器完成任务的分发，调动整个集群加速计算。
* 有需要**延迟执行**某些任务的业务场景：比如订单过期处理等。

### 设计目标
PowerJob 的设计目标为企业级的分布式任务调度平台，即成为公司内部的**任务调度中间件**。整个公司统一部署调度中心 powerjob-server，旗下所有业务线应用只需要依赖 `powerjob-worker` 即可接入调度中心获取任务调度与分布式计算能力。

### 在线试用
* [点击查看试用说明和教程](https://www.yuque.com/powerjob/guidence/trial)

### 同类产品对比
|                | QuartZ                   | xxl-job                                  | SchedulerX 2.0                                    | PowerJob                                                |
| -------------- | ------------------------ | ---------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------ |
| 定时类型       | CRON                     | CRON                                     | CRON、固定频率、固定延迟、OpenAPI                 | **CRON、固定频率、固定延迟、OpenAPI**                        |
| 任务类型       | 内置Java                 | 内置Java、GLUE Java、Shell、Python等脚本 | 内置Java、外置Java（FatJar）、Shell、Python等脚本 | **内置Java、外置Java（容器）、Shell、Python等脚本**          |
| 分布式计算     | 无                       | 静态分片                                 | MapReduce动态分片                                 | **MapReduce动态分片**                                        |
| 在线任务治理   | 不支持                   | 支持                                     | 支持                                              | **支持**                                                     |
| 日志白屏化     | 不支持                   | 支持                                     | 不支持                                            | **支持**                                                     |
| 调度方式及性能 | 基于数据库锁，有性能瓶颈 | 基于数据库锁，有性能瓶颈                 | 不详                                              | **无锁化设计，性能强劲无上限**                               |
| 报警监控       | 无                       | 邮件                                     | 短信                                              | **WebHook、邮件、钉钉与自定义扩展**                             |
| 系统依赖       | JDBC支持的关系型数据库（MySQL、Oracle...）                    | MySQL                                    | 人民币        | **任意Spring Data Jpa支持的关系型数据库（MySQL、Oracle...）** |
| DAG工作流      | 不支持                   | 不支持                                   | 支持                                              | **支持**                                   |


# 官方文档
**[中文文档](https://www.yuque.com/powerjob/guidence/intro)**

**[Docs](https://www.yuque.com/powerjob/en/introduce)**

# 接入登记
[点击进行接入登记，为 PowerJob 的发展贡献自己的力量！](https://github.com/PowerJob/PowerJob/issues/6)

ღ( ´・ᴗ・\` )ღ 感谢以下接入用户的大力支持 ღ( ´・ᴗ・\` )ღ

<p align="center">
<img src="https://raw.githubusercontent.com/KFCFans/PowerJob/master/others/images/user.png" alt="PowerJob User" title="PowerJob User"/>
</p>

# 其他
* 开源许可证：Apache License, Version 2.0
* 欢迎共同参与本项目的贡献，PR和Issue都大大滴欢迎（求求了）～
* 觉得还不错的话，可以点个Star支持一下哦～ =￣ω￣=
* 联系方式@KFCFans -> `tengjiqi@gmail.com`
* 用户交流QQ群（因广告信息泛滥，加群需要验证，请认真填写申请原因）：
  * 一群（已满）：487453839
  * 二群：834937813