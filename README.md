# English | [简体中文](./README_zhCN.md)

<p align="center">
<img src="https://raw.githubusercontent.com/KFCFans/PowerJob/master/others/images/logo.png" alt="PowerJob" title="PowerJob" width="557"/>
</p>

<p align="center">
<a href="https://github.com/PowerJob/PowerJob/actions"><img src="https://github.com/PowerJob/PowerJob/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master" alt="actions"></a>
<a href="https://central.sonatype.com/search?smo=true&q=powerjob-worker&namespace=tech.powerjob"><img alt="Maven Central" src="https://img.shields.io/maven-central/v/tech.powerjob/powerjob-worker"></a>
<a href="https://github.com/PowerJob/PowerJob/releases"><img alt="GitHub release (latest SemVer)" src="https://img.shields.io/github/v/release/kfcfans/powerjob?color=%23E59866"></a>
<a href="https://github.com/PowerJob/PowerJob/blob/master/LICENSE"><img src="https://img.shields.io/github/license/KFCFans/PowerJob" alt="LICENSE"></a>
</p>

[PowerJob](https://github.com/PowerJob/PowerJob) is an open-source distributed computing and job scheduling framework which allows developers to easily schedule tasks in their own application.

Refer to [PowerJob Introduction](https://www.yuque.com/powerjob/en/introduce) for detailed information.

# Java 21 / Spring Boot 4

All modules now require **JDK 21**. Applications and the worker starter target **Spring Boot 4.0.9**;
the starter is not compatible with Boot 2 or 3. Rebuild consumers and dynamic processor projects with
Java 21. Servlet, WebSocket, persistence and resource-injection integrations now use `jakarta.*`.
The server uses embedded **Tomcat 11**, not Undertow; the independent HTTP/AKKA/MU transports remain available.

PowerJob's public JSON utilities and existing Jackson annotations remain on **Jackson 2**.
The server intentionally uses Boot's deprecated `spring-boot-jackson2` compatibility bridge, with
`spring.http.converters.preferred-json-mapper=jackson2`. Move custom mapper settings from
`spring.jackson.*` to `spring.jackson2.*`. Jackson 3 may coexist for framework integrations.

## Building and testing

Select JDK 21 in `JAVA_HOME` and put `$JAVA_HOME/bin` on `PATH`, then use **mvnd 1.0.3**:

```bash
java -version
mvnd --version
bash .github/scripts/verify-java21.sh
```

The [verification script](.github/scripts/verify-java21.sh) builds every module and runs an explicit,
local-only contract suite, including real embedded-Tomcat HTTP/WebSocket and HTTP/AKKA/MU loopback tests.
It does not run legacy tests that connect to shared databases, object stores or public HTTP services.
Do not run an unrestricted `mvnd test` or `mvnd verify` until those fixtures have been isolated.
The [Java CI workflow](.github/workflows/maven_build.yml) also runs `PersistenceMigrationIT` against
disposable MySQL and PostgreSQL services. That test accepts only loopback databases named
`powerjob_upgrade_test`, bootstraps frozen pre-upgrade DDL and validates mappings without schema updates.

Downloaded processor templates now default to Java 21 and Spring 7. Build them with JDK 21;
explicit older processor bytecode targets remain accepted for compatibility with existing requests.
This does not make the Java 21 PowerJob artifacts loadable by older JVMs.

## Database and deployment compatibility

| Database | JDBC driver retained | Java 21 migration validation |
|---|---|---|
| MySQL | Connector/J 8.0.33 | MySQL 8.4: legacy schema, identity IDs, large text and transaction routing |
| PostgreSQL | 42.6.0 | PostgreSQL 18: legacy schema, shared `native` sequence, text LOBs and transaction routing |
| Oracle | ojdbc8 19.7.0.0 | Real-database validation still required |
| SQL Server | 7.4.1.jre8 | Driver/runtime and real-database validation still required |
| DB2 | 11.5.0.0 | Driver/runtime and real-database validation still required |

No schema migration is included. Native sequence allocation remains 1, table prefixes and names are
preserved, and MySQL LOB columns retain their existing `longtext` capacity. Both configured PostgreSQL
dialect class names remain available; the Hibernate community dialect dependency preserves their
PostgreSQL-10-era SQL baseline. This does not certify every historical database version.
Before rollout, validate a disposable copy of your actual schema with
`spring.datasource.remote.hibernate.properties.hibernate.hbm2ddl.auto=validate`; never use automatic
schema updates to conceal an incompatibility.

Application Dockerfiles pin a multi-architecture Temurin 21 JDK image. The agent's `python` command now
runs **Python 3**; Python 2 processors must be ported before using that image. Development GC options use
Java 21 unified logging. Docker image startup and architecture checks must be run on a Docker-enabled host.

Java 8 consumers cannot load these Java 21 artifacts. Frozen Java 8 JSON/Kryo fixtures and separate-JVM
Java 8/Java 21 request/reply checks passed for HTTP, AKKA and MU. The upgraded MU transport also releases
request-timeout threads on shutdown. These checks do not replace rolling-upgrade tests for your actual
server-worker topology and processors. Retain the previous artifacts and a database backup; do not assume
that application rollback reverses data or DDL changes. Release coordinates are unchanged: choose a new
release version before publishing, rather than overwriting `5.1.7`.

# Introduction

### Features
- **Friendly UI:** [Front-end](http://try.powerjob.tech/#/welcome?appName=powerjob-agent-test&password=123) page is provided and developers can manage their task, monitor the status, check the logs online, etc.

- **Abundant Timing Strategies:** Four timing strategies are supported, including CRON expression, fixed rate, fixed delay and OpenAPI which allows you to define your own scheduling policies, such as delaying execution.

- **Multiple Execution Mode:** Four execution modes are supported, including stand-alone, broadcast, Map and MapReduce. Distributed computing resource could be utilized in MapReduce mode, try the magic out [here](https://www.yuque.com/powerjob/en/za1d96#9YOnV)!

- **Workflow(DAG) Support:** Both job dependency management and data communications between jobs are supported.

- **Extensive Processor Support:** Developers can write their processors in Java, Shell, Python, and will subsequently support multilingual scheduling via HTTP.

- **Powerful Disaster Tolerance:** As long as there are enough computing nodes, configurable retry policies make it possible for your task to be executed and finished successfully.

- **High Availability & High Performance:**  PowerJob supports unlimited horizontal expansion. It's easy to achieve high availability and performance by deploying as many PowerJob server and worker nodes.

### Applicable scenes

- Timed tasks, for example, allocating e-coupons on 9 AM every morning.
- Broadcast tasks, for example, broadcasting to the cluster to clear logs.
- MapReduce tasks, for example, speeding up certain job like updating large amounts of data.
- Delayed tasks, for example, processing overdue orders.
- Customized tasks, triggered with [OpenAPI](https://www.yuque.com/powerjob/en/openapi).

### Online trial
- Address: [try.powerjob.tech](http://try.powerjob.tech/#/welcome?appName=powerjob-agent-test&password=123)
- Recommend reading the documentation first: [here](https://www.yuque.com/powerjob/en/trial)

# Documents
**[Docs](https://www.yuque.com/powerjob/en/introduce)**

**[中文文档](https://www.yuque.com/powerjob/guidence/intro)**

# Known Users
[Click to register as PowerJob user!](https://github.com/PowerJob/PowerJob/issues/6)  
ღ( ´・ᴗ・\` )ღ Many thanks to the following registered users. ღ( ´・ᴗ・\` )ღ
<p style="text-align: center">
<img src="https://raw.githubusercontent.com/KFCFans/PowerJob/master/others/images/user.png" alt="PowerJob User" title="PowerJob User"/>
</p>

# Stargazers over time

[![Stargazers over time](https://starchart.cc/PowerJob/PowerJob.svg)](https://starchart.cc/PowerJob/PowerJob)

# License

PowerJob is released under Apache License 2.0. Please refer to [License](./LICENSE) for details.

# Others

- Any developer interested in getting more involved in PowerJob may join our [Reddit](https://www.reddit.com/r/PowerJob) or [Gitter](https://gitter.im/PowerJob/community) community and make [contributions](https://github.com/PowerJob/PowerJob/pulls)!

- Reach out to me through email **tengjiqi@gmail.com**. Any issues or questions are welcomed on [Issues](https://github.com/PowerJob/PowerJob/issues).

- Look forward to your opinions. Response may be late but not denied.
