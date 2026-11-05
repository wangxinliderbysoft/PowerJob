# Java 21 JDK (amd64 and arm64), Ubuntu 22.04 LTS
FROM eclipse-temurin:21-jdk-jammy@sha256:4b1a74fbffbe6b4e7b6f0171a472478282fb463790a51edf10e465d578f22b69
# 维护者
MAINTAINER dudiao(idudiao@163.com)
# 设置时区
ENV TZ=Asia/Shanghai

# 设置其他环境变量
ENV APP_NAME=powerjob-worker-samples
# 传递 SpringBoot 启动参数 和 JVM参数
ENV PARAMS=""
ENV JVMOPTIONS=""
# 将应用 jar 包拷入 docker
COPY target/powerjob-worker-samples-*.jar /powerjob-worker-samples.jar
# 暴露端口（HTTP + AKKA）
EXPOSE 8081 27777
# 启动应用
ENTRYPOINT ["sh","-c","exec java $JVMOPTIONS -jar /powerjob-worker-samples.jar $PARAMS"]
