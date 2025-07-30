# 使用官方的openjdk:8-jre镜像作为基础镜像
FROM openjdk:8-jre

# 将web模块生成的jar包复制到镜像中，并设置为可执行
COPY target/data-push-web-1.0-SNAPSHOT.jar /app/push-web.jar

# 设置工作目录
WORKDIR /app

# 暴露应用的端口号，假设web应用使用8080端口
EXPOSE 8080

# 启动应用的命令，使用java命令运行jar包
ENTRYPOINT ["java","-jar","push-web.jar"]