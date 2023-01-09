# 引入openjdk18镜像
FROM gitlab.snz1.cn:9288/dp/openjdk18-springboot-app:1.0

ADD src/main/resources/scripts/entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

# 复制打包好的jar文件到/app目录
COPY target/jdbcrest.jar /app/app.jar
