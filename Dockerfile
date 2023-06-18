# 引入openjdk18镜像
FROM snz1dp/openjdk18-springboot-app:1.0

ADD src/main/resources/scripts/entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

# 复制打包好的jar文件到/app目录
COPY target/jdbcrest.jar /app/app.jar
