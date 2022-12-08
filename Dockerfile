# 引入openjdk18镜像
FROM gitlab.snz1.cn:9288/dp/openjdk18-springboot-app:1.0

# 复制打包好的jar文件到/app目录
COPY target/dashboard.jar /app/app.jar
