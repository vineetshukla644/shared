FROM openjdk:8-jdk-alpine
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
COPY docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["./docker-entrypoint.sh"]