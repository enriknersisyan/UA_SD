FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY . .
RUN mvn -q -pl WM_Central -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/WM_Central/target/WM_Central.jar WM_Central.jar
RUN mkdir -p /data
ENV WM_DB_PATH=/data/wm_central.basedatos
ENV SOCKET_PORT=5000
EXPOSE 5000
ENTRYPOINT ["sh", "-c", "exec java -jar WM_Central.jar ${SOCKET_PORT} ${KAFKA_HOST} ${KAFKA_PORT}"]