FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY . .
RUN mvn -q -pl WM_WS/WM_WS_M -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/WM_WS/WM_WS_M/target/WM_WS_M.jar WM_WS_M.jar
ENTRYPOINT ["java", "-jar", "WM_WS_M.jar"]