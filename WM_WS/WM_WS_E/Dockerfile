FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY . .
RUN mvn -q -pl WM_WS/WM_WS_E -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/WM_WS/WM_WS_E/target/WM_WS_E.jar WM_WS_E.jar
ENTRYPOINT ["java", "-jar", "WM_WS_E.jar"]