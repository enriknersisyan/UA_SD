FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY . .
RUN mvn -q -pl WM_FO -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/WM_FO/target/WM_FO.jar WM_FO.jar
ENTRYPOINT ["java", "-jar", "WM_FO.jar"]