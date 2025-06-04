# Build stage
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy the entire project
COPY my-data-pipeline ./

# Build the application with executable JAR
RUN mvn clean package -DskipTests -Dmaven.compiler.source=21 -Dmaven.compiler.target=21

# Run stage
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/test-runner/target/test-runner-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"] 