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

# Set JVM options for 4GB RAM limit
ENV JAVA_OPTS="-Xms1g -Xmx3g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"

# Run Main class with MODE environment variable
CMD ["sh", "-c", "java $JAVA_OPTS -cp app.jar com.example.pipeline.Main $MODE"] 