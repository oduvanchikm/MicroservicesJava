FROM eclipse-temurin:21-jre-ubi9-minimal
WORKDIR /app
COPY build/libs/JavaServiceDeduplication-1.0-SNAPSHOT.jar .
EXPOSE 8080
CMD ["java", "-jar", "JavaServiceDeduplication-1.0-SNAPSHOT.jar"]