FROM eclipse-temurin:11
VOLUME /tmp
COPY target/kafka-0.0.1-SNAPSHOT.jar ${JAR_FILE}kafka-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","kafka-0.0.1-SNAPSHOT.jar"]