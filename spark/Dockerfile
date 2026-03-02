FROM apache/spark:3.5.1

USER root

# Install curl
RUN apt-get update && apt-get install -y curl

ENV SPARK_HOME=/opt/spark


# Add PostgreSQL JDBC driver
RUN curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
    
# ===== Hadoop AWS (S3) =====
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar \
      -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.517.jar

ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /app