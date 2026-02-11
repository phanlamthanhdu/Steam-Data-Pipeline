FROM eclipse-temurin:17-jdk-jammy

# ===== System =====
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    ca-certificates \
    procps \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# ===== Spark =====
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ===== Hadoop AWS (S3) =====
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar \
      -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.517.jar

# ===== Python libs =====
RUN pip3 install pyspark==3.5.0 pandas pyarrow boto3

WORKDIR /app