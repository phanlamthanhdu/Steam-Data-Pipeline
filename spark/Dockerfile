FROM python:3.12-slim

# ===== System deps =====
RUN apt-get update && \
    apt-get install -y curl ca-certificates gnupg && \
    rm -rf /var/lib/apt/lists/*

# ===== Add Temurin APT repo =====
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public \
    | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg

RUN echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" \
    > /etc/apt/sources.list.d/adoptium.list

# ===== Install Java 17 =====
RUN apt-get update && \
    apt-get install -y temurin-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# ===== ENV =====
ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# ===== Python deps =====
COPY spark/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /
