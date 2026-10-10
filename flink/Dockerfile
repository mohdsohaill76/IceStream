FROM flink:1.19.2-scala_2.12-java11

USER root

# Install Python 3.11 and system prerequisites
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        python3.11-dev \
        python3.11-venv \
        python3-pip \
        curl \
        ca-certificates && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and Python streaming dependencies
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -m pip install --no-cache-dir \
        apache-flink==1.19.2 \
        python-dotenv==1.0.1 \
        kafka-python==2.0.2 \
        numpy==1.24.4 \
        apache-beam==2.48.0

# Add Kafka SQL connector JAR
RUN mkdir -p /opt/flink/lib /opt/flink/flink_app/lib && \
    curl -fSL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar \
        -o /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar && \
    cp /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar /opt/flink/flink_app/lib/

# Add Kafka proxy helper for internal docker connectivity
COPY kafka_proxy.py /opt/flink/kafka_proxy.py

USER flink
WORKDIR /opt/flink
