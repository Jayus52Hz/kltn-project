FROM apache/airflow:2.11.0

USER root

# Install Java 17 — required by spark-submit (PySpark still runs on JVM)
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
