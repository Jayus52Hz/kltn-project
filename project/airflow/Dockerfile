FROM apache/airflow:2.11.0-python3.9

USER root

# Install Java 17 — required by spark-submit (PySpark still runs on JVM)
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyspark==3.4.0 \
    numpy==1.24.4 \
    pandas==2.0.3 \
    pyarrow==12.0.1 \
    scikit-learn==1.3.0 \
    joblib==1.3.2
