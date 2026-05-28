FROM apache/spark:3.4.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates python3.9 python3.9-venv \
    && curl -sS https://bootstrap.pypa.io/pip/3.9/get-pip.py | python3.9 \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages needed by silver_job Pandas UDF
# (must match versions used to train and save the BoW model)
RUN python3.9 -m pip install --no-cache-dir \
    numpy==1.24.4 \
    pandas==2.0.3 \
    pyarrow==12.0.1 \
    scikit-learn==1.3.0 \
    joblib==1.3.2

USER spark
