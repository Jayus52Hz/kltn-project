FROM apache/spark:3.4.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates python3.9 python3.9-venv \
    && curl -sS https://bootstrap.pypa.io/pip/3.9/get-pip.py | python3.9 \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages needed by silver_job Pandas UDF.
# scikit-learn/joblib are kept for the BoW baseline; torch/transformers are
# required by the RoBERTa fine-tuned model used as the main NLP model.
RUN python3.9 -m pip install --no-cache-dir \
    numpy==1.24.4 \
    pandas==2.0.3 \
    pyarrow==12.0.1 \
    scikit-learn==1.3.0 \
    joblib==1.3.2 \
    torch==2.5.1 \
    transformers==4.45.2 \
    safetensors==0.4.5

USER spark
