FROM apache/spark:3.4.0

USER root

# Install Python packages needed by silver_job Pandas UDF
# (must match versions used to train and save the BoW model)
RUN pip3 install --no-cache-dir \
    pandas==2.0.3 \
    scikit-learn==1.3.0 \
    joblib==1.3.2

USER spark
