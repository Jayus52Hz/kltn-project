FROM apache/superset

USER root

RUN pip install --no-cache-dir \
    sqlalchemy-bigquery==1.15.0 \
    pandas-gbq==0.26.1

USER superset
