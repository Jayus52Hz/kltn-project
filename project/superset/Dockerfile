FROM apache/superset

USER root

RUN uv pip install --python /app/.venv/bin/python --no-cache \
    sqlalchemy-bigquery==1.15.0 \
    pandas-gbq==0.26.1

COPY create_telesales_dashboard.py /app/create_telesales_dashboard.py

USER superset
