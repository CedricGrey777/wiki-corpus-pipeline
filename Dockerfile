FROM apache/airflow:3.3.1-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=python \
    PYTHONDONTWRITEBYTECODE=1
USER airflow
RUN pip install --no-cache-dir apache-airflow==3.3.1 'pyspark[ml]==3.5.9' numpy==1.26.4 scipy==1.13.1 duckdb==1.4.5 pyarrow==20.0.0 pytest==8.3.5
COPY --chown=airflow:root pyproject.toml /opt/corpus/pyproject.toml
COPY --chown=airflow:root corpus_pipeline /opt/corpus/corpus_pipeline
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root tests /opt/corpus/tests
ENV PYTHONPATH=/opt/corpus
WORKDIR /opt/corpus
RUN mkdir -p /opt/airflow/data
