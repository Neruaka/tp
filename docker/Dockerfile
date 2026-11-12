FROM jupyter/pyspark-notebook:latest

USER root
RUN set -eux; \
    CORE_JAR=$(ls ${SPARK_HOME}/jars/ | grep '^spark-core_'); \
    SCALA_V=$(echo "$CORE_JAR" | sed -E 's/spark-core_([0-9.]+)-.*/\1/'); \
    SPARK_V=$(echo "$CORE_JAR" | sed -E 's/spark-core_[0-9.]+-([0-9.]+)\.jar/\1/'); \
    echo "spark=${SPARK_V} scala=${SCALA_V}"; \
    M=https://repo1.maven.org/maven2; \
    cd ${SPARK_HOME}/jars; \
    wget -q ${M}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_V}/${SPARK_V}/spark-sql-kafka-0-10_${SCALA_V}-${SPARK_V}.jar; \
    wget -q ${M}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_V}/${SPARK_V}/spark-token-provider-kafka-0-10_${SCALA_V}-${SPARK_V}.jar; \
    wget -q ${M}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    wget -q ${M}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
    wget -q ${M}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar; \
    ls -1 ${SPARK_HOME}/jars | grep -E 'kafka|pool2|postgresql'

USER ${NB_UID}

RUN pip install --no-cache-dir \
      'kafka-python>=2.0.3' \
      requests==2.32.3 \
      hdfs==2.7.3 \
      psycopg2-binary==2.9.10

RUN export PYTHONPATH="${SPARK_HOME}/python:$(ls ${SPARK_HOME}/python/lib/py4j-*-src.zip)" && \
    python -c "import pyspark, kafka, hdfs, psycopg2, requests; print('pyspark', pyspark.__version__, '| kafka-python', kafka.__version__)"
