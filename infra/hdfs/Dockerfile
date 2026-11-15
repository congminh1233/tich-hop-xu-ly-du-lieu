FROM eclipse-temurin:11-jre-jammy
ARG LOCAL_UID=1000
ARG LOCAL_GID=1000
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/tmp/hadoop-logs \
    HADOOP_PID_DIR=/tmp/hadoop-pids \
    HADOOP_HEAPSIZE_MAX=512 \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    SPARK_HOME=/opt/venv/lib/python3.10/site-packages/pyspark \
    LANG=C.UTF-8
ENV PATH=/opt/venv/bin:/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash curl ca-certificates python3 python3-venv tini procps \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fL --retry 5 --connect-timeout 30 \
      https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz \
      -o /tmp/hadoop.tar.gz \
    && printf '%s  %s\n' \
      ca5e12625679ca95b8fd7bb7babc2a8dcb2605979b901df9ad137178718821097b67555115fafc6dbf6bb32b61864ccb6786dbc555e589694a22bf69147780b4 \
      /tmp/hadoop.tar.gz | sha512sum -c - \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv /opt/hadoop-3.3.4 /opt/hadoop \
    && rm /tmp/hadoop.tar.gz
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir pyspark==3.5.7 pyarrow==19.0.1 numpy==1.26.4 \
    && /opt/venv/bin/python -c 'import sys,pyspark; assert sys.version_info[:2] == (3,10); assert pyspark.__version__ == "3.5.7"'
RUN groupadd --gid ${LOCAL_GID} hadoop \
    && useradd --uid ${LOCAL_UID} --gid ${LOCAL_GID} --create-home --shell /bin/bash hadoop \
    && mkdir -p /data/name /data/data /tmp/hadoop-logs /tmp/hadoop-pids /opt/it5427 \
    && chown -R hadoop:hadoop /data /tmp/hadoop-logs /tmp/hadoop-pids
COPY conf/ /opt/hadoop/etc/hadoop/
COPY jobs/ /opt/it5427/
COPY entrypoint.sh /opt/it5427/entrypoint.sh
RUN sed -i 's/\r$//' /opt/it5427/entrypoint.sh \
    && chmod 755 /opt/it5427/entrypoint.sh
USER hadoop
WORKDIR /opt/it5427
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/it5427/entrypoint.sh"]
