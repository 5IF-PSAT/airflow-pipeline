FROM apache/airflow:2.7.1

# Install dependencies
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    curl \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    libyaml-dev \
    python3-dev \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# Spark files and variables
ENV SPARK_HOME /opt/bitnami/spark
ENV SPARK_VERSION 3.5.0
ENV HADOOP_VERSION 3

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
    wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala2.13.tgz" && \
    tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala2.13.tgz" && \
    mkdir -p "${SPARK_HOME}/bin" && \
    mkdir -p "${SPARK_HOME}/assembly/target/scala-2.13/jars" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala2.13/bin/." "${SPARK_HOME}/bin/" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala2.13/jars/." "${SPARK_HOME}/assembly/target/scala-2.13/jars/" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala2.13.tgz"

# Create SPARK_HOME environment variable
RUN export SPARK_HOME
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

## Switch back to airflow user
## https://airflow.apache.org/docs/docker-stack/build.html#adding-a-new-pypi-package
USER airflow

# Install Python packages
RUN pip install apache-airflow-providers-mongo==2.3.1 apache-airflow-providers-apache-spark==4.4.0 geocoder==1.38.1