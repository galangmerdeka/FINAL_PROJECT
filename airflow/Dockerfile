FROM apache/airflow:2.4.3-python3.8

USER root

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get install -y ant && \
    apt-get clean &&\
    update-ca-certificates -f;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

WORKDIR /app

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt