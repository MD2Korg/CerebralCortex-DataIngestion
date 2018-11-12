FROM ubuntu:18.04
MAINTAINER Timothy Hnat twhnat@memphis.edu

RUN apt-get update \
  && apt-get install -yqq wget git python3-pip  openjdk-8-jre python3-setuptools libyaml-dev libev-dev liblapack-dev \
  && pip3 install --upgrade --force-reinstall pip==9.0.3 \
  && pip3 install cython

# Spark dependencies
ENV APACHE_SPARK_VERSION 2.3.2
ENV HADOOP_VERSION 2.7

RUN cd /tmp && \
        wget -q http://apache.cs.utah.edu/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
        tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local && \
        rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

ENV SPARK_HOME  /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip
# ENV SPARK_OPTS --driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info
ENV JAVA_HOME   /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH        $JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON python3


# Install Cerebral Cortex libraries for use in the notebook environment
RUN git clone https://github.com/MD2Korg/CerebralCortex -b 2.3.0 \
    && cd CerebralCortex \
    && /usr/local/bin/pip3 install -r requirements.txt \
    && python3 setup.py install \
    && cd .. && rm -rf CerebralCortex

RUN mkdir -p /data /cc_conf /spark_app

COPY . /spark_app


VOLUME /data
