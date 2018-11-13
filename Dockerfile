FROM md2korg/cerebralcortex:2.3.0

RUN mkdir -p /cc_conf /spark_app
COPY . /spark_app

VOLUME /data /cc_data

WORKDIR /spark_app
CMD ["sh","run.sh"]
