FROM md2korg/cerebralcortex:latest

RUN mkdir -p /cc_conf /spark_app
COPY . /spark_app

VOLUME /data /cc_data

WORKDIR /spark_app
CMD ["sh","run.sh"]
