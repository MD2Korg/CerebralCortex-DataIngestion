FROM md2korg/cerebralcortex-kernel:2.4.0

RUN mkdir -p /cc_conf /spark_app
COPY . /spark_app

VOLUME /data /cc_data

HEALTHCHECK --interval=1m --timeout=3s --start-period=30s \
CMD wget --quiet --tries=1 http://localhost:4040/ || exit 1

WORKDIR /spark_app
CMD ["sh","run.sh"]
