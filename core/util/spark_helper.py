
def get_or_create_sc(type="sparkContext", name="CerebralCortex Data Ingestion", enable_spark_ui=True):
    """
    get or create spark context

    Args:
        type (str): type (sparkContext, SparkSessionBuilder, sparkSession, sqlContext). (default="sparkContext")
        name (str): spark app name (default="CerebralCortex-Kernal")

    Returns:

    """
    from pyspark.sql import SQLContext
    from pyspark.sql import SparkSession

    ss = SparkSession.builder
    if name:
        ss.appName(name)

    ss.config("spark.streaming.backpressure.enabled", True)
    ss.config("spark.streaming.backpressure.initialRate", 1)
    ss.config("spark.streaming.kafka.maxRatePerPartition", 2)
    ss.config("spark.sql.session.timeZone", "UTC")

    if enable_spark_ui==False:
        ss.config("spark.ui.enabled", True)

    sparkSession = ss.getOrCreate()

    sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")


    sqlContext = SQLContext(sc)
    if type=="SparkSessionBuilder":
        return sc
    elif type=="sparkContext":
        return sc
    elif type=="sparkSession":
        return sparkSession
    elif type=="sqlContext":
        return sqlContext
    else:
        raise ValueError("Unknown type.")