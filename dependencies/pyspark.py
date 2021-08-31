from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


def app(app_nome: str, master='local[*]', jar_packages=[],files=[], spark_config={}):
    '''
        Function to start the spark, define the master and make the settings
    :param app_nome: 
    :param master: 
    :param jar_packages: 
    :param files: 
    :param spark_config: 
    :return: Spark Instance
    '''
    
    sc_spark_session = SparkSession \
            .builder\
            .master(master)\
            .appName(name=app_nome)
    
    #Spark Jar packages
    spark_jars_packages = ','.join(list(jar_packages))
    sc_spark_session.config('spark.jars.packages',spark_jars_packages)


    spark_files = ','.join(list(files))
    sc_spark_session.config('spark.files', spark_files)
    
    # add other config params
    for key, val in spark_config.items():
        sc_spark_session.config(key, val)
    
    sc_spark_session.getOrCreate()

    return sc_spark_session

