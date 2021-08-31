from dependencies.pyspark import app
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from loguru import logger

def pipeline() -> None:
    '''
        Function that makes pipeline calls
    '''
    spark = app('Tweets')
    logger.start("Iniciando a leitura dos dados")
    data = data_read(spark, '../data/Tweets.csv')
    data_transformed = data_transformation(spark, data)
    data_transformed.show()
    logger.success("Transformação realizada")


def data_transformation(spark : SparkSession.Builder,data_frame: pyspark.sql.dataframe.DataFrame)->pyspark.sql.dataframe.DataFrame:
   '''
   Function to perform data transformation, vectoring and one hot encoding
   :param spark: Spark session
   :param data_frame: DataFrame data
   :return: dataframe
   '''
   #Data Cleaning
   df_step1 = data_frame.drop(*['airline_sentiment_gold','negativereason_gold','tweet_coord','tweet_location'])
   df_step1.registerTempTable('TWEETS')
   df_step2 = spark.getOrCreate().sql('''SELECT * FROM TWEETS WHERE airline_sentiment IN ('neutral','positive','negative')''')

   logger.success('Transformações 1 e 2 executadas com sucesso')
   #Data Transform
   indexers = [StringIndexer(inputCol=column, outputCol=column + "_NUMERIC").fit(df_step2) for column in
                ['airline', 'airline_sentiment']]
   pipeline = Pipeline(stages=indexers)
   df_step3 = pipeline.fit(df_step2).transform(df_step2)

   #One Hot Encoder
   onehotencoder_qualification_vector = OneHotEncoder(inputCol="airline_NUMERIC", outputCol="airline_vec")
   df_step4 = onehotencoder_qualification_vector.fit(df_step3).transform(df_step3)
   data_write(df_step2, 'df_step2')
   data_write(df_step3, 'df_step3')
   df_step4.write.parquet("../output/df_step4.parquet", mode='overwrite')
   logger.success('Transformações 3 e 4 executadas com sucesso')
   return df_step4


def data_write(data_frame: pyspark.sql.dataframe.DataFrame, file_name: str)-> None:
    '''
    :param spark:
    :param file_name:
    :return:
    '''

    data_frame\
    .coalesce(1)\
    .write.format('csv')\
    .csv(f'../output/{file_name}.csv', mode='overwrite', header=True, encoding='UTF-8')


def data_read(spark: SparkSession.Builder, file_name: str) -> pyspark.sql.dataframe.DataFrame:
    '''
           Load data csv format.

    :param file_name: Name csv file
    :param spark: Spark session
    :return: Spark DataFrame
    '''
    df = (
        spark
            .getOrCreate()
            .read
            .format('csv')
            .option('header', 'true')
            .load(file_name)
    )
    return df


if __name__ == '__main__':
    pipeline()

