import pyspark as ps
import pandas as pd
from pyspark.sql.functions import lit

class PipelineToPandas():
    '''
    Class that performs ETL from datalake json file to structured pandas dataframe, then to local csv.
    '''
    def __init__(self):
        self.spark = (ps.sql.SparkSession
         .builder
         .master('local[4]')
         .appName('lecture')
         .getOrCreate()
        )
        self.sc = self.spark.sparkContext

    def spark_df_to_pandas(self, path_to_json, from_state, search_term_key):
        spark_df = self.spark.read.format('json').load(path_to_json)
        self.json_attributes = ['id','user','lang','entities','place','created_at','text','source']
        truncated_spark_df = spark_df[self.json_attributes]
        
        truncated_spark_df = truncated_spark_df.withColumn('state',lit(from_state))
        truncated_spark_df = truncated_spark_df.withColumn('search_term_key',lit(search_term_key))
        truncated_spark_df.createOrReplaceTempView('sql_temp_table')

        self.new_spark_df = self.spark.sql('''
            SELECT 
                id AS tweet_id,
                state,
                search_term_key,
                user.`id` AS user_id,
                created_at AS time_created,
                text AS tweet_text,
                source,
                place.`full_name` AS geo_name,
                place.`id` AS geo_id,
                place.`bounding_box`.`coordinates`[0][0][1] AS geo_coords_SW_lat,
                place.`bounding_box`.`coordinates`[0][0][0] AS geo_coords_SW_long,
                place.`bounding_box`.`coordinates`[0][2][1] AS geo_coords_NE_lat,
                place.`bounding_box`.`coordinates`[0][2][0] AS geo_coords_NE_long,
                entities.`hashtags`[0].`text` AS hash_tag_1,
                entities.`hashtags`[1].`text` AS hash_tag_2,
                entities.`hashtags`[2].`text` AS hash_tag_3,
                entities.`hashtags`[3].`text` AS hash_tag_4,
                entities.`hashtags`[4].`text` AS hash_tags_5,
                entities.`user_mentions`[0].`screen_name` AS user_mentions_1,
                entities.`user_mentions`[1].`screen_name` AS user_mentions_2,
                entities.`user_mentions`[2].`screen_name` AS user_mentions_3,
                entities.`user_mentions`[3].`screen_name` AS user_mentions_4,
                entities.`user_mentions`[4].`screen_name` AS user_mentions_5,
                user.`created_at` AS user_date_created,
                user.`location` AS location,
                user.`description` AS description
            FROM sql_temp_table
            WHERE
                lang = 'en'
            ''')
        self.pandas_df = self.new_spark_df.toPandas()

    def save_to_csv(self, path_to_csv):
        self.pandas_df.to_csv(path_to_csv, encoding='utf-8')



if __name__ == "__main__":

    print('hi')
    # pipeline = PipelineToPandas()
    # path_to_json
    # pipeline.load_to_spark_df('../zip_data/data/test.json')
    # pipeline.truncate_spark_df('co', 0)

    # tweet_df = pipeline.spark_df_to_pandas()
    # print(tweet_df.head(5))

    # # IFNEM block works, 4/7/10 @10