import pyspark as ps
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql.functions import lit
from bs4 import BeautifulSoup

class pipelineToPandas():
    '''
    Class that performs ETL from datalake json file to structured pandas dataframe, then to local csv.
    '''
    def __init__(self):
        self.spark = (ps.sql.SparkSession
         .builder
         .master('local[4]')
         .appName('lecture')
         .config('spark.debug.maxToStringFields', 1000)
         .getOrCreate()
        )
        # self.spark.conf.set("spark.debug.maxToStringFields", 1000)
        self.sc = self.spark.sparkContext
        self.state_dict = {0: 'AR', 1: 'CO', 2: 'OR'}
        self.term_dict = {1: '@joebiden', 2: '@joebiden_#COVID19', 3: '#COVID19',
                        4: '@realdonaldtrump_#COVID19', 5: '@realdonaldtrump'}
        self.base_path = '../data/'

    def _spark_df_to_pandas(self, from_state_key, search_term_key):
        path_to_json = f'{self.base_path}{self.state_dict.get(from_state_key)}/{search_term_key}_agg.json'

        spark_df = self.spark.read.format('json').load(path_to_json)
        self.json_attributes = ['id','display_text_range','user','lang','created_at','text','source']
        truncated_spark_df = spark_df[self.json_attributes]
        
        truncated_spark_df = truncated_spark_df.withColumn('state',lit(self.state_dict.get(from_state_key)))
        truncated_spark_df = truncated_spark_df.withColumn('search_term_key',lit(self.term_dict.get(search_term_key)))
        truncated_spark_df.createOrReplaceTempView('sql_temp_table')

        new_spark_df = self.spark.sql('''
            SELECT 
                id AS tweet_id,
                state,
                search_term_key,
                created_at AS tweet_time,
                text AS tweet_text,
                display_text_range AS tweet_text_range,
                source,
                user.`id` AS user_id,
                user.`created_at` AS user_date_created,
                user.`location` AS location,
                user.`description` AS description,
                user.`verified` AS user_verified
            FROM sql_temp_table
            WHERE
                lang = 'en'
            ''')
        self.pandas_df = new_spark_df.toPandas()

    def load_multiple_files(self, all=True):
        if all:
            for state_key in self.state_dict.keys():
                for term_key in self.term_dict.keys():
                    self._spark_df_to_pandas(state_key, term_key)
                    if (state_key == 0 and term_key == 1):
                        self.df_all = self.pandas_df
                    else:
                        self.df_all = pd.concat([self.df_all, self.pandas_df], ignore_index=True)

    def clean_df(self, all=True):
        '''
        Clean df. This includes,
            dropping duplicate rows (based on tweet_id)
            eliminated mentions from tweet text
            format source from html to iPhone, Android, Web, Other
        '''
        if all:
            # dropping duplicates
            self.df_all.drop_duplicates(subset='tweet_id', ignore_index=True, inplace=True)

            # Eliminating mentions, dropping 
            range_as_tuple = self.df_all['tweet_text_range'].apply(self._format_text_range) 
            self.df_all['tweet_text_wo_mentions'] = self._no_mentions_text(
                range_as_tuple,
                self.df_all['tweet_text']
            )
            self.df_all.drop(['tweet_text', 'tweet_text_range'], axis=1, inplace=True)  

            # formatting source
            self.df_all['source'].apply(lambda x: BeautifulSoup(x).find('a').getText())

    def _format_text_range(self, raw_range):
        '''
        Formats str tweet_range as tuple if present, otherwise leave as nan
        '''
        if type(raw_range) == str: 
            start, end = ''.join([char for char in raw_range if char not in ['[', ',', ']']]).split(' ')
            return (int(start), int(end))
        else:
            return raw_range

    def _no_mentions_text(self, tup_range_series, raw_tweet_text_series):
        wo_mentions = np.empty(raw_tweet_text_series.shape, dtype='U256')
        for idx, tup in enumerate(tup_range_series):
            if type(tup) == tuple:
                wo_mentions[idx] = raw_tweet_text_series[idx][tup[0]:tup[1]]
            else:
                wo_mentions[idx] = raw_tweet_text_series[idx]
        return wo_mentions
    
    def save_to_csv(self, csv_file_name, all=True):
        now_date = str(datetime.now()).split(' ')[0]
        file_path = f'{self.base_path}{now_date}_{csv_file_name}'
        if all:
            self.df_all.to_csv(file_path, index=False)
        else:
            self.pandas_df.to_csv(file_path, index=False)
        

if __name__ == "__main__":

    print('hi')
    pipeline = pipelineToPandas()
    pipeline.load_multiple_files()