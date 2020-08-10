from pipeline_to_pandas import PipelineToPandas
from s3_interfacer import BucketInterfacer
import os

'''
This will grab data from s3 -> spark -> local csv
'''

def aggregate_data_file(path_to_data_folder='../data', state='CO',treatment=0):
    '''
    This function aggregates data files imported from s3 to project data folder.
    '''
    path_to_subfolder = f'{path_to_data_folder}/{state}/{treatment}'
    new_agg_file = f'{path_to_data_folder}/{state}/{treatment}_agg.json'

    file_lst = os.listdir(path_to_subfolder)
    for elem in file_lst:
        with open(f'{path_to_subfolder}/{elem}') as f1:
            with open(new_agg_file,'a') as f2:
                lines = f1.readlines()
                for line in lines:
                    f2.write(line)


if __name__ == "__main__":
    state_list = ['AR', 'CO', 'OR']

    '''
    Imports files on s3 to project data folder.
    '''
    bucket = '-tweet-data-cap1'
    test_object = BucketInterfacer(bucket)
    for state in state_list:
        test_object.retrieve_from_bucket(s3_folder=f'{state}/', file_lst=[], dir_dest=f'../data/', is_file=False)

    '''
    Aggregates streamed json files (~1K tweets each) to 1 file per treatment (~8K)
    '''
    for state in state_list:
        [aggregate_data_file(state=state, treatment=treatment_folder) for treatment_folder in range(1,6)]

    '''
    Loads json files, queries to SQL table, saves as pandas.to_csv()
    '''
    json_list = ['1_agg.json','2_agg.json','3_agg.json','4_agg.json','5_agg.json']

    pipeline = PipelineToPandas()

    for state in state_list:
        for treatment in range(1,6):
            path_to_json = f'../data/{state}/{json_list[treatment-1]}'
            pipeline.spark_df_to_pandas(path_to_json, state, treatment)
            pipeline.save_to_csv(f'../data/{state}_{treatment}.csv')