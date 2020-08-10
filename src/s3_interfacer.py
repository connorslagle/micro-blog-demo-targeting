import boto3
import botocore
import os

class BucketInterfacer():
    '''
    Class that interfaces with my local environment and S3 for my AWS account.
    '''
    def __init__(self, bucket):
        self.boto3_connection = boto3.resource('s3')
        self.username = 'conslag'
        self.client = boto3.client('s3')
        self.bucket_name = self.username + bucket

    def create_bucket(self,bucket):
        self.bucket_name = self.username + bucket
        self.boto3_connection.create_bucket(Bucket=self.bucket_name)

    def send_to_bucket(self, s3_folder='', file_lst=[], path_to_dir='../data/', is_file=True):
        '''
        Method to send file (or directory of files) to s3 bucket.     
        '''
        file_lst_in_dir = os.listdir(path_to_dir)
        try:
            if is_file:
                [self.client.upload_file(f'{path_to_dir}{elem}', self.bucket_name, f'{s3_folder}{elem}') for elem in file_lst]
            else:
                [self.client.upload_file(f'{path_to_dir}{elem}', self.bucket_name, f'{s3_folder}{elem}') for elem in file_lst_in_dir]

        except BaseException as e:
            print(e)



    def retrieve_from_bucket(self, s3_folder='', file_lst=[], dir_dest='../data/', is_file=True):
        '''
        Method that retrieves file (or all bucket contents) from s3 bucket.
        '''

        bucket_elements = [elem['Key'] for s3_file in self.client.get_paginator("list_objects_v2")\
                         .paginate(Bucket=self.bucket_name,Prefix=s3_folder) for elem in s3_file['Contents']]
        bucket_elements = bucket_elements[1:]

        try:
            if is_file:
                [self.client.download_file(self.bucket_name, f'{s3_folder}{elem}', f'{dir_dest}{elem}') for elem in file_lst]
            else:
                [self.client.download_file(self.bucket_name, f'{elem}', f'{dir_dest}{elem}') for elem in bucket_elements]

        except BaseException as e:
            print(e)
    


if __name__ == "__main__":
    
    bucket = '-tweet-data-cap1'
    test_object = BucketInterfacer(bucket)

    test_object.send_to_bucket(file_lst=['del_me.txt'], path_to_dir='./', is_file=True)

    # loads all files from specified bucket
    # test_object.retrieve_from_bucket(s3_folder='test/', file_lst=['del_me.txt'])
