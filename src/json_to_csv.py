import numpy as np
import pandas as pd
from datetime import datetime

class classificationEncoder():
    '''
    Read json files to pandas, encode with location and search term
    '''

    def __init__(self):
        self.state_dict = {0: 'AR', 1: 'CO', 2: 'OR'}
        self.term_dict = {1: '@joebiden', 2: '@joebiden_#COVID19', 3: '#COVID19',
                        4: '@realdonaldtrump_#COVID19', 5: '@realdonaldtrump'}
    
    def _load_json(self, state_num, term_num):
        self.state_num = state_num
        self.term_num = term_num

        path_to_json = f'../data/{self.state_dict.get(self.state_num)}/{self.term_num}_agg.json'
        self.df = pd.read_json(path_to_json, orient='records', lines=True)

    def _encode(self):
        self.df['search_state'] = self.state_dict.get(self.state_num)
        self.df['search_term'] = self.term_dict.get(self.term_num)

    def build_full_df(self):
        for state_num in self.state_dict.keys():
            for term_num in self.term_dict.keys():
                self._load_json(state_num, term_num)
                self._encode()
                
                if (state_num == 0 and term_num == 1):
                    self.df_all = self.df
                else:
                    self.df_all = pd.concat([self.df_all, self.df], ignore_index=True)

    def to_csv(self, file_path):
        self.df_all.to_csv(file_path)

class csvToDataframePipeline():
    '''
    Expand raw csv to relevant fields
    '''



    def _load_csv(self, raw_csv_file):
        csv_file_path = f'../data/{raw_csv_file}'
        self.raw_df = pd.read_csv(csv_file_path)

    def _make_text_df(self):
        



if __name__ == "__main__":
    encoder = classificationEncoder()
    # encoder.build_full_df()
    # now = str(datetime.now()).replace(' ', '_')
    # encoder.to_csv(f'../data/all_{now}.csv')
    