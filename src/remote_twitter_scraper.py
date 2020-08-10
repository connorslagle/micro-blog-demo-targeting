from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os
from os import path
import argparse
from twitterInterfacer import TwitterStreamer
from twitterInterfacer import StdOutListener

'''
Script for operating twitter streamer on EC2. Controlled by 'scrape_control.sh' bash file.
'''

parser = argparse.ArgumentParser(description='run tweet grab for cap1')
parser.add_argument('-s','--state',help='select state: 0-OR, 1-CO, 2-AR')
parser.add_argument('-l','--list',help='select search lst:\n0. #COVID19\n1. @joebiden\n2.@realdonaldtrump\n3. COVID+Joe\n4. COVID+Trump')
parser.add_argument('-n','--num',help='select the number of tweets to collect per treatment')
args = parser.parse_args()

# list of locations to search for tweets - pair of lat,long for example [-122.75,36.8,-121.75,37.8] for San Fran
or_latlong = [-124.544499, 42.006346, -116.908916, 45.965382]
co_latlong = [-109.044671,36.999605,-102.070735,40.941074]
ar_latlong = [-94.421464, 33.050564, -90.242844, 36.454143]

state_lst = ['OR','CO','AR']

tweet_file_size = int(args.num)
async_bool = True

latlong_lst = [or_latlong, co_latlong, ar_latlong]

hash_lst = [['#COVID19'], ['@joebiden'], ['@realdonaldtrump'],
             ['#COVID19' '@joebiden'], ['#COVID19' '@realdonaldtrump']]

print(f'In state: {state_lst[int(args.state)]}:\n')

print(f'Collecting: {hash_lst[int(args.list)]}:\n')

hash_filter = hash_lst[int(args.list)]

fetched_tweets_filename = f'../data/{state_lst[int(args.state)]}_{hash_filter}.json'
path_ = path.relpath(fetched_tweets_filename)
i = 0
while path.exists(path_):
    i += 1
    fetched_tweets_filename = f'../data/{state_lst[int(args.state)]}_{hash_filter}_{i}.json'
    path_ = path.relpath(fetched_tweets_filename)


twitter_streamer = TwitterStreamer()
twitter_streamer.stream_tweets(fetched_tweets_filename, hash_filter, latlong_lst[int(args.state)],tweet_file_size, async_bool)