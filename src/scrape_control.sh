#!/bin/bash

# Bash script to remotely control 'remote_twitter_scraper.py' for collecting tweets in the cloud.
# two command line arguments:
# 	$1: Length of tweets to collect before changing streaming 'searchterm'
#	$2: Amount of loops to go through entire treatment/state combinations
#
#	Executed  as: ./scrape_control.sh $1 $2, after granting execute permissions with 'chmod +x'


for ((k = 0; k <= $2; k++))
do
	for ((i = 0; i <= 2; i++))
	do
		for ((j = 0; j <= 4; j++))
		do
			python loop_script.py -s $i -l $j -n $1
		done
	done
done