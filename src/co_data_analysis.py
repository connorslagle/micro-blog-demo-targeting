import pandas as pd
import numpy as np
import os
import re
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('seaborn-darkgrid')
plt.rcParams.update({'font.size': 20})

from plotting_functions import make_boxplot, save_fig, make_hist, make_ci_lineplot, make_hor_barchart
from TextCleaner import TextCleaner
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def to_count_list(input_lst, sorted_by_vals_desc=True):
    '''
    Counts words in word list, converts to count dictionary, then outputs
    words (keys) and counts (values)
    '''
    new_dict ={}
    for word in input_lst:
        if word not in new_dict:
            new_dict[word] = 1
        else:
            new_dict[word] += 1
    
    if sorted_by_vals_desc:
        keys = sorted(new_dict, key=new_dict.__getitem__, reverse=True)
        values = [new_dict[elem] for elem in keys]
    else:
        keys = list(new_dict.keys())
        values = list(new_dict.values())

    return keys, values


def tweet_col_to_vader_df(analyzer_object, tweet_col):
    '''
    Iterates through tweet column of df, applying VADER algorithm and exporting as new df.
    '''
    vader_df = pd.DataFrame()
    vader_df['tweet'] = tweet_col

    sentiment_keys = ['neg', 'pos', 'neu', 'compound']

    for key in sentiment_keys:
        vader_df[key] = [analyzer_object.polarity_scores(str(tweet))[key] for tweet in vader_df['tweet']]
    return vader_df
    
def bootstrap(x, resamples=1000):
    """Draw bootstrap resamples from the array x.

    Parameters
    ----------
    x: np.array, shape (n, )
      The data to draw the bootstrap samples from.
    
    resamples: int
      The number of bootstrap samples to draw from x.
    
    Returns
    -------
    bootstrap_samples: np.array, shape (resamples, n)
      The bootsrap resamples from x.
    """
    bootstrap_samples = []

    for _ in range(resamples):
        bootstrap = np.random.choice(x, size=len(x), replace=True)
        bootstrap_samples.append(bootstrap)

    return bootstrap_samples

def make_sent_sensitivity_array (text_df, treatment, sentiment_type, num_resamples, total_cleaning_steps_list=range(1,6)):
    '''
    Performs iterative tweet pre-processing, scores with VADER algorithm and computed the bootstrapped sample means.
    '''
    sample_means_array = np.zeros((num_resamples, len(total_cleaning_steps_list)))

    for step in total_cleaning_steps_list:
        clean_tweet_col = text_df[treatment].apply(lambda x: cleaner.clean_tweets(x,last_clean_step=step))
        vader_df = tweet_col_to_vader_df(analyzer, clean_tweet_col)

        bootstrap_samples = bootstrap(vader_df[sentiment_type],resamples=num_resamples)
        bootstrap_sample_means = list(map(lambda x: np.mean(x), bootstrap_samples))
        # bootstrap_sample_means = list(np.array(bootstrap_sample_means) / (num_resamples**0.5))

        sample_means_array[:,step-1] = bootstrap_sample_means
    return sample_means_array

if __name__ == '__main__':
    '''
    Imports all csv files for given state for analysis.
    '''
    state = 'CO'
    treatment_dict = {1: '@joebiden', 2: '@joebiden + #COVID19', 3: '#COVID19',
                      4: '@realdonaldtrump + #COVID19', 5: '@realdonaldtrump'}
    colorado_dict = {}

    for treatment in range(1,6):
        df = pd.read_csv(f'../data/{state}_{treatment}.csv', engine='python')

        raw_tweets = df['tweet_text']

        colorado_dict[treatment_dict[treatment]] = raw_tweets

    colorado_df = pd.DataFrame.from_dict(colorado_dict)

    cleaner = TextCleaner()

    '''
    Plotting fig 1: Raw (unprocessed tweets)
    '''
    fig, ax = plt.subplots(3,1, figsize=(10,22), sharex=True)

    fig_pos_lst = [[0.25, 0.67, 0.7, 0.30],
                  [0.25, 0.35, 0.7, 0.30],
                  [0.25, 0.03, 0.7, 0.30]]


    for i, treatment in enumerate(['@joebiden', '#COVID19', '@realdonaldtrump']):
        clean_tweet_col = colorado_df[treatment].apply(lambda x: cleaner.clean_tweets(x))
        total_tweet_string = clean_tweet_col.str.cat(sep=' ')
        total_tweet_list = total_tweet_string.split()
        words, counts = to_count_list(total_tweet_list)
        if i == 2:
            make_hor_barchart(ax[i],fig_pos_lst[i],words[:25][::-1],counts[:25][::-1],'Relative Frequency (a.u.)',f'Top 25 Words for {treatment}')
        else:
            make_hor_barchart(ax[i],fig_pos_lst[i],words[:25][::-1],counts[:25][::-1],'',f'Top 25 Words for {treatment}')
    save_fig(fig, f'{state}_single_word_cleaned_bar.png')

    fig, ax = plt.subplots(2,1, figsize=(10,22), sharex=True)

    fig_pos_lst = [[0.10, 0.51, 0.8, 0.30],
                  [0.10, 0.18, 0.8, 0.30]]


    for i, treatment in enumerate(['@joebiden + #COVID19', '@realdonaldtrump + #COVID19']):
        clean_tweet_col = colorado_df[treatment].apply(lambda x: cleaner.clean_tweets(x))

        total_tweet_string = clean_tweet_col.str.cat(sep=' ')
        total_tweet_list = total_tweet_string.split()
        words, counts = to_count_list(total_tweet_list)

        if i == 1:
            make_hor_barchart(ax[i],fig_pos_lst[i],words[:25][::-1],counts[:25][::-1],'Relative Frequency (a.u.)',f'Top 25 Words for {treatment}')
        else:
            make_hor_barchart(ax[i],fig_pos_lst[i],words[:25][::-1],counts[:25][::-1],'',f'Top 25 Words for {treatment}')
    save_fig(fig,f'{state}_multi_word_clean_bar.png')

    '''
    VADER analysis on RAW tweets, Figure 2
    '''
    analyzer = SentimentIntensityAnalyzer()

    # for i, treatment in enumerate(treatment_dict.values()):
    #     fig, ax = plt.subplots(1,figsize=(8,6))
    #     vader_df = tweet_col_to_vader_df(analyzer, colorado_df[treatment])
    #     make_hist(ax, treatment, vader_df['compound'], (0,4000), 'Compound Sentiment', (-1, 1))
    #     save_fig(fig, f'{state}_{i}_raw_compound_sentiment.png')

    '''
    Bootstrap raw tweets, Figure 3
    '''
    # fig, ax = plt.subplots(1,figsize=(12,8))

    # sentiment_parameter = 'pos'

    # for i, treatment in enumerate(treatment_dict.values()):

    #     vader_df = tweet_col_to_vader_df(analyzer, colorado_df[treatment])

    #     bootstrap_samples = bootstrap(vader_df[sentiment_parameter],resamples=1000)
    #     bootstrap_sample_means = list(map(np.std, bootstrap_samples))

    #     make_hist(ax, treatment, bootstrap_sample_means, (0,140), 'Std. Positive Sentiment', (0, 1))
    # save_fig(fig, f'test_images/{state}_all_raw__std_{sentiment_parameter}_sentiment.png')

    '''
    preprocess tweets, perform VADER analysis, and make sensitivity boxplots Figures 4,5
    '''
    
    num_resamples = 1000
    total_cleaning_steps = range(7)

    # new_lst = ['#COVID19']

    # for treatment in new_lst:
    #     two_dim_array = make_sent_sensitivity_array(colorado_df, treatment, 'compound', num_resamples, total_cleaning_steps)

    #     # make boxplots
    #     fig, ax = plt.subplots(1,figsize=(8,6))
    #     make_boxplot(ax,two_dim_array,(-0.045,0.055),total_cleaning_steps,'Mean Compound Sentiment',title=f'Text Cleaning Sensitivity for\n{treatment}')
    # #     save_fig(fig,f'{state}_2_mean_compound_boxplot.png')

    # Replace boxplots with shadded line plots

    '''
    Removal steps:
    0 - nothing
    1 - lowercase
    2 - eliminate cultural abbreviations
    3 - eliminate grammatical abbreviationnp.std(x)/(num_resamples**0.5)
    4 - Removing Punctuations
    5 - removing special characters
    6 - removing stop words
    '''

    # vader_param_dict = {'pos':[(0.05, 0.25), 'Positive Proportion'],'neg': [(0.04, 0.18), 'Negative Proportion'], 'compound':[(-0.05, 0.30), 'Compound Score']}

    # for abbrev, lst in vader_param_dict.items():
    #     fig, ax = plt.subplots(1,figsize=(10,8))

    #     for key, treatment in treatment_dict.items():
    #         two_dim_array = make_sent_sensitivity_array(colorado_df, treatment, abbrev, num_resamples, total_cleaning_steps)
    #         make_ci_lineplot(ax, treatment,two_dim_array,lst[0], total_cleaning_steps, f'Mean {lst[1]} (95% CI)',title=f'{lst[1]} vs Text Pre-processing')

    #     save_fig(fig, f'{state}_all_combo_{abbrev}_shaded_line_plot.png')
    '''
    Hypothesis tests, mean and variance - Think about some kind of Bayesian test
    '''

    # Test on means


