"""
This test is to determine how well the regex modules work together

When $ pytest tests is called, this file will use the regex modules to parse the
data from sites_data/initial.csv and output deals data onto sites_data/parsed.csv
"""
from regex import *
import pandas as pd
import math


# define parsing logic
def parse(text):
    """Parses a string input and returns deals found by regex module with confidence score

    On calculating score:
    • discounts are the most indicative of potential offer so score is weighted 2x that of freebies
    • sigma_d is total unique discounts found
    • sigma_f is total unique 'free' matches found
    • logging the sum of 4 : 1 sigma_d : sigma_f weighting means single mentions of 'free' or 'gift' produce score of zero, but single mentions of discounts produce score of 6

    Args:
        text (str): String to be parsed

    Returns:
        dict: {'score': x, 'discounts': y, 'freebies': z}
    """
    discounts = find_discounts(text)
    freebies = find_freebies(text)

    if discounts:
        sigma_d = len(discounts)
    else:
        sigma_d = 0
    if freebies:
        sigma_f = len(freebies)
    else:
        # ensure no math error (score will still be zero)
        sigma_f = 1
    score = 10 * math.log(4 * sigma_d + sigma_f)

    return {'score': int(score), 'discounts': discounts, 'freebies': freebies}


df = pd.read_csv('sites_data/initial.csv', index_col='id', encoding='utf-8')
# clean df of unwanted escape \n characters
df['content'] = df['content'].apply(lambda x: x.replace('\n', ''))

# iterate through df to get array of dicts
parsed = df['content'].apply(lambda x: parse(x))

# iterate through parsed to get lists of score, discounts and freebies
scores = []
discounts = []
freebies = []
for result in parsed:
    scores.append(result['score'])
    discounts.append(result['discounts'])
    freebies.append(result['freebies'])

# create new columns from parsed lists
df['score'] = scores
df['discounts'] = discounts
df['freebies'] = freebies

# drop contents column to replicate real csv file
df.drop('content', axis=1, inplace=True)

# export to csv
df.to_csv('sites_data/parsed.csv', encoding='utf-8-sig')