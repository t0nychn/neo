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
    """Parses a string input and returns deals found by regex modules with confidence/compatibility score

    Updated score calculation (02/08/2021):
    • discounts are the most indicative of potential offer so score is weighted 9x that of subs
    • freebies are more useful than subscriptions so weighted 3x that of subs
    • sigma_d is total unique discounts found
    • sigma_f is total unique freeby matches found containing 'free'/'gift'/'voucher' (check out regex package to see additional checks in place)
    • sigma_s is total unique subs found

    Args:
        text (str): String to be parsed

    Returns:
        dict: {'score': x, 'discounts': y, 'freebies': z}
    """
    discounts = find_discounts(text)
    freebies = find_freebies(text)
    subs = find_subs(text)

    sigma_d = (len(discounts) if discounts else 0)
    sigma_f = (len(freebies) if freebies else 0)
    sigma_s = (len(subs) if subs else 0)

    score = 9 * sigma_d + 3 * sigma_f + sigma_s

    return {'score': int(score), 'discounts': discounts, 'freebies': freebies, 'subs': subs}


df = pd.read_csv('sites_data/initial.csv', index_col='id', encoding='utf-8')

# iterate through df to get array of dicts
parsed = df['content'].apply(lambda x: parse(x))

# iterate through parsed to get lists of score, discounts and freebies
scores = []
discounts = []
freebies = []
subs = []
for result in parsed:
    scores.append(result['score'])
    discounts.append(result['discounts'])
    freebies.append(result['freebies'])
    subs.append(result['subs'])

# create new columns from parsed lists
df['score'] = scores
df['discounts'] = discounts
df['freebies'] = freebies
df['subs'] = subs

# drop contents column to replicate real csv file
df.drop('content', axis=1, inplace=True)

# export to csv
df.to_csv('sites_data/parsed.csv', encoding='utf-8-sig')