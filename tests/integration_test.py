"""
This test is to determine how well the regex functions work together in the Trinity module

When $ pytest tests is called, this file will use the regex modules to parse the
data from sites_data/initial.csv and output deals data onto sites_data/parsed.csv
"""
from regex import *
import pandas as pd

df = pd.read_csv('sites_data/initial.csv', index_col='id', encoding='utf-8')

# initialize Trinity for all content
parsed = df['content'].apply(lambda x: Trinity(x))

df['score'] = [trin.score() for trin in parsed]
df['discounts'] = [trin.disc for trin in parsed]
df['freebies'] = [trin.free for trin in parsed]
df['subsciptions'] = [trin.subs for trin in parsed]

# drop contents column to replicate real csv file
df.drop('content', axis=1, inplace=True)

# export to csv
df.to_csv('sites_data/parsed.csv', encoding='utf-8-sig')