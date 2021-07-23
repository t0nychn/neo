import pytest
from regex import find_discount
import pandas as pd

class Tests:
    def test_bad_args(self):
        assert find_discount('sdsdvsvsd') is None
        assert find_discount('/%%%%') is None
    
    def test_special_args(self):
        # test multiple instances in text
        assert find_discount('20%off 50 % off') == {'20%off', '50 % off'}
        # test does not return repeat of the same discount
        assert find_discount('15% off, 15% off') == {'15% off'}
        # test missing numeric parameter
        assert find_discount('% off selected') is None
        # test for codec interruptions
        assert find_discount('10% â\x80\x93 off selected items') == {'10% â\x80\x93 off'}
        assert find_discount('10 â\x80\x93 % â\x80\x93 off  selected items') == {'10 â\x80\x93 % â\x80\x93 off'}

    def test_normal_args(self):
        assert find_discount('receive 15% off your first order') == {'15% off'}
        assert find_discount('policy.25% off your next visit') == {'25% off'}
        # test on real data
        df = pd.read_csv('sites_data/initial.csv')
        assert find_discount(df.iloc[21]['content']) == {'15% off'}
        assert find_discount(df.iloc[35]['content']) == {'25% off'}