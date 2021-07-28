import pytest
from regex import find_discounts
import pandas as pd

class Tests:
    def test_bad_args(self):
        assert find_discounts('sdsdvsvsd') is None
        assert find_discounts('/%%%%') is None
    
    def test_special_args(self):
        # test does not return repeat of the same discount
        assert find_discounts('15% off, 15% off') == {'15% off'}
        # test missing numeric parameter
        assert find_discounts('£ off selected') is None
        # test for codec interruptions
        assert find_discounts('£10 â\x80\x93 off selected items') == {'£10 â\x80\x93 off'}
        assert find_discounts('10 â\x80\x93 % â\x80\x93 off  selected items') == {'10 â\x80\x93 % â\x80\x93 off'}
        # test for coffee
        assert find_discounts('£3 coffee') is None

    def test_normal_args(self):
        # test for case insensitivity
        assert find_discounts('£15 OFF!') == {'£15 OFF'}
        assert find_discounts('25% Off Your Next Visit') == {'25% Off'}
        # test for phrase variations
        assert find_discounts('5% Discount') == {'5% Discount'}
        assert find_discounts('10% discount') == {'10% discount'}
        # test multiple instances in text
        assert find_discounts('20%off 50 % off') == {'20%off', '50 % off'}
        assert find_discounts('£20 off and a further 50% off') == {'£20 off', '50% off'}
        assert find_discounts('20% discount and 10% off') == {'20% discount', '10% off'}
        # test on real data
        df = pd.read_csv('sites_data/initial.csv', index_col='id')
        assert find_discounts(df.loc[8]['content']) == {'15% off'}
        assert find_discounts(df.loc[53]['content']) == {'25% off'}