import pytest
from regex import find_subscriptions
import pandas as pd

class Tests:
    def test_bad_args(self):
        assert find_subscriptions('sdsdvsvsd') == ''
        assert find_subscriptions('23f22g2wiji') == ''
    
    def test_special_args(self):
        # test for codec interruptions
        assert find_subscriptions('this product is a â\x80\x93 gift card ') == {'is product is a â\x80\x93 gift card '}

    def test_normal_args(self):
        # test for case insensitivity
        assert find_subscriptions('SUBSCRIBE!') == {'SUBSCRIBE!'}
        # test for multiple mentions
        assert find_subscriptions('product free for first month for voucher holders, and free for two months for subscribers') == {'or two months for subscribers', 'for first month for voucher holders, and free f'}
        # test on real data
        # should be captured with $pytest -rP
        df = pd.read_csv('sites_data/initial.csv', index_col='id')
        print(f"\nfind_subscriptions example from Rosslyn Coffee: {find_subscriptions(df.loc[18]['content'])}")
        print(f"\nfind_subscriptions example from Jack the Clipper: {find_subscriptions(df.loc[67]['content'])}")
        print(f"\nfind_subscriptions example from House of Spells: {find_subscriptions(df.loc[99]['content'])}")