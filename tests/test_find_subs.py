import pytest
from regex import find_subs
import pandas as pd

class Tests:
    def test_bad_args(self):
        assert find_subs('sdsdvsvsd') is None
        assert find_subs('23f22g2wiji') is None
    
    def test_special_args(self):
        # test for codec interruptions
        assert find_subs('this product is a â\x80\x93 giftcard ') == {'is product is a â\x80\x93 giftcard '}

    def test_normal_args(self):
        # test for case insensitivity
        assert find_subs('SUBSCRIBE!') == {'SUBSCRIBE!'}
        # test for multiple mentions
        assert find_subs('product free for first month for voucher holders, and free for two months for subscribers') == {'or two months for subscribers', 'for first month for voucher holders, and free f'}
        # test on real data
        # should be captured with $pytest -rP
        df = pd.read_csv('sites_data/initial.csv', index_col='id')
        print(f"\nfind_subs example from Rosslyn Coffee: {find_subs(df.loc[18]['content'])}")
        print(f"\nfind_subs example from Jack the Clipper: {find_subs(df.loc[67]['content'])}")
        print(f"\nfind_subs example from House of Spells: {find_subs(df.loc[99]['content'])}")