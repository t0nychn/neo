import pytest
from regex import find_freebies
import pandas as pd

class Tests:
    def test_bad_args(self):
        assert find_freebies('sdsdvsvsd') is None
        assert find_freebies('23f22g2wiji') is None
    
    def test_special_args(self):
        # test for codec interruptions
        assert find_freebies('this product is a â\x80\x93 gift ') == {'his product is a â\x80\x93 gift '}

    def test_normal_args(self):
        # test for case insensitivity
        assert find_freebies('GIFTCARD!') == {'GIFTCARD!'}
        # test for multiple mentions
        assert find_freebies('product free for first month for non-members, and free for two months for subscribers') == {' non-members, and free for two months for ', 'product free for first month for'}
        # test to avoid gluten
        # function may exclude gluten free gifts but hopefully not too many of those
        assert find_freebies('gluten free bread') is None
        assert find_freebies('our cafe serves muffins free from gluten') is None
        # test to find vouchers
        assert find_freebies('get our new voucher') == {'get our new voucher'}
        assert find_freebies('see our new gift voucher') == {'see our new gift voucher'}
        # test on real data
        # should be captured with $pytest -rP
        df = pd.read_csv('sites_data/initial.csv', index_col='id')
        print(f"\nfind_freebies example from Rosslyn Coffee: {find_freebies(df.loc[18]['content'])}")
        print(f"\nfind_freebies example from Jack the Clipper: {find_freebies(df.loc[67]['content'])}")
        print(f"\nfind_freebies example from House of Spells: {find_freebies(df.loc[99]['content'])}")