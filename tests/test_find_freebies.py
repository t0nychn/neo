import pytest
from regex import find_freebies
import pandas as pd

class Tests:
    def test_bad_args(self):
        assert find_freebies('sdsdvsvsd') == ''
        assert find_freebies('23f22g2wiji') == ''
    
    def test_special_args(self):
        # test for codec interruptions
        assert find_freebies('this product is a â\x80\x93 gift ') == {'is product is a â\x80\x93 gift '}

    def test_normal_args(self):
        # test for case insensitivity
        assert find_freebies(' GIFT!') == {' GIFT!'}
        # test for multiple mentions
        assert find_freebies('product free for first month for non-members, and free for two months for subscribers') == {' non-members, and free for two months for ', 'product free for first month for'}
        # test to avoid gluten
        # function may exclude gluten free gifts but hopefully not too many of those
        assert find_freebies('gluten free bread ................. free range egg.......... gluten free range bread') == ''
        assert find_freebies('our cafe serves muffins free from gluten') == ''
        # test on real data
        # should be captured with $pytest -rP
        df = pd.read_csv('sites_data/initial.csv', index_col='id')
        print(f"\nfind_freebies example from Rosslyn Coffee: {find_freebies(df.loc[18]['content'])}")
        print(f"\nfind_freebies example from Jack the Clipper: {find_freebies(df.loc[67]['content'])}")
        print(f"\nfind_freebies example from House of Spells: {find_freebies(df.loc[99]['content'])}")