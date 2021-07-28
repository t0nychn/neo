"""This package contains regex modules that will help identify deal info in sites"""

import re


def find_discounts(text):
    """Identifies % discount in '£x off', 'x% off' or 'x% discount' form and returns matching set of cases

    Args:
        text (str): Text to be parsed
    
    Returns:
        Matching set of string sequences if discount found, else None.
    """

    # remember to account for '£x coff' i.e. coffee
    pattern = r"£\d{1,3}[^£C]{0,5}off|\d{1,2}[^%]{0,5}%[^%]{0,5}off|\d{1,2}[^%]{0,5}%[^%]{0,5}discount"
    result = re.findall(pattern, text, re.I)

    if result:
        return set(result)
    else:
        return


def find_freebies(text):
    """Identifies freebies in text and returns matching set of cases.
    Much more general than find_discount, so confidence in result will also be lower.

    Args:
        text (str): Text to be parsed
    
    Returns:
        Matching set of string sequences if discount found, else None.
    """

    # aim is to also return context 20 characters before and after mention of free, gift or voucher
    # prevent parsing words with 'free' and 'gift' as root (e.g. freedom or gifted) but allow for giftcard
    pattern1 = r".{0,19}\Wfree\W.{0,19}|.{0,20}voucher.{0,20}"
    pattern2 = r".{0,20}giftcard.{0,20}|.{0,20}\Wgift\W.{0,29}"
    result = re.findall(pattern1, text, re.I) + re.findall(pattern2, text, re.I)

    if result:
        # filter for mentions of gluten & range (common word associated with free) and 'gift wrap'
        # create new set of items to be removed (can't remove iteratively since index positions change)
        remove = set()
        for e in result:
            # account for case differences by creating lowercase comparison text
            comp = e.lower()
            if ('gift' and 'wrap') in comp:
                remove.add(e)
            if 'gluten' in comp or 'range' in comp:
                remove.add(e)
        for e in remove:
            result.remove(e)
        if len(result) > 0:
            return set(result)
        else:
            return
    else:
        return