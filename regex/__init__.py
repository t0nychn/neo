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

    # aim is to also return context 20 characters before and after mention
    # prevent parsing words with 'free' and 'gift' as root (e.g. freedom or gifted)
    pattern = r".{0,19}\Wfree\W.{0,19}|.{0,19}\Wgift\W.{0,19}"
    result = re.findall(pattern, text, re.I)

    if result:
        # create new set of items to be removed (can't remove iteratively since index positions change)
        filter = set()
        # remove duplicates before operating
        result = set(result)
        for e in result:
            # account for case differences by creating lowercase comparison text
            comp = e.lower()
            if ('gift' and 'wrap') in comp:
                filter.add(e)
            # filter for mentions of gluten & range (common word associated with free) and 'gift wrap'
            # also prevent double counting with find_subs (i.e. 'gift voucher' or 'gift subscription')
            # note that '(x or y or z) in comp' does not work
            elif 'gluten' in comp or 'range' in comp or 'voucher' in comp or 'subscription' in comp:
                filter.add(e)
        # remove overlapping values
        result.difference_update(filter)
        if len(result) > 0:
            return result
        else:
            return
    else:
        return


def find_subs(text):
    """Identifies subscriptions in form of giftcards and vouchers and returns matching set of cases

    Args:
        text (str): Text to be parsed
    
    Returns:
        Matching set of string sequences if discount found, else None.
    """

    # use 'subscri' since it's a root that can cover 'subscription' and 'subscribe'
    pattern = r".{0,20}giftcard.{0,20}|.{0,20}voucher.{0,20}|.{0,20}subscri.{0,20}|.{0,20}membership.{0,20}"
    result = re.findall(pattern, text, re.I)

    if result:
        return set(result)
    else:
        return