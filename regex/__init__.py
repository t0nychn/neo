"""This package contains regex modules that will help identify deal info in sites

Idea behind returning no object rather than empty set is that it'll be easier to see
on the spreadsheet & identify null values later.
"""

import re


def find_discounts(text):
    """Identifies % discount in '£x off', 'x% off' or 'x% discount' form and returns matching set of cases.

    Args:
        text (str): Text to be parsed.
    
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
        text (str): Text to be parsed.
    
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


def find_subscriptions(text):
    """Identifies subscriptions in form of giftcards, vouchers, subscriptions and memberships and returns matching set of cases.

    Args:
        text (str): Text to be parsed.
    
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


class Trinity:
    """Finds regex matches and calculates an overall score for compatibility with CRM product.
    Defaults to using the three regex modules above.
    
    Use:
        text = "The Answer Is Out There, Neo..."
        trin = Trinity(text)
        discounts = trin.disc
        score = trin.score()
    
    Args:
        text (str): Input text to be processed.

    Attributes:
        disc (set): Discounts found.
        free (set): Freebies found.
        subs (set): Subscriptions found.
    
    Methods:
        score: Calculates compatibility score.
    """

    def __init__(self, text):
        self.disc = find_discounts(text)
        self.free = find_freebies(text)
        self.subs = find_subscriptions(text)

    def _len(self, attr):
        """Private method to deal with nonetype objects if no matches found.
        
        More preferable than returning empty set so we have null values in data when put into spreadsheet/database.
        """

        return (len(attr) if attr else 0)

    def score(self):
        """Returns compatibility score for CRM product.
        
        Updated score calculation (2021-08-02):
        • discounts are the most indicative of potential offer so score is weighted 9x that of subs
        • freebies are more useful than subscriptions so weighted 3x that of subs
        • sigma_d is total unique discounts found
        • sigma_f is total unique freeby matches found containing 'free'/'gift'/'voucher' (check out regex package to see additional checks in place)
        • sigma_s is total unique subs found
        • no longer logging score, just using weighted sum to rank later
        """

        return 9 * self._len(self.disc) + 3 * self._len(self.free) + self._len(self.subs)