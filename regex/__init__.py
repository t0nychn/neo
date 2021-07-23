"""This package contains regex modules that will help identify deal info in sites"""

import re


def find_discount(text):
    """Identifies % discount in 'x% off' form and returns matching set of cases

    Args:
        text (str): Text to be parsed
    
    Returns:
        Matching set of string sequences if discount found, else None.
    """

    pattern = r"\d{1,2}[^%]{0,5}%[^%]{0,5}off"
    result = re.findall(pattern, text)

    if result:
        return set(result)
    else:
        return