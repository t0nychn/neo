"""Dynamic constructs for customizing regex functions (remember to unit test any new regex sequence!)"""

import re


def find_custom(pattern, text):
    """Creates simple regex function from input pattern
    
    Args:
        pattern (str): Regex pattern to match
        text (str): Text to find
    
    Returns:
        Matching set of string sequences if discount found, else None.      
    """
    result = re.findall(pattern, text, re.I)

    if result:
        return set(result)
    else:
        return


class New:
    """Allows for a more complex regex function that can take extra patterns and filtering than find_custom(). 
    See find_freebies in regex module for applied filtering logic.
    
    Use:
        patterns = [r".{\10}regex.{\10}", ...]
        foo(text):
            # return text if it passes logic, else None
            ...
        find_new = New(patterns, logic=foo)
        text = "Regex is fun!"
        find_new.match(text)

    Args:
        pattern (str): Regex pattern to match
        logic (func): Function to clean regex match that outputs None if results don't match. Default is None.

    Attributes:
        patterns (seq): Regex patterns to match in list/set form
        logic (func): Function to clean regex match that outputs None if results don't match.
    
    Methods:
        match(): Returns matched text after applying pattern and logic (if specified)
    """

    def __init__(self, patterns, logic=None):
        self.patterns = patterns
        self.logic = logic
    
    def match(self, text):
        """Same construct as find_freebies but dynamic"""
        result = []
        for pattern in self.patterns:
            result += re.findall(pattern, text, re.I)
        result = set(result)
        if result:
            if self.logic is None:
                return result
            else:
                filter = set()
                for e in result:
                    # if passes inspection
                    if self.logic(e):
                        continue
                    # else add to filter
                    else:
                        filter.add(e)
                # remove those that didn't pass logic using set difference
                result.difference_update(filter)
                if len(result) > 0:
                    return result
                else:
                    return
        else:
            return