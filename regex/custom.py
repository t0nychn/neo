"""Dynamic constructs for customizing regex functions. Remember to unit test any new regex sequence!

Will have to define new score calculator (can inherit from Trinity)
"""

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
            # return text True if it passes logic, else False
            ...
        find_new = New(patterns, logic=foo)
        text1 = "Regex is fun!"
        result1 = find_new.match(text2)
        # we don't have to assign pattern again for parsing new text (unlike quicker find_custom method)
        text2 = "Regex is easy!"
        result2 = find_new.match(text2)

    Args:
        pattern (str): Regex pattern to match
        logic (func): Function to filter regex match that returns a boolean value. Default is None.

    Attributes:
        patterns (seq): Regex patterns to match in list/set form
        logic (func): Function to filter regex match that returns a boolean value.

    Methods:
        match: Matches input string to patterns.
    """

    def __init__(self, patterns, logic=None):
        self.patterns = patterns
        self.logic = logic
    
    def match(self, text):
        """Same construct as find_freebies but dynamic
        
        Args:
            text (str): Input text to be parsed.
        
        Returns:
            Matching set of string sequences if discount found, else None.
        """

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