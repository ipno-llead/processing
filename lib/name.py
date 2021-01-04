import re


def punctuation_strip(string):
    """Remove periods, commas, and redundant whitespace

    Parameters
    ----------
    string : str

    Returns
    -------
    stripped_string : str

    Examples
    --------
    >>> string_strip("Mary-Ellen.", False)
    'Mary-Ellen'
    >>> string_strip("     SADOWSKY,  J.R", False)
    'SADOWSKY JR'
    """
    stripped_string = re.sub(r'\s+', ' ', re.sub(r'\.|\,', '', string))
    return ' '.join(stripped_string.split())


def clean_name(name):
    """Return cleaned name by applying functions above

    Parameters
    ----------
    name : str

    Returns
    -------
    cleaned_name : str

    Examples
    --------
    >>> cleaned_first_name = clean_name(first_name)
    >>> cleaned_last_name = clean_name(last_name)
    """
    return punctuation_strip(name).title()
