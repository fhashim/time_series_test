import re

from typing import Union


def parse_frequency(norm_freq: str, comp_freq: str) \
        -> Union[float, str]:

    # allowed offsets D: Daily, M: Monthly, W: Weekly, Y: Yearly
    offset_chars = set('DWQMY')

    # Generate days for Normalisation Frequency
    try:
        if norm_freq == 'NA':
            norm_days = 'NA'
        elif any((c in offset_chars) for c in norm_freq):
            value = int(re.findall(r'\d+', norm_freq)[0])
            if 'D' in norm_freq:
                norm_days = value
            elif 'W' in norm_freq:
                norm_days = value * (365.25 / 52)
            elif 'M' in norm_freq:
                norm_days = value * (365.25 / 12)
            elif 'Q' in norm_freq:
                norm_days = value * (365.25 / 4)
            else:
                norm_days = value * 365.25
        else:
            norm_days = 'ERROR: Normalisation frequency is incorrect'
            comp_days = 'ERROR: Normalisation frequency is incorrect'
    except (Exception,):
        norm_days = 'ERROR: Normalisation frequency is incorrect'
        comp_days = 'ERROR: Normalisation frequency is incorrect'
        return norm_days, comp_days

    # Generate days for Compounding Frequency
    try:
        if any((c in offset_chars) for c in comp_freq):
            value = int(re.findall(r'\d+', comp_freq)[0])
            if 'D' in comp_freq:
                comp_days = value
            elif 'W' in comp_freq:
                comp_days = value * (365.25 / 52)
            elif 'M' in comp_freq:
                comp_days = value * (365.25 / 12)
            elif 'Q' in comp_freq:
                comp_days = value * (365.25 / 4)
            else:
                comp_days = value * 365.25
        else:
            norm_days = 'ERROR: Compounding frequency is incorrect'
            comp_days = 'ERROR: Compounding frequency is incorrect'

    except (Exception,):
        norm_days = 'ERROR: Compounding frequency is incorrect'
        comp_days = 'ERROR: Compounding frequency is incorrect'
        return norm_days, comp_days

    return norm_days, comp_days
