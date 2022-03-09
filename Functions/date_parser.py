import re

from typing import Union

import pandas as pd

from dateutil.parser import parse

from dateutil.relativedelta import relativedelta


def parse_dates(period_start: str, period_end: Union[str, None],
                data_frame: pd.DataFrame) -> Union[pd.Timestamp,
                                                   str]:
    """

    :param period_start: Date String
    :param period_end: Date String
    :param data_frame: Pandas DataFrame
    :return: Pandas Timestamp
    """

    # allowed offsets D: Daily, M: Monthly, W: Weekly, Y: Yearly
    offset_chars = set('DWQMY')

    # Parse and deal with Period End Date as Period Start
    # depends on Period End
    try:
        if period_end == 'Latest':
            end_date = data_frame.Date.max()
        elif any((c in offset_chars) for c in period_end):
            end_date = data_frame.Date.max()
            value = int(re.findall(r'\d+', period_end)[0])
            if 'D' in period_end:
                end_date = end_date - relativedelta(days=value)
            elif 'W' in period_end:
                end_date = end_date - relativedelta(weeks=value)
            elif 'M' in period_end:
                end_date = end_date - relativedelta(months=value)
            elif 'Q' in period_end:
                end_date = end_date - relativedelta(months=3*value)
            else:
                end_date = end_date - relativedelta(years=value)
        else:
            end_date = pd.Timestamp(parse(period_end, fuzzy=False))

    except (Exception,):
        start_date = 'ERROR: Period End date is not correct'
        end_date = 'ERROR: Period End date is not correct'
        return start_date, end_date

    # check if price on end date exists else use last available price
    if end_date not in data_frame.Date.values:
        if max(data_frame.Date.values) < end_date:
            start_date = 'ERROR: No data found on or after end date'
            end_date = 'ERROR: No data found on or after end date'
            return start_date, end_date
        else:
            end_date = data_frame.loc[data_frame['Date'] <= end_date,
                                      'Date'].max()

    # Parse and deal with Period Start Date
    try:
        if period_start == 'Inception':
            start_date = data_frame.Date.min()
        elif any((c in offset_chars) for c in period_start):
            start_date = end_date
            value = int(re.findall(r'\d+', period_start)[0])
            if 'D' in period_start:
                start_date = start_date - relativedelta(days=value)
            elif 'W' in period_start:
                start_date = start_date - relativedelta(weeks=value)
            elif 'M' in period_start:
                start_date = start_date - relativedelta(months=value)
            elif 'Q' in period_start:
                start_date = start_date - relativedelta(months=3*value)
            else:
                start_date = start_date - relativedelta(years=value)
        else:
            start_date = pd.Timestamp(parse(period_start, fuzzy=False))

    except (Exception,):
        start_date = 'ERROR: Period Start date is not correct'
        end_date = 'ERROR: Period Start date is not correct'
        return start_date, end_date

    # check if price on start date exists else use previous available
    # price
    if start_date not in data_frame.Date.values:
        start_date = data_frame.loc[data_frame['Date']
                                    <= start_date, 'Date'].max()

    if start_date >= end_date:
        start_date = 'ERROR: Period Start date is greater than End date'
        end_date = 'ERROR: Period Start date is greater than End date'

    return start_date, end_date
