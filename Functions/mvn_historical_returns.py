"""'

Calculate historic returns for an asset/portfolio

Function Signature:

mvn_historical_returns (Maven_Asset_ID, Price_Type, *Currency*,
    [Period_Start], *[Period_End], Normalisation, CompoundingFrequency*)

returns [FLOAT]

Function Inputs:

- Maven Asset ID - STRING - Price Type - STRING - Currency - STRING
optional - Period Start - [DATE or STRING] - Can be exact date ; an
offset D/W/M/ Q/Y from Period End Date; or Inception(ie starting
Oldest Price Date) - Period End - [DATE or STRING] optional. Defaults
to Latest Price Date. Can be exact date or as an offset D/W/M/Q/Y
from Latest Price Date - Normalisation - STRING optional - In
D/W/M/Q/Y. Can also be set to None. Defaults to 1Y -
CompoundingFrequency - STRING optional - In D/W/M/Q/Y. Defaults to 1Y

Function Outputs:

- Rate of Return - [FLOAT]

Methodology:

- If Currency is provided, calculate the price for each date needed from
    the time-series as follows
- AssetPrice*CCYUSD(Asset Currency)/CCYUSD(Currency)
- FX rate fetched from FX Time-Series table
- Asset Currency is fetched from Universal Master by looking up Currency 
    for the Maven Asset ID
- For assets where CCY = USD, CCYUSD = 1
- When FX rate not available for a given date, use prior FX
- Rate of Return(Normalisation is not None) = (((EndValue/StartValue)^
    (CompoundingFrequency(days)/
    NumberofDays from PeriodStart to Period End))-1)*
    Normalisation(days)/CompoundingFrequency(days)
- Rate of Return(Normalisation is None) = EndValue/StartValue - 1

"""

from typing import Union

import pandas as pd

import numpy as np

from Functions.date_parser import parse_dates

from Functions.data_reader import read_data

from Functions.normalization_parser import parse_frequency


def get_historical_returns(main_df: pd.DataFrame,
                           period_start: Union[str],
                           period_end: Union[str, None],
                           norm_freq: Union[str, None],
                           comp_freq: Union[str, None]):
    # Return error when start date is None
    if period_start is None:
        rate_of_return = "ERROR: Start Date is required"
        return rate_of_return

    # using defaults where passed value is none
    period_end = 'Latest' if period_end is None else period_end
    norm_freq = '1Y' if norm_freq is None else norm_freq
    comp_freq = '1Y' if comp_freq is None else comp_freq

    # Get parsed start and end dates
    start_date, end_date = parse_dates(period_start, period_end,
                                       main_df)

    if isinstance(start_date, str):
        rate_of_return = start_date
        return rate_of_return

    if pd.isnull(start_date):
        rate_of_return = "ERROR: No data found prior to start date"
        return rate_of_return

    # Get parsed days for normalisation and compounding frequency
    norm_days, comp_days = parse_frequency(norm_freq, comp_freq)

    if isinstance(norm_days, str) and norm_days != 'NA':
        rate_of_return = norm_days
        return rate_of_return

    # Filter data
    main_df = main_df.set_index('Date')
    main_df = main_df[(main_df.index >= start_date) &
                      (main_df.index <= end_date)]

    # days diff between start and end date
    days_diff = (end_date - start_date).days

    if norm_freq == 'NA':
        rate_of_return = (main_df.values[-1] / main_df.values[0]) - 1
        rate_of_return = (np.round(rate_of_return, 6))[0]
    else:
        rate_of_return = (((main_df.values[-1] / main_df.values[0]) **
                          (comp_days / days_diff) - 1) *
                          (norm_days / comp_days))
        rate_of_return = (np.round(rate_of_return, 6))[0]

    return rate_of_return


def historical_returns(asset_code: str, price_type: str,
                       currency: str, period_start: list,
                       period_end: list, normalisation_freq: str,
                       compounding_freq: str
                       ) -> dict:
    """
    :param currency:
    :param normalisation_freq:
    :param compounding_freq:
    :param asset_code: Asset Code str
    :param price_type: Price Type str
    :param period_start: Period Start str
    :param period_end: Period End str
    :return:
    """
    # NotImplementedError for currency (will be removed later)
    if currency is not None:
        raise NotImplementedError('ERROR: Currency is not supported')

    # read data
    main_df = read_data(asset_code, price_type)

    list_it = iter([period_start, period_end])
    list_lens = len(next(list_it))
    if not all(len(l) == list_lens for l in list_it):
        raise ValueError('ERROR: Ensure all passed list are '
                         'of same length!')

    rate_of_return_list = []
    for start_date, end_date in zip(period_start, period_end):
        try:
            rate_of_return = get_historical_returns(main_df,
                                                    start_date,
                                                    end_date,
                                                    normalisation_freq,
                                                    compounding_freq)
            rate_of_return_list.append(rate_of_return)

        except (Exception,):
            rate_of_return_list.append(None)

    result_dict = {'rate_of_return': rate_of_return_list}

    return result_dict
