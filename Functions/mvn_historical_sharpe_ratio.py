from typing import Union

import pandas as pd

import numpy as np

from Functions.date_parser import parse_dates

from Functions.data_reader import read_data

from Functions.mvn_historical_volatility import \
    get_historical_volatility

from Functions.mvn_historical_returns import get_historical_returns


def get_historical_sharpe_ratio(main_df: pd.DataFrame,
                                period_start: Union[str],
                                period_end: Union[str, None],
                                norm_freq: Union[None, str],
                                comp_freq: str,
                                lambda_factor: Union[None, float],
                                riskfree_rate: float):
    # Return error when start date is None
    if period_start is None:
        sharpe_ratio, rate_of_return, volatility_val = \
            "ERROR: Start Date is required"
        return sharpe_ratio, rate_of_return, volatility_val

    # using defaults where passed value is none
    period_end = 'Latest' if period_end is None else period_end

    # Get parsed start and end dates
    start_date, end_date = parse_dates(period_start, period_end,
                                       main_df)
    if isinstance(start_date, str):
        sharpe_ratio, rate_of_return, volatility_val = start_date
        return sharpe_ratio, rate_of_return, volatility_val

    if pd.isnull(start_date):
        sharpe_ratio = rate_of_return = volatility_val = \
            "ERROR: No data found prior to start date"
        return sharpe_ratio, rate_of_return, volatility_val

    try:
        rate_of_return = get_historical_returns(main_df, period_start,
                                                period_end, norm_freq,
                                                comp_freq)
    except (Exception,):
        rate_of_return = None

    if isinstance(rate_of_return, str):
        sharpe_ratio, volatility_val = rate_of_return
        return sharpe_ratio, rate_of_return, volatility_val

    if rate_of_return is None:
        sharpe_ratio, volatility_val = None
        return sharpe_ratio, rate_of_return, volatility_val

    try:
        volatility_val = get_historical_volatility(
            main_df, period_start, period_end, lambda_factor)

    except (Exception,):
        volatility_val = None

    if isinstance(volatility_val, str):
        sharpe_ratio, rate_of_return = volatility_val
        return sharpe_ratio, rate_of_return, volatility_val

    if volatility_val is None:
        sharpe_ratio, rate_of_return = None
        return sharpe_ratio, rate_of_return, volatility_val

    sharpe_ratio = np.round((rate_of_return - riskfree_rate) /
                            volatility_val, 6)

    return sharpe_ratio, rate_of_return, volatility_val


def historical_sharpe_ratio(maven_asset_code: str, price_type: str,
                            currency: str, period_start: list,
                            period_end: list,
                            normalization_freq: Union[None, str] = '1Y',
                            compounding_freq: str = '1Y',
                            lambda_factor: Union[None, float] = None,
                            riskfree_rate: float = 0
                            ) -> dict:
    """
    :param compounding_freq:
    :param normalization_freq:
    :param riskfree_rate:
    :param lambda_factor:
    :param currency:
    :param maven_asset_code: Asset Code str
    :param price_type: Price Type str
    :param period_start: Period Start str
    :param period_end: Period End str
    :return:
    """
    # NotImplementedError for currency (will be removed later)
    if currency is not None:
        raise NotImplementedError('ERROR: Currency is not supported')

    # read data
    main_df = read_data(maven_asset_code, price_type)

    list_it = iter([period_start, period_end])
    list_lens = len(next(list_it))
    if not all(len(l) == list_lens for l in list_it):
        raise ValueError('ERROR: Ensure all passed list are '
                         'of same length!')

    volatility_list = []
    rate_of_return_list = []
    sharpe_ratio_list = []
    for start_date, end_date in zip(period_start, period_end):
        try:
            sharpe_ratio, rate_of_return, volatility_val = \
                get_historical_sharpe_ratio(main_df, start_date,
                                            end_date,
                                            normalization_freq,
                                            compounding_freq,
                                            lambda_factor,
                                            riskfree_rate)
            sharpe_ratio_list.append(sharpe_ratio)
            rate_of_return_list.append(rate_of_return)
            volatility_list.append(volatility_val)

        except (Exception,):
            sharpe_ratio_list.append(None)
            rate_of_return_list.append(None)
            volatility_list.append(None)

    result_dict = {'Sharpe Ratio': sharpe_ratio_list,
                   'Rate of Return': rate_of_return_list,
                   'Volatility': volatility_list}

    return result_dict


# mvn_historical_sharpe_ratio (SPY US, PR, , [1Y,2W,6M,3Q,95D,Inception,30Y,50Y])
#
# results = historical_sharpe_ratio('SPY US', 'PR', None,
#                                   period_start = ['1Y','2W','6M','3Q','95D','Inception','30Y','50Y'],
#                                   period_end = [None, None, None, None, None, None, None, None]
#                                   )
# maven_asset_code = 'SPY US'
# price_type = 'PR'
# period_start = '1Y'
# period_end = 'Latest'
# lambda_factor = None
# norm_freq = '1Y'
# comp_freq = '1Y'
# riskfree_rate = 0