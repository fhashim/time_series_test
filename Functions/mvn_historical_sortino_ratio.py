from typing import Union

import pandas as pd

import numpy as np

from Functions.date_parser import parse_dates

from Functions.data_reader import read_data

from Functions.mvn_historical_returns import get_historical_returns


def get_historical_sortino_ratio(main_df: pd.DataFrame,
                                period_start: Union[str],
                                period_end: Union[str, None],
                                norm_freq: Union[None, str],
                                comp_freq: str,
                                lambda_factor: Union[None, float],
                                riskfree_rate: float):
    # Return error when start date is None
    if period_start is None:
        sortino_ratio, rate_of_return, downside_volatility = \
            "ERROR: Start Date is required"
        return sortino_ratio, rate_of_return, downside_volatility

    # using defaults where passed value is none
    period_end = 'Latest' if period_end is None else period_end

    # Get parsed start and end dates
    start_date, end_date = parse_dates(period_start, period_end,
                                       main_df)
    if isinstance(start_date, str):
        sortino_ratio, rate_of_return, downside_volatility = start_date
        return sortino_ratio, rate_of_return, downside_volatility

    if pd.isnull(start_date):
        sortino_ratio = rate_of_return = downside_volatility = \
            "ERROR: No data found prior to start date"
        return sortino_ratio, rate_of_return, downside_volatility

    try:
        rate_of_return = get_historical_returns(main_df, period_start,
                                                period_end, norm_freq,
                                                comp_freq)
    except (Exception,):
        rate_of_return = None

    if isinstance(rate_of_return, str):
        sortino_ratio, downside_volatility = rate_of_return
        return sortino_ratio, rate_of_return, downside_volatility

    if rate_of_return is None:
        sortino_ratio, downside_volatility = None
        return sortino_ratio, rate_of_return, downside_volatility


    # Compute downside performance
    # Filter data
    main_df = main_df.set_index('Date')
    main_df = main_df[(main_df.index >= start_date) &
                      (main_df.index <= end_date)]

    # Order by date
    main_df = main_df.sort_values(by='Date')

    # Compute performance
    main_df['Performance'] = np.log(main_df.Price / main_df.
                                    Price.shift())
    main_df = main_df[1:]

    # Filter out negative performance only
    main_df = main_df[main_df.Performance < 0]

    # Calculate volatility with Lambda
    if lambda_factor is None:
        main_df['Vol'] = (main_df['Performance'] -
                          main_df['Performance'].mean()) ** 2

        downside_volatility = np.sqrt(((main_df['Vol'].sum() * 252)
                                       / main_df.shape[0]))

        downside_volatility = np.round(downside_volatility, 6)

    # Calculate volatility without Lambda
    else:
        main_df = main_df.sort_values(by='Date', ascending=False)
        main_df['Weight'] = (1 - lambda_factor) * lambda_factor ** \
                            np.arange(len(main_df))

        downside_volatility = np.round(
            np.sqrt(
                ((main_df['Weight'] * main_df[
                    'Performance'] ** 2).sum()
                 * 252) / (main_df['Weight'].sum())
            ), 6
        )

    sortino_ratio = np.round((rate_of_return - riskfree_rate)
                             / downside_volatility, 6)

    return sortino_ratio, rate_of_return, downside_volatility


def historical_sortino_ratio(maven_asset_code: str, price_type: str,
                             currency: str, period_start: list,
                             period_end: list,
                             normalization_freq: Union[
                                 None, str] = '1Y',
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

    downside_volatility_list = []
    rate_of_return_list = []
    sortino_ratio_list = []
    for start_date, end_date in zip(period_start, period_end):
        try:
            sortino_ratio, rate_of_return, downside_volatility = \
                get_historical_sortino_ratio(main_df, start_date,
                                             end_date,
                                             normalization_freq,
                                             compounding_freq,
                                             lambda_factor,
                                             riskfree_rate)
            sortino_ratio_list.append(sortino_ratio)
            rate_of_return_list.append(rate_of_return)
            downside_volatility_list.append(downside_volatility)

        except (Exception,):
            sortino_ratio_list.append(None)
            rate_of_return_list.append(None)
            downside_volatility_list.append(None)

    result_dict = {'Sortino Ratio': sortino_ratio_list,
                   'Rate of Return': rate_of_return_list,
                   'Downside Volatility': downside_volatility_list}

    return result_dict



# results = historical_sortino_ratio('SPY US', 'PR', None,
#                                   period_start = ['1Y','2W','6M','3Q','95D','Inception','30Y','50Y'],
#                                   period_end = [None, None, None, None, None, None, None, None]
#                                   )
#
# maven_asset_code = 'SPY US'
# price_type = 'PR'
# period_start = '1Y'
# period_end = 'Latest'
# lambda_factor = None
# norm_freq = '1Y'
# comp_freq = '1Y'
# riskfree_rate = 0

results = historical_sortino_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.9,
                                  riskfree_rate=0.05
                                  )
