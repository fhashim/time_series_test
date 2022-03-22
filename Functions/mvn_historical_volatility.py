from typing import Union

import pandas as pd

import numpy as np

from Functions.date_parser import parse_dates

from Functions.data_reader import read_data


def get_historical_volatility(main_df: pd.DataFrame,
                              period_start: Union[str],
                              period_end: Union[str, None],
                              lambda_factor: Union[None, float]):
    # Return error when start date is None
    if period_start is None:
        rate_of_return = "ERROR: Start Date is required"
        return rate_of_return

    # using defaults where passed value is none
    period_end = 'Latest' if period_end is None else period_end

    # Get parsed start and end dates
    start_date, end_date = parse_dates(period_start, period_end,
                                       main_df)

    if isinstance(start_date, str):
        volatility_val = start_date
        return volatility_val

    if pd.isnull(start_date):
        volatility_val = "ERROR: No data found prior to start date"
        return volatility_val

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

    # Calculate volatility with Lambda
    if lambda_factor is None:
        main_df['Vol'] = (main_df['Performance'] -
                          main_df['Performance'].mean()) ** 2

        volatility_val = np.sqrt(((main_df['Vol'].sum() * 252)
                                  / main_df.shape[0]))

        volatility_val = np.round(volatility_val, 6)

    # Calculate volatility without Lambda
    else:
        main_df = main_df.sort_values(by='Date', ascending=False)
        main_df['Weight'] = (1 - lambda_factor) * lambda_factor \
                            ** np.arange(len(main_df))
        volatility_val = np.round(
            np.sqrt(
                ((main_df['Weight'] * main_df['Performance'] ** 2).sum()
                 * 252) / (main_df['Weight'].sum())
            ), 6
        )

    return volatility_val


def historical_volatility(maven_asset_code: str, price_type: str,
                          currency: str, period_start: list,
                          period_end: list,
                          lambda_factor: Union[None, float] = None
                          ) -> dict:
    """
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
    for start_date, end_date in zip(period_start, period_end):
        try:
            volatility_val = get_historical_volatility(main_df,
                                                       start_date,
                                                       end_date,
                                                       lambda_factor)
            volatility_list.append(volatility_val)

        except (Exception,):
            volatility_list.append(None)

    result_dict = {'Volatility': volatility_list}

    return result_dict
# #
# mvn_historical_volatility (SPY US, PR, , [1Y,2W,6M,3Q,95D,Inception],,0.9)
# maven_asset_code = 'SPY US'
# price_type = 'PR'
# period_start = ['1Y','2W','6M','3Q','95D','Inception']
# period_end = [None, None, None, None, None, None]
# # lambda_factor = 0.9
# #
# # result = historical_volatility(maven_asset_code, price_type, None,period_start, period_end, lambda_factor)
# # print(result)
# #
# result = historical_volatility(maven_asset_code, price_type, None,period_start, period_end)
# print(result)