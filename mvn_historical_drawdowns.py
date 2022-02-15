"""

Calculate largest historic drawdowns and recovery period post a
drawdown for a given portfolio
(Can also be used on individual assets)

Function Signature:

mvn_historical_drawdowns (Code, Price_Type, [Period_Start],
*[Period_End], [Rank]*)

returns [DATE,DATE,FLOAT,INT]

Function Inputs:

- Asset Code - STRING
- Price Type - STRING
- Period Start - [DATE or STRING] - Can be exact date ;
                 an offset D/W/M/Y from Period End Date;
                 or Inception(ie starting Oldest Price Date)
- Period End - [DATE or STRING] - optional. Defaults to Latest Price
               Date. Can be exact date or as an offset D/W/M/Y
               from Latest Price Date
- Rank - [INT]

Function Outputs:

- Drawdown Start - [DATE]
- Drawdown End - [DATE]
- Drawdown Performance - [FLOAT]
- Recovery Days - [INT]

Methodology:

- From the asset time-series(using Asset Code/Price Type),
    find the Nth largest peak to trough performance(N being the Rank)
    and the time in days to recover the the previous peak.

- Return the Drawdown Period(Start/End), Drawdown Performance,
  Recovery Days

Comments:

- Period Start/Period End/Rank accept arrays, so multiple drawdowns
    can be calculated/returned at once
"""
import re

from typing import Union

import pandas as pd

import numpy as np

from dateutil.parser import parse

from dateutil.relativedelta import relativedelta

from db_connection import create_connection


def parse_dates(period_start: str, period_end: str,
                data_frame: pd.DataFrame) -> Union[pd.Timestamp,
                                                   pd.Timestamp]:
    '''

    :param period_start: Date String
    :param period_end: Date String
    :param data_frame: Pandas DataFrame
    :return: Pandas Timestamp
    '''
    # allowed offsets D: Daily, M: Monthly, W: Weekly, Y: Yearly
    offset_chars = set('DWMY')

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
            else:
                end_date = end_date - relativedelta(years=value)
        else:
            end_date = pd.Timestamp(parse(period_end, fuzzy=False))
    # except ValueError as error:
    #     raise ValueError("Period End date is not correct") from error
    except:
        start_date = 'Period End date is not correct'
        end_date = 'Period End date is not correct'
        return start_date, end_date

    # check if price on end date exists else use last available price
    if end_date in data_frame.Date.values:
        pass
    else:
        end_date = data_frame.loc[data_frame['Date'] <= end_date,
                                  'Date'].max()
        print(f"Period End Date not found in date using last "
              f"available date i.e. {end_date}")

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
            else:
                start_date = start_date - relativedelta(years=value)
        else:
            start_date = pd.Timestamp(parse(period_start, fuzzy=False))

    except:
        start_date = 'Period Start date is not correct'
        end_date = 'Period Start date is not correct'
        return start_date, end_date
    # except ValueError as error:
    #     raise ValueError("Period Start date is not correct") from error

    # check if price on start date exists else use previous available
    # price

    if start_date in data_frame.Date.values:
        pass
    else:
        start_date = data_frame.loc[data_frame['Date']
                                    <= start_date, 'Date'].max()

        print(f"Period Start Date not found in date using last "
              f"available date i.e. {start_date}")

    return start_date, end_date


def read_data(code: str, price_type: str) -> pd.DataFrame:
    '''

    :param code: Asset Code String
    :param price_type: Price Type String
    :return: Pandas DataFrame
    '''
    # Create Connection with DB
    conn = create_connection()

    # Get available codes and PriceType from db
    sql_distinct_code = ''' SELECT DISTINCT(CONCAT(Asset_code, ' - ',
    price_type)) [code] FROM time_series'''

    df_distinct_code = pd.read_sql(sql_distinct_code, conn)

    # Check if passed code and PriceType is available in DB
    if code + ' - ' + price_type in df_distinct_code.code.values:
        # Read data for specific code and Price. Return resulting
        # dataframe

        read_data_sql = f''' SELECT Date, Price
                            FROM time_series
                            WHERE Asset_code = '{code}'
                            and price_type = '{price_type}'
                            ORDER BY Date ASC  '''
        data_frame = pd.read_sql(read_data_sql, conn)
        data_frame['Date'] = pd.to_datetime(data_frame['Date'],
                                            format='%Y-%m-%d')
        # return df

    # Raise error if not available
    else:
        raise ValueError("code or Price Type does not exists in DB")

    return data_frame


def get_historical_drawdowns(main_df: pd.DataFrame, period_start: Union[str, None],
                             period_end: Union[str, None],
                             rank: Union[int, None]) -> \
        Union[np.datetime64, float, int]:
    """
    :param main_df:
    :param period_start: Date str
    :param period_end: Date str
    :param rank: Rank of drawdown
    :return:
    """
    # using defaults where passed value is none
    # period_start = 'Inception' if period_start is None else period_start
    period_end = 'Latest' if period_end is None else period_end
    rank = 1 if rank is None else rank

    # main_df = read_data(code, price_type)
    start_date, end_date = parse_dates(period_start, period_end,
                                       main_df)

    if type(start_date) == str:
        drawdown_start = start_date
        drawdown_end = start_date
        drawdown_performance = start_date
        recovery_days = start_date

        return drawdown_start, drawdown_end, drawdown_performance, recovery_days

    # Set date as df index
    main_df.set_index('Date', inplace=True)

    # Filter out all data after start_date as Recovery days
    # is not bound by Period End
    main_df = main_df[main_df.index >= start_date]

    # Compute returns for Price time series
    main_df['Returns'] = main_df['Price'].pct_change()

    # Compute cumulative returns using Returns
    main_df['Cum_Returns'] = (1 + main_df['Returns']).cumprod()

    # Generate column Previous Peak to identify last peak
    # (highest cumulative returns) from current data point.
    main_df['Previous_Peak'] = main_df['Cum_Returns'].cummax()

    # Calculate Drawdown
    main_df['Drawdown'] = (main_df['Cum_Returns'] /
                           main_df['Previous_Peak']) - 1.0

    # Returns df with cumulative maximum applied on Cum_Returns
    # along with Dates to find Date on which Previous Peak occurred.
    df_cum = pd.DataFrame(
        pd.concat([main_df.Cum_Returns, main_df.index.to_series()],
                  axis=1)
            .agg(tuple, axis=1)
            .cummax()
            .to_list(),
        columns=["Previous_Peak", "Previous_Peak_index"],
    )

    # Previous Peak index replaced with NaT where Previous_Peak is NaN

    df_cum[df_cum.isna().any(axis=1)] = np.nan

    # Creates a group by object on Previous_Peak_index agg as list and
    # results are shift to find the upcoming Previous
    # Peak index (i.e. Next_PP_index) '''
    df_grouped = (
        df_cum.groupby("Previous_Peak_index")["Previous_Peak_index"]
            .agg(list)
            .str[0]
            .shift(-1)
    )

    # With df_cum containing Previous_Peak_index apply left join
    # with df_grouped a group by object containing Next_PP_index.
    # Use Previous_Peak_index as a key to join from left frame
    # Use index from df_grouped as key to perform left join.
    # Rename columns as Previous_Peak_index and Next_PP_index
    # respectively.
    df_cum = df_cum.merge(
        df_grouped, left_on="Previous_Peak_index", right_index=True,
        how="left"
    ).rename(
        columns={
            "Previous_Peak_index_x": "Previous_Peak_index",
            "Previous_Peak_index_y": "Next_PP_index",
        }
    )

    # Create column for Previous_Peak_index & Next_PP_index in actual
    # df - Used for further calculations.
    # Replaced Previous_Peak value.
    main_df[["Previous_Peak", "Previous_Peak_index",
             "Next_PP_index"]] = df_cum.values

    # Create Column for Price on Previous Peak
    main_df['PP_Price'] = \
        main_df.join(main_df.drop('Previous_Peak_index', axis=1),
                     on='Previous_Peak_index',
                     rsuffix='_y')['Price_y'].values

    # Calculate recovery days in case data is NA Recovery_Days = -1
    main_df['Recovery_Days'] = np.where(
        pd.isna(main_df['Next_PP_index']),
        -1,
        (main_df['Next_PP_index'] - main_df.index).dt.days)

    # Filter out up to end_date to generate stats.
    main_df = main_df[
        (main_df.index >= start_date) & (main_df.index <= end_date)]

    # Sort Values based on Drawdown to out desried rank
    main_df = main_df.sort_values('Drawdown').drop_duplicates(
        'Previous_Peak_index')

    # Remove results where draw down in 0 or NaN
    main_df = main_df[~(main_df.Drawdown >= 0) & ~(main_df.Drawdown.isna())]

    # Check if rank is within available limits else throw exception
    if rank <= main_df.shape[0]:
        main_df = main_df.iloc[:rank, :][-1:]
    else:
        raise ValueError("Required rank not found.")

    # Function output in desired format.
    drawdown_start = (main_df['Previous_Peak_index'].values)[0].astype(
        'M8[D]')
    drawdown_end = main_df.index.values[0].astype('M8[D]')
    drawdown_performance = (np.round(main_df['Drawdown'].values, 6))[0]
    recovery_days = ((main_df['Recovery_Days'].values).astype(int))[0]

    return drawdown_start, drawdown_end, drawdown_performance, \
           recovery_days


def historical_drawdowns(asset_code: str, price_type: str,
                         period_start: list, period_end: list,
                         rank: list) -> dict:
    '''
    :param asset_code:
    :param price_type:
    :param period_start:
    :param period_end:
    :param rank:
    :return:
    '''

    if None in period_start:
        raise TypeError('Period Start - Date, Offset or String '
                        '("Inception") expected instead got NoneType')

    drawdown_vec = np.vectorize(get_historical_drawdowns,
                                otypes=[np.datetime64, np.datetime64,
                                        float, int])

    result_list = drawdown_vec(asset_code, price_type, period_start,
                               period_end, rank)
    result_dict = {'drawdown_start': result_list[0],
                   'drawdown_end': result_list[1],
                   'drawdown_performance': result_list[2],
                   'recovery_days': result_list[3]}
    return result_dict


def case_iterator(asset_code: str, price_type: str, period_start, period_end,
                  rank):
    main_df = read_data(asset_code, price_type)

    drawdown_start_list = []
    drawdown_end_list = []
    drawdown_performance_list = []
    recovery_days_list = []
    for start_date, end_date, rank_val in zip(period_start, period_end,
                                              rank):
        try:
            drawdown_start, drawdown_end, drawdown_performance, \
            recovery_days = get_historical_drawdowns(main_df,
                                                     start_date,
                                                     end_date,
                                                     rank_val)
            drawdown_start_list.append(drawdown_start)
            drawdown_end_list.append(drawdown_end)
            drawdown_performance_list.append(drawdown_performance)
            recovery_days_list.append(recovery_days)

        except:
            drawdown_start_list.append(None)
            drawdown_end_list.append(None)
            drawdown_performance_list.append(None)
            recovery_days_list.append(None)

    return drawdown_start_list, drawdown_end_list, \
           drawdown_performance_list, recovery_days_list


drawdown_start_list, drawdown_start_list, \
drawdown_performance_list, recovery_days_list =case_iterator('SPY US', 'GTR',
                     period_start= ['2020-12-31', '3M', '6M', '1Y', '3Y', '5Y', '10Y', '15Y', '30Y'],
                     period_end=   [None, None, None, None, None, None, None, None, None],
                     rank = [None, None, None, None, None, None, None, None, None])


drawdown_start_list, drawdown_end_list, \
drawdown_performance_list, recovery_days_list =case_iterator('SPY US', 'GTR',
                     period_start= ['1M'],
                     period_end=   [None],
                     rank = [5])

result  = historical_drawdowns('SPY US', 'GTR',
                     period_start= ['2020-12-31', '3M', '6M', '1Y', '3Y', '5Y', '10Y', '15Y'],
                     period_end=   [None, None, None, None, None, None, None, None],
                     rank = [None, None, None, None, None, None, None, None])

drawdown_start_list = [date.astype('M8[D]') for date in drawdown_start_list if date is not None]

test = [np.datetime64('2021-01-01'), None]

conv_test = [date.astype('M8[D]') for date in test if date is not None]

https: // funktion2.herokuapp.com / api / dynamic / historical_drawdowns?asset_code = SPY % 20U
S & price_type = GTR & period_start = 1
M & period_end = Latest & rank = 5