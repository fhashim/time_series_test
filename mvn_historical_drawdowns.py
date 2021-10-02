"""
Calculate largest historic drawdowns and recovery period post a drawdown for a given portfolio(Can also be used on individual assets)

Function Signature:

mvn_historical_drawdowns (Code, Price_Type, [Period_Start], *[Period_End], [Rank]*)

returns [DATE,DATE,FLOAT,INT]

Function Inputs:

- Asset Code - STRING
- Price Type - STRING
- Period Start - [DATE or STRING] - Can be exact date ; an offset D/W/M/Y from Period End Date; or Inception(ie starting Oldest Price Date)
- Period End - [DATE or STRING] - optional. Defaults to Latest Price Date. Can be exact date or as an offset D/W/M/Y from Latest Price Date
- Rank - [INT]

Function Outputs:

- Drawdown Start - [DATE]
- Drawdown End - [DATE]
- Drawdown Performance - [FLOAT]
- Recovery Days - [INT]

Methodology:

- From the asset time-series(using Asset Code/Price Type), find the Nth largest peak to trough performance(N being the Rank) and the time in days to recover the the previous peak.
- Return the Drawdown Period(Start/End), Drawdown Performance, Recovery Days

Comments:

- Period Start/Period End/Rank accept arrays, so multiple drawdowns can be calculated/returned at once
"""

import pandas as pd

import numpy as np

from dateutil.parser import parse

from dateutil.relativedelta import relativedelta

import re

from db_connection import create_connection


def parse_dates(Period_Start, Period_End, df):
    # allowed offsets D: Daily, M: Monthly, W: Weekly, Y: Yearly
    offset_chars = set('DWMY')

    # Parse and deal with Period End Date as Period Start depends on Period End
    try:
        if Period_End == 'Latest':
            end_date = df.Date.max()
        elif any((c in offset_chars) for c in Period_End):
            end_date = df.Date.max()
            value = int(re.findall(r'\d+', Period_End)[0])
            if 'D' in Period_End:
                end_date = end_date - relativedelta(days=value)
            elif 'W' in Period_End:
                end_date = end_date - relativedelta(weeks=value)
            elif 'M' in Period_End:
                end_date = end_date - relativedelta(months=value)
            else:
                end_date = end_date - relativedelta(years=value)
        else:
            end_date = pd.Timestamp(parse(Period_End, fuzzy=False))
    except ValueError:
        raise ValueError("Period End date is not correct")

    # check if price on end date exists else use last available price
    if end_date in df.Date.values:
        pass
    else:
        end_date = df.loc[df['Date'] <= end_date, 'Date'].max()
        print('Period End Date not found in date using last available date i.e. {}'.format(end_date))

    # Parse and deal with Period Start Date
    try:
        if Period_Start == 'Inception':
            start_date = df.Date.min()
        elif any((c in offset_chars) for c in Period_Start):
            start_date = end_date
            value = int(re.findall(r'\d+', Period_Start)[0])
            if 'D' in Period_Start:
                start_date = start_date - relativedelta(days=value)
            elif 'W' in Period_Start:
                start_date = start_date - relativedelta(weeks=value)
            elif 'M' in Period_Start:
                start_date = start_date - relativedelta(months=value)
            else:
                start_date = start_date - relativedelta(years=value)
        else:
            start_date = pd.Timestamp(parse(Period_Start, fuzzy=False))
    except ValueError:
        raise ValueError("Period Start date is not correct")

    # check if price on start date exists else use previous available price
    if start_date in df.Date.values:
        pass
    else:
        start_date = df.loc[df['Date'] <= start_date, 'Date'].max()
        print('Period Start Date not found in date using last available date i.e. {}'.format(start_date))

    return start_date, end_date


def read_data(Code, Price_Type):
    # Create Connection with DB
    conn = create_connection()

    # Get available Codes and PriceType from db
    sql_distinct_code = ''' SELECT DISTINCT(CONCAT(Asset_Code, ' - ', Price_Type)) [Code] FROM time_series'''
    df_distinct_code = pd.read_sql(sql_distinct_code, conn)

    # Check if passed Code and PriceType is available in DB
    if Code + ' - ' + Price_Type in df_distinct_code.Code.values:
        # Read data for specific Code and Price. Return resulting dataframe
        read_data_sql = ''' SELECT Date, Price 
                            FROM time_series  
                            WHERE Asset_Code = '{}' and Price_Type = '{}'
                            ORDER BY Date ASC  '''.format(Code, Price_Type)
        df = pd.read_sql(read_data_sql, conn)
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        return df

    # Raise error if not available
    else:
        raise ValueError("Code or Price Type does not exists in DB")


def mvn_historical_drawdowns(Code, Price_Type, Period_Start='Inception', Period_End='Latest', Rank=1):
    main_df = read_data(Code, Price_Type)
    start_date, end_date = parse_dates(Period_Start, Period_End, main_df)

    ''' Set date as df index '''
    main_df.set_index('Date', inplace=True)

    ''' Filter out all data after start_date as Recovery days is not bound by Period End '''
    main_df = main_df[main_df.index >= start_date]

    ''' Compute returns for Price time series '''
    main_df['Returns'] = main_df['Price'].pct_change()

    ''' Compute cumulative returns using Returns '''
    main_df['Cum_Returns'] = (1 + main_df['Returns']).cumprod()

    ''' Generate column Previous Peak to identify last peak (highest cumulative returns) from current data point. '''
    main_df['Previous_Peak'] = main_df['Cum_Returns'].cummax()

    ''' Calculate Drawdown '''
    main_df['Drawdown'] = (main_df['Cum_Returns'] / main_df['Previous_Peak']) - 1.0

    ''' Returns df with cumulative maximum applied on Cum_Returns along with Dates to find Date on which Previous Peak
        occurred. '''
    x = pd.DataFrame(
        pd.concat([main_df.Cum_Returns, main_df.index.to_series()], axis=1)
            .agg(tuple, axis=1)
            .cummax()
            .to_list(),
        columns=["Previous_Peak", "Previous_Peak_index"],
    )

    ''' Previous Peak index replaced with NaT where Previous_Peak is NaN '''
    x[x.isna().any(axis=1)] = np.nan

    '''Creates a group by object on Previous_Peak_index agg as list and results are shift to find the upcoming Previous
        Peak index (i.e. Next_PP_index) '''
    g = (
        x.groupby("Previous_Peak_index")["Previous_Peak_index"]
            .agg(list)
            .str[0]
            .shift(-1)
    )

    ''' With x containing Previous_Peak_index apply left join with g a group by object containing Next_PP_index.
        Use Previous_Peak_index as a key to join from left frame
        Use index from g as key to perform left join.
        Rename columns as Previous_Peak_index and Next_PP_index respectively. '''
    x = x.merge(
        g, left_on="Previous_Peak_index", right_index=True, how="left"
    ).rename(
        columns={
            "Previous_Peak_index_x": "Previous_Peak_index",
            "Previous_Peak_index_y": "Next_PP_index",
        }
    )

    ''' Create column for Previous_Peak_index & Next_PP_index in actual df - Used for further calculations.
        Replaced Previous_Peak value. '''
    main_df[["Previous_Peak", "Previous_Peak_index", "Next_PP_index"]] = x.values

    ''' Create Column for Price on Previous Peak '''
    main_df['PP_Price'] = main_df.join(main_df.drop('Previous_Peak_index', axis=1), on='Previous_Peak_index',
                                       rsuffix='_y')['Price_y'].values

    ''' Calculate recovery days in case data is NA Recovery_Days = -1 '''
    main_df['Recovery_Days'] = np.where(pd.isna(main_df['Next_PP_index']),
                                        -1,
                                        (main_df['Next_PP_index'] - main_df.index).dt.days)

    ''' Filter out up to end_date to generate stats. '''
    main_df = main_df[(main_df.index >= start_date) & (main_df.index <= end_date)]

    ''' Sort Values based on Drawdown to out desried rank '''
    main_df = main_df.sort_values('Drawdown').drop_duplicates('Previous_Peak_index')

    '''Check if Rank is within available limits else throw exception'''
    if Rank <= main_df.shape[0]:
        main_df = main_df.iloc[:Rank, :][-1:]
    else:
        raise ValueError("Required Rank not found.")

    ''' Function output in desired format. '''
    drawdown_start = (main_df['Previous_Peak_index'].values)[0].astype(str)
    drawdown_end = (main_df.index.values)[0].astype(str)
    drawdown_performance = (np.round(main_df['Drawdown'].values, 6))[0]
    recovery_days = ((main_df['Recovery_Days'].values).astype(int))[0]

    return drawdown_start, drawdown_end, drawdown_performance, recovery_days


drawdown_start, drawdown_end, drawdown_performance, recovery_days = \
    mvn_historical_drawdowns(Code='IEFA US', Price_Type='GTR', Period_Start="2021-06-09", Period_End='2021-08-09',
                             Rank=1)
