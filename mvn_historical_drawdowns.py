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


def mvn_historical_drawdowns(Code, Price_Type, Period_Start='Inception', Period_End='Latest', Rank=1):
    # create db connection and load data
    sql = '''
        SELECT Date, Asset_Code [Asset Code], Price_Type [Price Type], Price from time_series order by Date ASC
    '''
    conn = create_connection()
    df = pd.read_sql(sql, conn)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    main_df = df[(df['Asset Code'] == Code) & (df['Price Type'] == Price_Type)]

    chars = set('DWMY')

    # Parse and deal with Period End Date as Period Start depends on Period End
    try:
        if Period_End == 'Latest':
            end_date = main_df.Date.max()
        elif any((c in chars) for c in Period_End):
            end_date = main_df.Date.max()
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

    # check if price on end date exists
    if end_date in main_df.Date.values:
        pass
    else:
        end_date = main_df.loc[main_df['Date'] <= end_date, 'Date'].max()
        print('Period End Date not found in date using last available date i.e. {}'.format(end_date))

    # Parse and deal with Period Start Date
    try:
        if Period_Start == 'Inception':
            start_date = main_df.Date.min()
        elif any((c in chars) for c in Period_Start):
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

    # check if price on start date exists
    if start_date in main_df.Date.values:
        pass
    else:
        start_date = main_df.loc[main_df['Date'] <= start_date, 'Date'].max()
        print('Period Start Date not found in date using last available date i.e. {}'.format(start_date))

    work_df = main_df.sort_values(by='Date')
    work_df = work_df.set_index('Date')
    work_df = work_df[work_df.index >= start_date]

    work_df['Returns'] = work_df['Price'].pct_change()
    work_df['Cum_Returns'] = (1 + work_df['Returns']).cumprod()
    work_df['Previous_Peak'] = work_df['Cum_Returns'].cummax()

    # Calculate Drawdown
    work_df['Drawdown'] = (work_df['Cum_Returns'] / work_df['Previous_Peak']) - 1.0

    x = pd.DataFrame(
        pd.concat([work_df.Cum_Returns, work_df.index.to_series()], axis=1)
            .agg(tuple, axis=1)
            .cummax()
            .to_list(),
        columns=["Previous_Peak", "Previous_Peak_index"],
    )

    x[x.isna().any(axis=1)] = np.nan

    g = (
        x.groupby("Previous_Peak_index")["Previous_Peak_index"]
            .agg(list)
            .str[0]
            .shift(-1)
    )

    x = x.merge(
        g, left_on="Previous_Peak_index", right_index=True, how="left"
    ).rename(
        columns={
            "Previous_Peak_index_x": "Previous_Peak_index",
            "Previous_Peak_index_y": "Next_PP_index",
        }
    )

    work_df[["Previous_Peak", "Previous_Peak_index", "Next_PP_index"]] = x.values

    int_df = pd.DataFrame(index=work_df['Previous_Peak_index'])
    int_df = int_df.join(work_df['Price'])
    int_df.columns = ['PP_Price']

    work_df['PP_Price'] = int_df['PP_Price'].values
    work_df['Recovery_Days'] = np.where(pd.isna(work_df['Next_PP_index']),
                                        -1,
                                        (work_df['Next_PP_index'] - work_df.index).dt.days)

    sorted_df = work_df[(work_df.index >= start_date) & (work_df.index <= end_date)]
    sorted_df = sorted_df.sort_values('Drawdown').drop_duplicates('Previous_Peak_index')

    results_df = sorted_df.iloc[:Rank, :][-1:]

    drawdown_start = (results_df['Previous_Peak_index'].values)[0].astype(str)
    drawdown_end = (results_df.index.values)[0].astype(str)
    drawdown_performance = (np.round(results_df['Drawdown'].values, 6))[0]
    recovery_days = ((results_df['Recovery_Days'].values).astype(int))[0]

    return drawdown_start, drawdown_end, drawdown_performance, recovery_days, work_df


drawdown_start, drawdown_end, drawdown_performance, recovery_days, results = \
    mvn_historical_drawdowns(Code='IEFA US', Price_Type='GTR', Period_Start="2021-06-09",
                             Period_End="2021-08-09", Rank=1)
