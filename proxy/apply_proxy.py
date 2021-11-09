import pandas as pd

import numpy as np

from mvn_historical_drawdowns import read_data

from db_connection import create_connection, connection_engine

from dateutil.relativedelta import relativedelta

import re


def write_data(df):
    engine = connection_engine()
    tsql_chunksize = 2097 // len(df.columns)
    tsql_chunksize = 1000 if tsql_chunksize > 1000 else tsql_chunksize
    df.to_sql('time_series_proxy', engine, if_exists='append', index=False, chunksize=tsql_chunksize)


def read_dependency(asset_type, price_type, proxy_level):
    asset_type_list = [asset_type]
    price_type_list = [price_type]
    source_type_list = []
    source_price_list = []
    proxy_level_list = [proxy_level]
    cnxn = create_connection()
    cursor = cnxn.cursor()

    while proxy_level >= 1:
        sql_str = '''
        SELECT Source_Type, Source_Price from dependency_graph 
        WHERE Proxy_Level = {} and Asset_Type = '{}' and Price_Type = '{}'
        '''.format(proxy_level_list[-1], asset_type_list[-1], price_type_list[-1])

        rows = cursor.execute(sql_str)
        for row in rows.fetchall():
            if row[0] is None and row[1] is None:
                source_type_list.append(asset_type)
                source_price_list.append(price_type)
                if proxy_level >= 1:
                    asset_type_list.append(asset_type)
                    price_type_list.append(price_type)
            else:
                source_type_list.append(row[0])
                source_price_list.append(row[1])
                if proxy_level >= 1:
                    asset_type_list.append(row[0])
                    price_type_list.append(row[1])

        proxy_level = proxy_level - 1
        if proxy_level >= 1:
            proxy_level_list.append(proxy_level)

    asset_type_list = asset_type_list[:-1]
    price_type_list = price_type_list[:-1]

    return asset_type_list[::-1], price_type_list[::-1], source_type_list[::-1], \
           source_price_list[::-1], proxy_level_list[::-1]


def apply_proxy_level_1(asset_code, price_type, cutoff="NA"):
    at, pt, st, sp, pl = read_dependency(asset_code, price_type, 1)
    df = read_data(st[0], sp[0])
    df['Asset_Code'] = at[0]
    df['Price_Type'] = pt[0]
    df['Proxy_Level'] = 1
    df['Proxy_Name'] = np.nan
    df = df[['Asset_Code', 'Price_Type', 'Date', 'Price', 'Proxy_Level', 'Proxy_Name']]
    if cutoff == "NA":
        write_data(df)
    else:
        df = df[df.Date >= cutoff]
        return write_data(df)


def parse_check_overlap_period(min_date, max_date, min_period, max_period, div_method):
    '''
    Method to check if overlap period condition is satisfied, if satisfied returns overlap start and end date.
    :param div_method:
    :param min_date:
    :param max_date:
    :param min_period:
    :param max_period:
    :return: Boolean
    '''

    value_min = int(re.findall(r'\d+', min_period)[0])
    value_max = int(re.findall(r'\d+', max_period)[0])

    if 'M' in min_period:
        overlap_min_end_date = min_date + relativedelta(months=value_min)
    elif 'Y' in min_period:
        overlap_min_end_date = min_date + relativedelta(years=value_min)
    else:
        raise ValueError("Minimum overlap period is not correct")

    if 'M' in max_period:
        overlap_max_end_date = min_date + relativedelta(months=value_max)
    elif 'Y' in max_period:
        overlap_max_end_date = min_date + relativedelta(years=value_max)
    else:
        raise ValueError("Minimum overlap period is not correct")

    if div_method == 'Average Calendar Quarterly':
        month_factor = 0 if overlap_min_end_date.month % 3 == 0 and \
                            overlap_min_end_date.day == overlap_min_end_date.days_in_month else 1
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.QuarterEnd() * month_factor, \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.QuarterEnd() * month_factor
        period = 'Q'

    elif div_method == 'Average Calendar Monthly':
        month_factor = 0 if overlap_min_end_date.day % overlap_min_end_date.days_in_month == 0 else 1
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.MonthEnd() * month_factor, \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.MonthEnd() * month_factor
        period = 'M'

    elif div_method == 'Average Calendar Semi-Annual':
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.DateOffset(months=6), \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.DateOffset(months=6)
        period = '6M'

    elif div_method == 'Average Calendar Annual':
        year_factor = 1 if overlap_min_end_date.day == overlap_min_end_date.days_in_month \
                           and overlap_min_end_date.month == 12 else 2
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor, \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor
        period = 'Y'
    elif div_method == 'Average Annual':
        pass

    else:
        raise ValueError("Provide a valid Dividend Yield Period")

    if overlap_min_end_date > max_date:
        end_date, result = overlap_max_end_date, False
    elif overlap_max_end_date <= max_date:
        end_date, result = overlap_max_end_date, True
    else:
        end_date, result = overlap_min_end_date, True

    return end_date, period, result


def apply_proxy_level_2(asset_code, price_type, methodology):
    asset_code = 'SPX'
    price_type = 'GTR'
    methodology = 'Raw + ExtendGTRFromPR'
    at, pt, st, sp, pl = read_dependency(asset_type=asset_code, price_type=price_type, proxy_level=2)

    l1_at = at[0]
    l1_pt = pt[0]
    l1_st = st[0]
    l1_sp = sp[0]
    l1_pl = pl[0]

    level_one_sql = ''' SELECT * FROM time_series_proxy WHERE Asset_Code = '{}'
                        AND Price_Type = '{}' and Proxy_Level = {}'''.format(l1_at, l1_pt, l1_pl)
    cnxn = create_connection()
    df = pd.read_sql(level_one_sql, cnxn)

    if df.shape[0] == 0:
        apply_proxy_level_1(asset_code=l1_at, price_type=l1_pt)
    else:
        overlap_sql = '''SELECT * FROM time_series_proxy WHERE Asset_Code = 'SPX'
                        AND Price_Type IN ('PR', 'GTR') and Proxy_Level = 1'''
        overlap_df = pd.read_sql(overlap_sql, cnxn)
        overlap_df['quarter'] = overlap_df['Date'].dt.quarter
        overlap_df['quarter_index'] = pd.PeriodIndex(overlap_df.Date, freq='Q')
        pr_series = overlap_df[overlap_df['Price_Type'] == 'PR']
        gtr_series = overlap_df[overlap_df['Price_Type'] == 'GTR']

        pr_series.set_index('Date', inplace=True)
        gtr_series.set_index('Date', inplace=True)

        overlap_min_date = max(pr_series.index.min(), gtr_series.index.min())
        overlap_max_date = min(pr_series.index.max(), gtr_series.index.max())
        overlap_years = np.floor((overlap_max_date - overlap_min_date).days / 365.25)

        # pr_series = pr_series
        #
        # loc = pr_series.index.get_loc(overlap_min_date)
        # pr_series = pr_series.iloc[loc - 1:,]
        pr_series = pr_series[pr_series.index >= overlap_min_date]
        gtr_series = gtr_series[gtr_series.index >= overlap_min_date]

        re_gtr_series = gtr_series.resample('Q').last()
        re_pr_series = pr_series.resample('Q').last()

        max_date = overlap_min_date + relativedelta(years=3)
        max_date = max_date - relativedelta(days=max_date.day)
        max_date = max_date + relativedelta(months=3)

        re_gtr_series = re_gtr_series[re_gtr_series.index <= max_date]
        re_pr_series = re_pr_series[re_pr_series.index <= max_date]

        re_series = re_gtr_series.reset_index(). \
            rename(columns={'Price': 'GTR_Price'})[['quarter_index', 'GTR_Price']]. \
            set_index('quarter_index'). \
            join(re_pr_series.reset_index().rename(columns={'Price': 'PR_Price'})[['quarter_index',
                                                                                   'PR_Price']]. \
                 set_index('quarter_index'))

        re_series['div_yield'] = (re_series['GTR_Price'] / re_series['GTR_Price'].shift()) - \
                                 (re_series['PR_Price'] / re_series['PR_Price'].shift())

        re_series['quarter'] = re_series.index.quarter

        div_yield_df = re_series.reset_index()[['div_yield', 'quarter']].groupby(['quarter']).mean()

        div_yield_df['div_yield'] = div_yield_df['div_yield'] * 4

        work_df = overlap_df[(overlap_df['Price_Type'] == 'PR') & (overlap_df['Date'] < overlap_min_date)]

        work_df = pd.concat([work_df,
                             overlap_df[(overlap_df['Price_Type'] == 'GTR') &
                                        (overlap_df['Date'] >= overlap_min_date)]])

        work_df = work_df.set_index('quarter').join(div_yield_df)

        work_df['PR'] = np.where(work_df['Price_Type'] == 'PR', work_df['Price'], np.nan)

        # work_df['Price_Proxied'] = np.where(work_df['Price_Type'] == 'GTR', work_df['Price'], np.nan)

        work_df.sort_values('Date', inplace=True)

        work_df.reset_index(inplace=True)

        work_df.set_index('Date', inplace=True)

        loc = work_df.index.get_loc(overlap_min_date)

        work_df = work_df.iloc[:loc + 1, ]

        test_df = work_df.join(pr_series['Price'].rename('PR_Price'))

        test_df = test_df[test_df.index <= '1988-01-04']

        test_df['PR_Final'] = np.where(test_df['PR'].isna(), test_df['PR_Price'], test_df['PR'])

        test_df = test_df.reset_index().sort_values('Date', ascending=False)

        test_df['Prox_Price'] = np.where(test_df['Price_Type'] == 'GTR', test_df['Price'], np.nan)

        test_df['Days'] = ((test_df['Date'].shift() - test_df['Date']).dt.days) / 365.25

        test_df['div_yield'] = test_df.div_yield.shift()

        test_df['Prox_Price'] = (
            test_df['Prox_Price'].combine_first(1 / ((test_df['PR_Final'].shift() / test_df['PR_Final']) +
                                                     (test_df['div_yield'] * test_df['Days'])))
                .cumprod())

        test_df['Price_Type'] = 'GTR'

        test_df = test_df[['Asset_Code', 'Price_Type', 'Date', 'Price']]

        test_df = test_df.iloc[1:, :]

        test_df['Proxy_Level'] = 2

        test_df['Proxy_Name'] = None

        gtr_series.reset_index(inplace=True)

        gtr_series = gtr_series[['Asset_Code', 'Price_Type', 'Date', 'Price', 'Proxy_Level', 'Proxy_Name']]

        results = pd.concat([gtr_series.reset_index()
                             [['Asset_Code', 'Price_Type', 'Date', 'Price', 'Proxy_Level', 'Proxy_Name']]
                                , test_df])

        results.sort_values('Date', inplace=True)

        results.set_index('Date').to_csv('Data/C1_Raw+ExtendGTRFromPR.csv')

        work_df['Price_Proxied'] = work_df['Price']

        work_df['Price_Proxied'] = work_df['Price_Proxied']

        df['Price'] = (df['Price'].combine_first(df['Proxy'].shift() / df.eval('Proxy*Div*Days'))
                       .cumprod().round(2))

    pass


def proxy_two_extend_gtr_pr(asset_code, price_type, min_period='1Y', max_period='3Y',
                            div_method='Average Calendar Quarterly', tax_min=-0.05, tax_max=0.5):
    '''
    Apply proxy level 2 to extend NTR using PR and GTR.
    1) Identify if proxy level 1 series of price type PR & GTR exists for current Asset Code.
    2) Identify if proxy level 1 series exists for current Asset Code and Price Type.
    3) Updates df data and ensure all level 1 data is inplace for further calculations.
    4) Slice PR, GTR, NTR series
    '''
    asset_code = 'SPX'
    price_type = 'NTR'
    # methodology = 'Raw+ExtendNTRFromPR+GTR'
    at, pt, st, sp, pl = read_dependency(asset_type=asset_code, price_type=price_type, proxy_level=2)

    l1_at = at[0]
    l1_pt = pt[0]
    # l1_st = st[0]
    # l1_sp = sp[0]
    l1_pl = pl[0]

    # Check if level 1 PR, GTR & NTR exists for current source asst and price type
    level_one_sql = ''' SELECT * FROM time_series_proxy WHERE Asset_Code = '{}'
                            AND Price_Type IN ('{}', 'PR','GTR') and Proxy_Level = {}''' \
        .format(l1_at, l1_pt, l1_pl)

    df = pd.read_sql(level_one_sql, create_connection())

    # Create level 1 GTR series if does not exists
    if df[(df['Asset_Code'] == l1_at) & (df['Price_Type'] == 'GTR')].shape[0] == 0:
        apply_proxy_level_1(asset_code=l1_at, price_type='GTR')

        # Update df with data
        df = pd.read_sql(level_one_sql, create_connection())

    # Create level 1 PR series if does not exists
    if df[(df['Asset_Code'] == l1_at) & (df['Price_Type'] == 'PR')].shape[0] == 0:
        apply_proxy_level_1(asset_code=l1_at, price_type='PR')

        # Update df with data
        df = pd.read_sql(level_one_sql, create_connection())

    # Create level 1 NTR series if does not exists
    if df[(df['Asset_Code'] == l1_at) & (df['Price_Type'] == l1_pt)].shape[0] == 0:
        apply_proxy_level_1(asset_code=l1_at, price_type=l1_pt)

        # Update df with data
        df = pd.read_sql(level_one_sql, create_connection())

    # Add Quarter and Quarter Index to df to be used for div yield and tax rate calculations.
    df['quarter'] = df['Date'].dt.quarter
    df['quarter_index'] = pd.PeriodIndex(df.Date, freq='Q')

    # Slice PR, GTR & NTR series
    pr_series = df[df['Price_Type'] == 'PR']
    gtr_series = df[df['Price_Type'] == 'GTR']
    ntr_series = df[df['Price_Type'] == 'NTR']

    pr_series.set_index('Date', inplace=True)
    gtr_series.set_index('Date', inplace=True)
    ntr_series.set_index('Date', inplace=True)

    # Computing overlap period
    overlap_min_date = max(pr_series.index.min(), gtr_series.index.min(), ntr_series.index.min())
    overlap_max_date = min(pr_series.index.max(), gtr_series.index.max(), ntr_series.index.max())

    end_date, period, process = parse_check_overlap_period(overlap_min_date, overlap_max_date, min_period, max_period,
                                                           div_method)
    if process:
        gtr_resampled = gtr_series[(gtr_series.index >= overlap_min_date) & (gtr_series.index <= end_date)]. \
            resample(period).last()
        pr_resampled = pr_series[(pr_series.index >= overlap_min_date) & (pr_series.index <= end_date)]. \
            resample(period).last()
        ntr_resampled = ntr_series[(ntr_series.index >= overlap_min_date) & (ntr_series.index <= end_date)]. \
            resample(period).last()

        resampled_series = gtr_resampled.reset_index(). \
            rename(columns={'Price': 'GTR_Price'})[['quarter_index', 'GTR_Price']]. \
            set_index('quarter_index'). \
            join(pr_resampled.reset_index().rename(columns={'Price': 'PR_Price'})[['quarter_index',
                                                                                   'PR_Price']]
                 .set_index('quarter_index'))

        resampled_series = resampled_series.join(
            ntr_resampled.reset_index().rename(columns={'Price': 'NTR_Price'})[['quarter_index',
                                                                                'NTR_Price']].set_index('quarter_index')
        )

        resampled_series['div_yield'] = (resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) - \
                                        (resampled_series['PR_Price'] / resampled_series['PR_Price'].shift())

        resampled_series['quarter'] = resampled_series.index.quarter

        # use period to determine 4
        div_yield_df = resampled_series.reset_index()[['div_yield', 'quarter']].groupby(['quarter']).mean() * 4

        # add div yield to resampled series
        resampled_series = resampled_series.reset_index().set_index('quarter'). \
            join(div_yield_df.rename(columns={'div_yield': 'ann_div_yield'}))

        resampled_series.sort_values('quarter_index', inplace=True)

        resampled_series['tax_rate'] = ((resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) -
                                        (resampled_series['NTR_Price'] / resampled_series['NTR_Price'].shift())) \
                                       / resampled_series['ann_div_yield']

        tax_rate_df = resampled_series.reset_index()[['tax_rate', 'quarter']].groupby(['quarter']).mean()

        # Tax rate check to be clarified and added

        # create extension to NTR
        extended_df = pd.DataFrame(index=pd.date_range(max(pr_series.index.min(), gtr_series.index.min()), end_date))

        # add PR price
        extended_df = extended_df.join(pr_series['Price'].rename('PR'))

        # add GTR price
        extended_df = extended_df.join(gtr_series['Price'].rename('GTR'))

        # drop rows containing any nan
        extended_df.dropna(inplace=True)

        # add ntr value after end_date to df
        extended_df ntr_series.iloc[ntr_series.index.get_loc(end_date) + 1, ]


        pass
    else:
        return print('Overlap requirement not satisfied process terminated.')


data = {'Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04', '2021-01-05',
                 '2021-01-06', '2021-01-07'],
        'Price': [np.nan, np.nan, np.nan, np.nan, 10, 11, 32],
        'Proxy': [10, 20, 30, 24, 35, 45, 32],
        'Calc': [np.nan, np.nan, np.nan, np.nan, 10 / 11, np.nan, np.nan]}
tester = pd.DataFrame(data)
tester = tester.sort_values('Date', ascending=False).reset_index(drop=True)
tester = tester.iloc[2:, :]
tester['Ca'] = tester.Calc.ffill() / tester.Calc.cumprod().shift(fill_value=1)

data = {'Date': ['2021-01-13', '2021-01-08', '2021-01-04', '2021-01-03', '2021-01-01'],
        'Price': [10, np.nan, np.nan, np.nan, np.nan],
        'Proxy': [20, 30, 40, 50, 60],
        'Div': [0.5, 0.6, 0.7, 0.8, 0.9],
        'Days': [np.nan, 5, 4, 1, 2]}

tester = pd.DataFrame(data)

tester['Price'] = (tester['Price'].combine_first(tester['Proxy'].shift() / tester.eval('Proxy*Div*Days'))
                   .cumprod().round(2))

tester['Price'] = (tester['Price'].combine_first((tester.eval('Proxy') / tester['Proxy'].shift()) + 1 / (tester['Div'] *
                                                                                                         tester[
                                                                                                             'Days']))
                   .cumprod().round(2))

tester['Price'] = (
    tester['Price'].combine_first(1 / ((tester.eval('Proxy').shift() / tester['Proxy']) + (tester['Div'] *
                                                                                           tester['Days'])))
        .cumprod().round(2))

tester['Price'] = (tester['Price'].combine_first((tester['Proxy'].shift() / tester.eval('Proxy+(Div*Days)')) +
                                                 tester.eval('Div*Days'))
                   .cumprod().round(2))

tester['Price'] = (tester['Price'].combine_first(tester['Proxy'].shift() / tester.eval('Proxy'))
                   .cumprod().round(2))
