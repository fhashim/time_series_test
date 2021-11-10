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
        period, factor = 'Q', 4

    elif div_method == 'Average Calendar Monthly':
        month_factor = 0 if overlap_min_end_date.day % overlap_min_end_date.days_in_month == 0 else 1
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.MonthEnd() * month_factor, \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.MonthEnd() * month_factor
        period, factor = 'M', 12

    elif div_method == 'Average Calendar Semi-Annual':
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.DateOffset(months=6), \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.DateOffset(months=6)
        period, factor = '6M', 2

    elif div_method == 'Average Calendar Annual':
        year_factor = 1 if overlap_min_end_date.day == overlap_min_end_date.days_in_month \
                           and overlap_min_end_date.month == 12 else 2
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor, \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor
        period, factor = 'Y', 1

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

    return end_date, period, factor, result


def proxy_two_extend_gtr(asset_code, price_type, min_period='1Y', max_period='3Y',
                        div_method='Average Calendar Quarterly'):
    asset_code = 'SPX'
    price_type = 'GTR'
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

    df = df[['Date', 'Price', 'Asset_Code', 'Price_Type']]

    # Add Quarter and Quarter Index to df to be used for div yield and tax rate calculations.
    # df['period'] = df['Date'].dt.quarter
    if div_method == 'Average Calendar Quarterly':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='Q')
    elif div_method == 'Average Calendar Monthly':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='M')
    elif div_method == 'Average Calendar Annual':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='Y')

    # Slice PR, GTR & NTR series
    pr_series = df[df['Price_Type'] == 'PR']
    gtr_series = df[df['Price_Type'] == 'GTR']

    pr_series.set_index('Date', inplace=True)
    gtr_series.set_index('Date', inplace=True)

    # Computing overlap period
    overlap_min_date = max(pr_series.index.min(), gtr_series.index.min())
    overlap_max_date = min(pr_series.index.max(), gtr_series.index.max())

    end_date, period, factor, process = parse_check_overlap_period(overlap_min_date, overlap_max_date, min_period,
                                                                   max_period,
                                                                   div_method)
    if process:
        gtr_resampled = gtr_series[(gtr_series.index >= overlap_min_date) & (gtr_series.index <= end_date)]. \
            resample(period).last()
        pr_resampled = pr_series[(pr_series.index >= overlap_min_date) & (pr_series.index <= end_date)]. \
            resample(period).last()


        resampled_series = gtr_resampled.reset_index(). \
            rename(columns={'Price': 'GTR_Price'})[['period_index', 'GTR_Price']]. \
            set_index('period_index'). \
            join(pr_resampled.reset_index().rename(columns={'Price': 'PR_Price'})[['period_index',
                                                                                   'PR_Price']]
                 .set_index('period_index'))

        resampled_series['div_yield'] = (resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) - \
                                        (resampled_series['PR_Price'] / resampled_series['PR_Price'].shift())

        # Add period on which avg is to be taken
        resampled_series['period'] = resampled_series.index.quarter
        div_yield_df = resampled_series.reset_index()[['div_yield', 'period']].groupby(['period']).mean() * factor

        # create extension to GTR
        # Assumption: On first available NTR series date there exist a value both for GTR and PR
        extended_df = pd.DataFrame(
            index=pd.date_range(pr_series.index.min(), gtr_series.index.min()))

        # add PR price
        extended_df = extended_df.join(pr_series['Price'].rename('PR'))

        # drop rows containing any nan
        extended_df.dropna(inplace=True)

        # sort series
        extended_df.sort_index(ascending=False, inplace=True)

        # add GTR first value as price
        extended_df = extended_df.join(gtr_series[gtr_series.index == gtr_series.index.min()]['Price'])

        extended_df.reset_index(inplace=True)
        extended_df['Days'] = ((extended_df['index'].shift() - extended_df['index']).dt.days) / 365.25
        extended_df.set_index('index',inplace=True)

        extended_df['period'] = extended_df.index.quarter
        extended_df = extended_df.reset_index().set_index('period').join(div_yield_df).set_index('index')
        extended_df.sort_index(ascending=False, inplace=True)
        extended_df['div_yield'] = extended_df['div_yield'].shift()

        extended_df['Price'] = (
            extended_df['Price'].combine_first(1 / ((extended_df['PR'].shift() / extended_df['PR']) +
                                                     (extended_df['div_yield'] * extended_df['Days'])))
                .cumprod())

        extended_df['Asset_Code'] = 'SPX'
        extended_df['Price_Type'] = 'GTR'
        extended_df['Proxy_Level'] = 2
        extended_df['Proxy_Name'] = np.nan
        extended_df.reset_index(inplace=True)
        extended_df = extended_df[['index', 'Asset_Code', 'Price_Type', 'Price', 'Proxy_Level', 'Proxy_Name']]
        extended_df.rename(columns={'index': 'Date'}, inplace=True)

        # Append with NTR series
        gtr_series['Proxy_Level'] = 1
        gtr_series['Proxy_Name'] = np.nan
        extended_df = pd.concat([extended_df[1:], gtr_series[['Asset_Code', 'Price_Type',
                                                              'Price', 'Proxy_Level', 'Proxy_Name',
                                                              ]].reset_index()])
        extended_df.sort_values('Date', inplace=True)

        return extended_df

    else:
        return print('Overlap requirement not satisfied process terminated.')



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

    df = df[['Date', 'Price', 'Asset_Code', 'Price_Type']]

    # Add Quarter and Quarter Index to df to be used for div yield and tax rate calculations.
    # df['period'] = df['Date'].dt.quarter
    if div_method=='Average Calendar Quarterly':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='Q')
    elif div_method=='Average Calendar Monthly':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='6M')
    elif div_method == 'Average Calendar Annual':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='Y')


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

    end_date, period, factor, process = parse_check_overlap_period(overlap_min_date, overlap_max_date, min_period,
                                                                   max_period,
                                                                   div_method)
    if process:
        gtr_resampled = gtr_series[(gtr_series.index >= overlap_min_date) & (gtr_series.index <= end_date)]. \
            resample(period).last()
        pr_resampled = pr_series[(pr_series.index >= overlap_min_date) & (pr_series.index <= end_date)]. \
            resample(period).last()
        ntr_resampled = ntr_series[(ntr_series.index >= overlap_min_date) & (ntr_series.index <= end_date)]. \
            resample(period).last()

        resampled_series = gtr_resampled.reset_index(). \
            rename(columns={'Price': 'GTR_Price'})[['period_index', 'GTR_Price']]. \
            set_index('period_index'). \
            join(pr_resampled.reset_index().rename(columns={'Price': 'PR_Price'})[['period_index',
                                                                                   'PR_Price']]
                 .set_index('period_index'))

        resampled_series = resampled_series.join(
            ntr_resampled.reset_index().rename(columns={'Price': 'NTR_Price'})[['period_index',
                                                                                'NTR_Price']].set_index('period_index')
        )

        resampled_series['div_yield'] = (resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) - \
                                        (resampled_series['PR_Price'] / resampled_series['PR_Price'].shift())

        # resampled_series['quarter'] = resampled_series.index.quarter
        #
        # # use period to determine 4
        # div_yield_df = resampled_series.reset_index()[['div_yield', 'quarter']].groupby(['quarter']).mean()  # * factor
        #
        # # add div yield to resampled series
        # resampled_series = resampled_series.reset_index().set_index('quarter'). \
        #     join(div_yield_df.rename(columns={'div_yield': 'ann_div_yield'}))
        #
        # resampled_series.sort_values('quarter_index', inplace=True)

        resampled_series['tax_rate'] = ((resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) -
                                        (resampled_series['NTR_Price'] / resampled_series['NTR_Price'].shift())) \
                                       / resampled_series['div_yield']
        # Add period on which avg is to be taken
        resampled_series['period'] = resampled_series.index.month
        tax_rate_df = resampled_series.reset_index()[['tax_rate', 'period']].groupby(['period']).mean()

        # Tax rate check to be clarified and added
        if (resampled_series.tax_rate < tax_min).any():
            return print('Minimum Tax Rate requirement not satisfied process terminated.')
        if (resampled_series.tax_rate > tax_max).any():
            return print('Maximum Tax Rate requirement not satisfied process terminated.')

        # create extension to NTR
        # Assumption: On first available NTR series date there exist a value both for GTR and PR
        extended_df = pd.DataFrame(
            index=pd.date_range(max(pr_series.index.min(), gtr_series.index.min()), ntr_series.index.min()))

        # add PR price
        extended_df = extended_df.join(pr_series['Price'].rename('PR'))

        # add GTR price
        extended_df = extended_df.join(gtr_series['Price'].rename('GTR'))

        # drop rows containing any nan
        extended_df.dropna(inplace=True)

        # sort series
        extended_df.sort_index(ascending=False, inplace=True)

        # add NTR first value as price
        extended_df = extended_df.join(ntr_series[ntr_series.index == ntr_series.index.min()]['Price'])

        # add avg.tax rate
        extended_df['period'] = extended_df.index.quarter
        extended_df = extended_df.reset_index().set_index('period').join(tax_rate_df).set_index('index')
        extended_df.sort_index(ascending=False, inplace=True)
        extended_df['tax_rate'] = extended_df['tax_rate'].shift()

        extended_df['Price'] = (extended_df['Price'].combine_first(1 / (
                ((extended_df['GTR'].shift()) / (extended_df['GTR'])) -
                (((extended_df['GTR'].shift()) / (extended_df['GTR'])) -
                 ((extended_df['PR'].shift()) / (extended_df['PR']))) * extended_df['tax_rate'])).cumprod())

        extended_df['Asset_Code'] = 'SPX'
        extended_df['Price_Type'] = 'NTR'
        extended_df['Proxy_Level'] = 2
        extended_df['Proxy_Name'] = np.nan
        extended_df.reset_index(inplace=True)
        extended_df = extended_df[['index', 'Asset_Code', 'Price_Type', 'Price', 'Proxy_Level', 'Proxy_Name']]
        extended_df.rename(columns={'index': 'Date'}, inplace=True)

        # Append with NTR series
        ntr_series['Proxy_Level'] = 1
        ntr_series['Proxy_Name'] = np.nan
        extended_df = pd.concat([extended_df[1:], ntr_series[['Asset_Code', 'Price_Type',
                                                              'Price', 'Proxy_Level', 'Proxy_Name',
                                                              ]].reset_index()])
        extended_df.sort_values('Date', inplace=True)
        return extended_df
    else:
        return print('Overlap requirement not satisfied process terminated.')
