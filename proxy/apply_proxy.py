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

    elif div_method == 'Average Calendar Monthly':
        month_factor = 0 if overlap_min_end_date.day % overlap_min_end_date.days_in_month == 0 else 1
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.MonthEnd() * month_factor, \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.MonthEnd() * month_factor

    elif div_method == 'Average Calendar Semi-Annual':
        year_factor = 0 if overlap_min_end_date.day == overlap_min_end_date.days_in_month \
                           and overlap_min_end_date.month == 12 else 1

        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor - \
                                                     pd.tseries.offsets.DateOffset(months=6), \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor - \
                                                     pd.tseries.offsets.DateOffset(months=6)

    elif div_method == 'Average Calendar Annual':
        year_factor = 0 if overlap_min_end_date.day == overlap_min_end_date.days_in_month \
                           and overlap_min_end_date.month == 12 else 1

        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor, \
                                                     overlap_max_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor
    elif div_method == 'Average Annual':
        year_factor = 1 if overlap_min_end_date.month <= 6 else 2
        overlap_min_end_date, overlap_max_end_date = overlap_min_end_date + \
                                                     pd.tseries.offsets.YearEnd() * year_factor - \
                                                     pd.tseries.offsets.DateOffset(months=6), \
                                                     overlap_max_end_date + pd.tseries.offsets.YearEnd() * \
                                                     year_factor - pd.tseries.offsets.DateOffset(months=6)
    else:
        raise ValueError("Provide a valid Dividend Yield Period")

    if overlap_min_end_date > max_date:
        end_date, result = overlap_max_end_date, False
    elif overlap_max_end_date <= max_date:
        end_date, result = overlap_max_end_date, True
    else:
        end_date, result = overlap_min_end_date, True

    return end_date, result


def create_period_index(df, div_method):
    # Creating month column to be used for filtration as per the passed div yield
    df['month_of_year'] = df['Date'].dt.month

    if div_method == 'Average Calendar Quarterly':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='Q')
        months = [3, 6, 9, 12]
        factor = 4

    elif div_method == 'Average Calendar Monthly':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='M')
        months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        factor = 12

    elif div_method == 'Average Calendar Semi-Annual':
        df['period_index'] = np.where(df['Date'].dt.month / 12 <= 0.5,
                                      df['Date'].dt.year.astype(str) + "SY" + '1',
                                      df['Date'].dt.year.astype(str) + "SY" + '2')
        months = [6, 12]
        factor = 2

    elif div_method == 'Average Calendar Annual':
        df['period_index'] = pd.PeriodIndex(df.Date, freq='Y')
        months = [12]
        factor = 1

    elif div_method == 'Average Annual':
        months = [6]
        factor = 1
        pass

    else:
        raise ValueError("Pass a valid div yield method!")

    return df, months, factor


def proxy_two_extend_gtr(asset_code, price_type, min_period='1Y', max_period='3Y',
                         div_method='Average Calendar Quarterly'):
    '''
    Apply proxy level 2 to extend GTR using PR. \n
    1) Identify if proxy level 1 series of price type PR exists for current Asset Code. \n
    2) Identify if proxy level 1 series exists for current Asset Code and Price Type. \n
    3) Updates df data and ensure all level 1 data is inplace for further calculations. \n
    5) If level 2 proxy data is available for PR than use for GTR extension calculations. \n
    6) Find max of PR, GTR min date. \n
    7) Find min of PR, GTR max date. \n
    8) Identify the overlap period using `parse_check_overlap_period`. \n
    9) Compute div yield for overlap period. \n
    10) Agg. div yield based on period index identified by div_method and get avg div yield. \n
    11) Use div yield to extend GTR on dates when PR value is known. \n
    12) Writes results to DB using `write_df`.
    :param asset_code:
    :param price_type:
    :param min_period:
    :param max_period:
    :param div_method:
    :return: extended_df
    '''

    # Read dependency
    at, pt, st, sp, pl = read_dependency(asset_type=asset_code, price_type=price_type, proxy_level=2)

    # level 1 asset type
    l1_at = at[0]

    # level 1 price type
    l1_pt = pt[0]

    # level 1 proxy level
    l1_pl = pl[0]

    # Check if level 1 PR, GTR exists for current source asset and price type
    level_one_sql = ''' SELECT * FROM time_series_proxy WHERE Asset_Code = '{}'
                                AND Price_Type IN ('{}', 'PR') and Proxy_Level = {}''' \
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

    # Use PR series level 2 data if available for passed asset and price type (TBC)
    level_two_sql = ''' SELECT * FROM time_series_proxy WHERE Asset_Code = '{}'
                                    AND Price_Type IN ('PR') and Proxy_Level = 2''' \
        .format(asset_code)

    # Append level 2 PR data if exists
    df = pd.concat([pd.read_sql(level_two_sql, create_connection()), df])

    # Slice relevant columns for process
    df = df[['Date', 'Price', 'Asset_Code', 'Price_Type']]

    # Creating month column to be used for filtration as per the passed div yield
    df['month_of_year'] = df['Date'].dt.month

    # Define period for passed div_yield and create period index for further process
    df, months, factor = create_period_index(df, div_method)

    # Slice PR, GTR & NTR series
    pr_series = df[df['Price_Type'] == 'PR']
    gtr_series = df[df['Price_Type'] == 'GTR']

    # Set date as index for both series
    pr_series.set_index('Date', inplace=True)
    gtr_series.set_index('Date', inplace=True)

    # Computing overlap period
    overlap_min_date = max(pr_series.index.min(), gtr_series.index.min())
    overlap_max_date = min(pr_series.index.max(), gtr_series.index.max())

    # Identify if overlap is successful and get the end date of overlap period
    end_date, process = parse_check_overlap_period(overlap_min_date, overlap_max_date, min_period,
                                                   max_period,
                                                   div_method)
    # If overlap check is passed begin with further process
    if process:
        # resample PR, GTR, NTR as per the div method
        period = 'M'
        gtr_resampled = gtr_series[(gtr_series.index >= overlap_min_date) & (gtr_series.index <= end_date)]. \
            resample(period).last()
        gtr_resampled = gtr_resampled[gtr_resampled['month_of_year'].isin(months)]
        pr_resampled = pr_series[(pr_series.index >= overlap_min_date) & (pr_series.index <= end_date)]. \
            resample(period).last()
        pr_resampled = pr_resampled[pr_resampled['month_of_year'].isin(months)]

        # df containing PR and GTR prices for overlap period
        resampled_series = gtr_resampled.reset_index(). \
            rename(columns={'Price': 'GTR_Price'})[['period_index', 'GTR_Price']]. \
            set_index('period_index'). \
            join(pr_resampled.reset_index().rename(columns={'Price': 'PR_Price'})[['period_index',
                                                                                   'PR_Price']]
                 .set_index('period_index'))

        # compute div yield
        resampled_series['div_yield'] = (resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) - \
                                        (resampled_series['PR_Price'] / resampled_series['PR_Price'].shift())

        # create extension to GTR
        # Assumption: On first available GTR series date there exist a value for PR
        extended_df = pd.DataFrame(
            index=pd.date_range(pr_series.index.min(), gtr_series.index.min()))

        # Add period on which avg is to be taken
        if div_method == 'Average Calendar Quarterly':
            resampled_series['period'] = resampled_series.index.quarter
            extended_df['period'] = extended_df.index.quarter

        elif div_method == 'Average Calendar Monthly':
            resampled_series['period'] = resampled_series.index.month
            extended_df['period'] = extended_df.index.month

        elif div_method == 'Average Calendar Semi-Annual':
            resampled_series['period'] = resampled_series.index.str.strip().str[-1]
            extended_df['period'] = np.where(extended_df.reset_index()['index'].dt.month / 12 <= 0.5, '1', '2')

        else:  # serves both annual and calender annual
            resampled_series['period'] = 1
            extended_df['period'] = 1

        # Agg. based on div method index
        div_yield_df = resampled_series.reset_index()[['div_yield', 'period']].groupby(['period']).mean() * factor

        # add PR price
        extended_df = extended_df.join(pr_series['Price'].rename('PR'))

        # drop rows containing any nan
        extended_df.dropna(inplace=True)

        # sort series
        extended_df.sort_index(ascending=False, inplace=True)

        # add GTR first value as price
        extended_df = extended_df.join(gtr_series[gtr_series.index == gtr_series.index.min()]['Price'])

        # check
        # extended_df['Days'] = ((extended_df.reset_index()['index'].shift() - extended_df.reset_index()['index']).dt.days) / 365.25
        # compute days difference
        extended_df.reset_index(inplace=True)
        extended_df['Days'] = ((extended_df['index'].shift() - extended_df['index']).dt.days) / 365.25
        extended_df.set_index('index', inplace=True)

        # Join div yield to df
        extended_df = extended_df.reset_index().set_index('period').join(div_yield_df).set_index('index')
        extended_df.sort_index(ascending=False, inplace=True)
        extended_df['div_yield'] = extended_df['div_yield'].shift()

        # Apply price extension formula
        extended_df['Price'] = (
            extended_df['Price'].combine_first(1 / ((extended_df['PR'].shift() / extended_df['PR']) +
                                                    (extended_df['div_yield'] * extended_df['Days'])))
                .cumprod())

        # restructure df to be passed on to DB
        extended_df['Asset_Code'] = asset_code
        extended_df['Price_Type'] = price_type
        extended_df['Proxy_Level'] = 2
        extended_df['Proxy_Name'] = np.nan
        extended_df.reset_index(inplace=True)
        extended_df = extended_df[['index', 'Asset_Code', 'Price_Type', 'Price', 'Proxy_Level', 'Proxy_Name']]
        extended_df.rename(columns={'index': 'Date'}, inplace=True)
        extended_df = extended_df[1:]
        extended_df.sort_values('Date', inplace=True)

        # write to db
        write_data(extended_df)

        return extended_df

    # process terminated if overlap check is failed
    else:
        return print('Overlap requirement not satisfied process terminated.')


def proxy_two_extend_gtr_pr(asset_code, price_type, min_period='1Y', max_period='3Y',
                            div_method='Average Calendar Quarterly', tax_min=-0.05, tax_max=0.5):
    '''
    Apply proxy level 2 to extend NTR using PR and GTR. \n
    1) Identify if proxy level 1 series of price type PR & GTR exists for current Asset Code. \n
    2) Identify if proxy level 1 series exists for current Asset Code and Price Type. \n
    3) Updates df data and ensure all level 1 data is inplace for further calculations. \n
    5) If level 2 proxy data is available for PR & GTR than use for NTR extension calculations. \n
    6) Find max of NTR, PR, GTR min date. \n
    7) Find min of NTR, PR, GTR max date. \n
    8) Identify the overlap period using `parse_check_overlap_period`. \n
    9) Compute div yield for overlap period and use div yield to compute tax rate. \n
    10) Agg. tax rate based on period index identified by div_method and get avg tax rate. \n
    11) Use tax_rate_df to extend NTR on dates when both PR and GTR value is known. \n
    12) Writes results to DB using `write_df`.

    :param asset_code:
    :param price_type:
    :param min_period:
    :param max_period:
    :param div_method:
    :param tax_min:
    :param tax_max:
    :return: extended_df
    '''

    at, pt, st, sp, pl = read_dependency(asset_type=asset_code, price_type=price_type, proxy_level=2)

    l1_at = at[0]
    l1_pt = pt[0]
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

    # Check if for the asset type level 2 proxy data exist when price type is PR or GTR
    level_two_sql = ''' SELECT * FROM time_series_proxy WHERE Asset_Code = '{}'
                                AND Price_Type IN ('PR','GTR') and Proxy_Level = 2 ''' \
        .format(asset_code)

    # add level 2 data if available.
    df = pd.concat([pd.read_sql(level_two_sql, create_connection()), df])

    df = df[['Date', 'Price', 'Asset_Code', 'Price_Type']]

    df, months, factor = create_period_index(df, div_method)

    # Slice PR, GTR & NTR series
    pr_series = df[df['Price_Type'] == 'PR']
    gtr_series = df[df['Price_Type'] == 'GTR']
    ntr_series = df[df['Price_Type'] == 'NTR']

    # Set date as index
    pr_series.set_index('Date', inplace=True)
    gtr_series.set_index('Date', inplace=True)
    ntr_series.set_index('Date', inplace=True)

    # Computing overlap period
    overlap_min_date = max(pr_series.index.min(), gtr_series.index.min(), ntr_series.index.min())
    overlap_max_date = min(pr_series.index.max(), gtr_series.index.max(), ntr_series.index.max())

    end_date, process = parse_check_overlap_period(overlap_min_date, overlap_max_date, min_period,
                                                   max_period,
                                                   div_method)
    if process:
        # resample PR, GTR, NTR as per the div method
        gtr_resampled = gtr_series[(gtr_series.index >= overlap_min_date) & (gtr_series.index <= end_date)]. \
            resample('M').last()
        gtr_resampled = gtr_resampled[gtr_resampled['month_of_year'].isin(months)]
        pr_resampled = pr_series[(pr_series.index >= overlap_min_date) & (pr_series.index <= end_date)]. \
            resample('M').last()
        pr_resampled = pr_resampled[pr_resampled['month_of_year'].isin(months)]
        ntr_resampled = ntr_series[(ntr_series.index >= overlap_min_date) & (ntr_series.index <= end_date)]. \
            resample('M').last()
        ntr_resampled = ntr_resampled[ntr_resampled['month_of_year'].isin(months)]

        # df containing PR and GTR prices for each index of overlap period
        resampled_series = gtr_resampled.reset_index(). \
            rename(columns={'Price': 'GTR_Price'})[['period_index', 'GTR_Price']]. \
            set_index('period_index'). \
            join(pr_resampled.reset_index().rename(columns={'Price': 'PR_Price'})[['period_index',
                                                                                   'PR_Price']]
                 .set_index('period_index'))

        # df containing PR, GTR & NTR prices for each index of overlap period
        resampled_series = resampled_series.join(
            ntr_resampled.reset_index().rename(columns={'Price': 'NTR_Price'})[['period_index',
                                                                                'NTR_Price']].set_index('period_index')
        )

        # compute div yield
        resampled_series['div_yield'] = (resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) - \
                                        (resampled_series['PR_Price'] / resampled_series['PR_Price'].shift())
        # compute tax rate
        resampled_series['tax_rate'] = ((resampled_series['GTR_Price'] / resampled_series['GTR_Price'].shift()) -
                                        (resampled_series['NTR_Price'] / resampled_series['NTR_Price'].shift())) \
                                       / resampled_series['div_yield']

        # Tax rate check to be clarified and added
        if (resampled_series.tax_rate < tax_min).any():
            return print('Minimum Tax Rate requirement not satisfied process terminated.')
        if (resampled_series.tax_rate > tax_max).any():
            return print('Maximum Tax Rate requirement not satisfied process terminated.')

        # create extension to NTR
        # Assumption: On first available NTR series date there exist a value both for GTR and PR
        extended_df = pd.DataFrame(
            index=pd.date_range(max(pr_series.index.min(), gtr_series.index.min()), ntr_series.index.min()))

        # Add period on which avg is to be taken
        if div_method == 'Average Calendar Quarterly':
            resampled_series['period'] = resampled_series.index.quarter
            extended_df['period'] = extended_df.index.quarter

        elif div_method == 'Average Calendar Monthly':
            resampled_series['period'] = resampled_series.index.month
            extended_df['period'] = extended_df.index.month

        elif div_method == 'Average Calendar Semi-Annual':
            resampled_series['period'] = resampled_series.index.str.strip().str[-1]
            extended_df['period'] = np.where(extended_df.reset_index()['index'].dt.month / 12 <= 0.5, '1', '2')

        else:
            resampled_series['period'] = 1
            extended_df['period'] = 1

        # Agg tax rate based on the index derived from div_method
        tax_rate_df = resampled_series.reset_index()[['tax_rate', 'period']].groupby(['period']).mean()

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
        extended_df = extended_df.reset_index().set_index('period').join(tax_rate_df).set_index('index')
        extended_df.sort_index(ascending=False, inplace=True)
        extended_df['tax_rate'] = extended_df['tax_rate'].shift()

        # apply price extension formula
        extended_df['Price'] = (extended_df['Price'].combine_first(1 / (
                ((extended_df['GTR'].shift()) / (extended_df['GTR'])) -
                (((extended_df['GTR'].shift()) / (extended_df['GTR'])) -
                 ((extended_df['PR'].shift()) / (extended_df['PR']))) * extended_df['tax_rate'])).cumprod())

        # restructure df to be passed on to DB
        extended_df['Asset_Code'] = asset_code
        extended_df['Price_Type'] = price_type
        extended_df['Proxy_Level'] = 2
        extended_df['Proxy_Name'] = np.nan
        extended_df.reset_index(inplace=True)
        extended_df = extended_df[['index', 'Asset_Code', 'Price_Type', 'Price', 'Proxy_Level', 'Proxy_Name']]
        extended_df.rename(columns={'index': 'Date'}, inplace=True)
        extended_df = extended_df[1:]
        extended_df.sort_values('Date', inplace=True)

        # write to db
        write_data(extended_df)

        return extended_df
    # process terminated if minimum overlap check is failed
    else:
        return print('Overlap requirement not satisfied process terminated.')
