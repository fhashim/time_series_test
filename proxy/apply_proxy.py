import pandas as pd

import numpy as np

from mvn_historical_drawdowns import read_data

from db_connection import create_connection, connection_engine

from dateutil.relativedelta import relativedelta


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

        work_df = work_df.iloc[:loc+1, ]

        work_df.set_index('Date', inplace=True)

        work_df['Price_Type'] = 'GTR'

        test_df = work_df.join(pr_series['Price'].rename('PR_Price'))

        test_df = test_df[test_df.index <= '1988-01-04']

        test_df['PR_Final'] = np.where(test_df['PR'].isna(), test_df['PR_Price'], test_df['PR'])

        test_df = test_df.reset_index().sort_values('Date', ascending=False)

        test_df['Prox_Price'] = np.where(test_df['Price_Type'] == 'GTR', test_df['Price'], np.nan)

        test_df['Days'] = ((test_df['Date'].shift() - test_df['Date']).dt.days)/365.25

        test_df['Prox_Price'] = (test_df['Prox_Price'].combine_first(1/((test_df['PR_Final'].shift() / test_df['PR_Final']) +
                                                                     (test_df['div_yield']*test_df['Days'])))
                                 .cumprod())

        test_df['Prox_Price'] = (test_df['Prox_Price'].combine_first(test_df['PR_Final'].shift() / test_df.eval('PR_Final')
                                                                     ).cumprod())




        work_df['Price_Proxied'] = work_df['Price']

        work_df['Price_Proxied'] = work_df['Price_Proxied']

        df['Price'] = (df['Price'].combine_first(df['Proxy'].shift() / df.eval('Proxy*Div*Days'))
                       .cumprod().round(2))

    pass


data = {'Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04', '2021-01-05',
                 '2021-01-06', '2021-01-07'],
        'Price': [np.nan, np.nan, np.nan, np.nan, 10, 11, 32],
        'Proxy': [10, 20, 30, 24, 35, 45, 32],
        'Calc': [np.nan, np.nan, np.nan, np.nan, 10 / 11, np.nan, np.nan]}
tester = pd.DataFrame(data)
tester = tester.sort_values('Date', ascending=False).reset_index(drop=True)
tester = tester.iloc[2:,:]
tester['Ca'] = tester.Calc.ffill()/tester.Calc.cumprod().shift(fill_value=1)


data = {'Date': ['2021-01-13', '2021-01-08', '2021-01-04', '2021-01-03', '2021-01-01'],
        'Price':[10, np.nan, np.nan, np.nan,np.nan],
        'Proxy':[20, 30, 40, 50, 60],
        'Div':[0.5, 0.6, 0.7, 0.8, 0.9],
        'Days':[np.nan, 5, 4, 1, 2]}

tester = pd.DataFrame(data)

tester['Price'] = (tester['Price'].combine_first(tester['Proxy'].shift()/tester.eval('Proxy*Div*Days'))
               .cumprod().round(2))

tester['Price'] = (tester['Price'].combine_first((tester.eval('Proxy')/tester['Proxy'].shift()) + 1/(tester['Div'] *
                                                                                                   tester['Days']))
               .cumprod().round(2))

tester['Price'] = (tester['Price'].combine_first(1/((tester.eval('Proxy').shift()/tester['Proxy']) + (tester['Div'] *
                                                                                                   tester['Days'])))
               .cumprod().round(2))

tester['Price'] = (tester['Price'].combine_first((tester['Proxy'].shift()/ tester.eval('Proxy+(Div*Days)')) +
                                                 tester.eval('Div*Days'))
               .cumprod().round(2))

tester['Price'] = (tester['Price'].combine_first(tester['Proxy'].shift()/tester.eval('Proxy'))
               .cumprod().round(2))