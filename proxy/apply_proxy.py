import pandas as pd

import numpy as np

from mvn_historical_drawdowns import read_data

from db_connection import create_connection


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
        return df
    else:
        df = df[df.Date >= cutoff]
        return df

