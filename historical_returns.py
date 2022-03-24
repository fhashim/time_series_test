"""'

Calculate historic returns for an asset/portfolio

Function Signature:

mvn_historical_returns (Maven_Asset_ID, Price_Type, *Currency*,
    [Period_Start], *[Period_End], Normalisation, CompoundingFrequency*)

returns [FLOAT]

Function Inputs:

- Maven Asset ID - STRING - Price Type - STRING - Currency - STRING
optional - Period Start - [DATE or STRING] - Can be exact date ; an
offset D/W/M/ Q/Y from Period End Date; or Inception(ie starting
Oldest Price Date) - Period End - [DATE or STRING] optional. Defaults
to Latest Price Date. Can be exact date or as an offset D/W/M/Q/Y
from Latest Price Date - Normalisation - STRING optional - In
D/W/M/Q/Y. Can also be set to None. Defaults to 1Y -
CompoundingFrequency - STRING optional - In D/W/M/Q/Y. Defaults to 1Y

Function Outputs:

- Rate of Return - [FLOAT]

Methodology:

- If Currency is provided, calculate the price for each date needed from
    the time-series as follows
- AssetPrice*CCYUSD(Asset Currency)/CCYUSD(Currency)
- FX rate fetched from FX Time-Series table
- Asset Currency is fetched from Universal Master by looking up Currency 
    for the Maven Asset ID
- For assets where CCY = USD, CCYUSD = 1
- When FX rate not available for a given date, use prior FX
- Rate of Return(Normalisation is not None) = (((EndValue/StartValue)^
    (CompoundingFrequency(days)/
    NumberofDays from PeriodStart to Period End))-1)*
    Normalisation(days)/CompoundingFrequency(days)
- Rate of Return(Normalisation is None) = EndValue/StartValue - 1

"""
import re

from typing import Union

import pandas as pd

import numpy as np

from dateutil.parser import parse

from dateutil.relativedelta import relativedelta

from db_connection import create_connection


def read_data(code: str, price_type: str) -> pd.DataFrame:
    """
    :param code: Asset Code String
    :param price_type: Price Type String
    :return: Pandas DataFrame
    """

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



def hist_returns(main_df: pd.DataFrame,
                 period_start: Union[str, None],
                 period_end: Union[str, None]):


    pass

asset_id = 'SPY US'
price_type = 'PR'
start_date = '1Y'#,2W,6M,3Q,95D,Inception,30Y,50Y])
end_date = 'Latest'
normalisation = '1Y'
cfreq='1Y'

df = read_data(asset_id, price_type)
start_date_p, end_date_p = parse_dates(start_date, end_date, df)

main_df = df.set_index('Date')


main_df = main_df[(main_df.index >= start_date_p) & (main_df.index <= end_date_p)]


pe_ps_days = (end_date_p -start_date_p).days

rr = ((main_df.values[-1]/main_df.values[0])**((365.25/pe_ps_days)-1) * (365.25/365.25)


