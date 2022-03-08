import pandas as pd

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
