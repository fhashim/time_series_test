import pandas as pd

df = pd.read_excel('Data/time_series.xlsx', header=None)

data = df.iloc[3:, ] \
    .set_index(0).rename_axis('Date') \
    .T \
    .set_index(pd.MultiIndex.from_arrays(df.iloc[:2, 1:].values, names=df.iloc[:2, 0])) \
    .T \
    .stack(level=[0, 1]) \
    .rename('Price') \
    .reset_index()


data.to_csv('Data/time_series.csv', index=False)
