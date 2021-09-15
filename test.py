import pandas as pd

test = pd.DataFrame({'Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04', '2021-01-05',
                              '2021-01-06', '2021-01-07', '2021-01-08', '2021-01-09', '2021-01-10',
                              '2021-01-11', '2021-01-12', '2021-01-13', '2021-01-14'],
                     'Price': [1, 1, 5, 3, 4, 3, 2, 5, 6, 4, 3, 2, 1, 7]})
test['Date'] = pd.to_datetime(test['Date'])
test.set_index('Date', inplace=True)
test['Returns'] = test['Price'].pct_change()
test['Cum_Returns'] = (1 + test['Returns']).cumprod()
test['Previous_Peak'] = test['Cum_Returns'].cummax()

x = pd.DataFrame(
    pd.concat([test.Cum_Returns, test.index.to_series()], axis=1)
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
test[["Previous_Peak", "Previous_Peak_index", "Next_PP_index"]] = x.values
print(test)

### All work
main_df = df[(df['Asset Code'] == 'SPY US') & (df['Price Type'] == 'GTR')]

if Period_Start == 'Inception':
    start_date = main_df.Date.min()

if Period_end == 'Latest':
    end_date = main_df.Date.max()

work_df = pd.DataFrame(index=pd.date_range(start_date, end_date))
work_df = work_df.join(main_df.set_index('Date'))

work_df.ffill(inplace=True)
work_df.bfill(inplace=True)

work_df['Returns'] = work_df['Price'].pct_change()
work_df['Cum_Returns'] = (1 + work_df['Returns']).cumprod()
work_df['Previous_Peak'] = work_df['Cum_Returns'].cummax()
# work_df['idxmax'] = work_df['Previous_Peak'].idxmax()
work_df['Drawdown'] = (work_df['Cum_Returns'] - work_df['Previous_Peak']) / work_df['Previous_Peak']

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

# work_df['Drawdown'].idxmin()
#
# work_df[work_df.index == '2009-03-09']

# df.rename(columns={"A": "a", "B": "c"})

int_df = pd.DataFrame(index=work_df['Previous_Peak_index'])
int_df = int_df.join(work_df['Price'])
int_df.columns = ['PP_Price']
work_df['PP_Price'] = int_df['PP_Price'].values
# final_df = work_df.set_index('Previous_Peak_index').\
#     join(work_df['Price'].rename("PP_Price"))
work_df.reset_index(inplace=True)
work_df['Recovery_Days'] = work_df['index'] - work_df['Previous_Peak_index']
work_df['Recovery_Days'] = (work_df['Next_PP_index'] - work_df['index']).dt.days
work_df[work_df.index == '2009-03-09']
work_df.to_csv('work_df2.csv')

# work_df.iloc[work_df.groupby('Previous_Peak_index')['Drawdown'].idxmin(), :].tail()

work_df.sort_values('Drawdown').drop_duplicates('Previous_Peak_index')
groupby_obj = work_df.groupby('Previous_Peak_index')
groupby_obj.nsmallest(3, 'Drawdown')

Period_End = '1Y'
chars = set('MDWY')

try:
    if Period_End == 'Latest':
        end_date = main_df.Date.max()
    elif any((c in chars) for c in Period_End):
        end_date = main_df.Date.max()
        end_date = end_date - pd.tseries.frequencies.to_offset(Period_End)
    else:
        end_date = str(parse(Period_End, fuzzy=False))

except ValueError:
    raise ValueError("Period End date is not correct")

print(end_date)

int(filter(str.isdigit, Period_End))

###

test = pd.DataFrame({'Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-05', '2021-01-08', '2021-01-09'],
                   'Price': [10, 20, 30, 40, 20, 10]})
test['Date'] = pd.to_datetime(test['Date'])

end_dates = ['2021-01-01', '2021-01-06', '2021-01-07', '2021-01-09']

for end_date in end_dates:
    print('End Date: {}'.format(end_date))
    end_date = test.loc[test['Date'] <= end_date, 'Date'].iloc[-1]
    print('Updated End Date: {}'.format(end_date))