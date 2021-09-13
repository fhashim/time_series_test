'''
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
'''

import pandas as pd

df = pd.read_csv('Data/time_series.csv')
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
Period_Start = 'Inception'
Period_end = 'Latest'
Asset_Code = 'SPY US'
Price_Type = 'GTR'
Rank = 1

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
work_df['idxmax'] = work_df['Previous_Peak'].idxmax()
work_df['Drawdown'] = (work_df['Cum_Returns'] - work_df['Previous_Peak']) / work_df['Previous_Peak']

work_df['Drawdown'].idxmin()

test = pd.DataFrame({'Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04', '2021-01-05',
                              '2021-01-06', '2021-01-07', '2021-01-08', '2021-01-09', '2021-01-10',
                              '2021-01-11', '2021-01-12', '2021-01-13', '2021-01-14'],
                     'Price': [1, 1, 5, 3, 4, 3, 2, 5, 6, 4, 3, 2, 1, 7]})
test['Date'] = pd.to_datetime(test['Date'])
test.set_index('Date', inplace=True)
test['Returns'] = test['Price'].pct_change()
test['Cum_Returns'] = (1 + test['Returns']).cumprod()
test['Previous_Peak'] = test['Cum_Returns'].cummax()