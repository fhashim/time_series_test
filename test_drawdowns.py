from mvn_historical_drawdowns import historical_drawdowns

from Functions.mvn_historical_returns import historical_returns

from Functions.mvn_historical_returns_dollar import \
    historical_returns_dollar

from Functions.mvn_historical_sharpe_ratio import historical_sharpe_ratio

from Functions.mvn_historical_sortino_ratio import historical_sortino_ratio

asset_code = 'SPY US'
price_type = 'GTR'
period_start = ['2021-12-31', '2020-12-31', '2019-12-31', '2018-12-31',
                '2017-12-31', '2016-12-31', '2015-12-31', '2014-12-31',
                '2013-12-31', '2012-12-31', '2011-12-31', '2010-12-31',
                '2009-12-31',
                '2008-12-31', '2007-12-31']

period_end = ['2022-12-31', '2021-12-31', 'balgham', '2019-12-31',
              '2018-12-31', '2017-12-31', '2016-12-31', '2015-12-31',
              '2014-12-31', '2013-12-31', '2012-12-31', '2011-12-31',
              '2010-12-31', '2009-12-31', '2008-12-31']
rank = [None, None, None, None, None,
        None, None, None, None, None,
        None, None, None, None, None]

#
# asset_code = 'SPY US'
# price_type = 'GTR'
# period_start = ['2017-12-31']
#
# period_end = ['2018-12-31']
# rank = [None]

results = historical_drawdowns(asset_code, price_type, period_start,
                               period_end, rank)

historical_returns

results = historical_returns('SPY US', 'PR', None,
                             ['1Y', '2W', '6M', '3Q', '95D',
                              'Inception', '30Y', '50Y'],
                             [None, None, None, None, None, None,
                              None, None]
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['1Y', '1Y', '1Y', '1Y', '1Y'],
                             ['2Y', '3Y', '5Y', '6Q', '3M']
                             )

results = historical_returns('QQQ US', 'PR', None,
                             ['1Y', '1Y', '1Y', '1Y', '1Y'],
                             ['2Y', '3Y', '5Y', '6Q', '3M']
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['1Y', '2W', '6M', '3Q', '95D',
                              'Inception', '30Y', '50Y'],
                             [None, None, None, None, None, None, None,
                              None],
                             normalisation_freq=None
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             compounding_freq='2W'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             compounding_freq='2M'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             compounding_freq='2Q'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             compounding_freq='50D'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             compounding_freq='3Y'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             '6M', '2W'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             '3Q', '2M'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             '5Y', '2Q'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             '30W', '50D'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             '10Y', '3Y'
                             )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['1Y', '2W', '6M', '3Q', '95D',
                                     'Inception', '30Y', '50Y'],
                                    [None, None, None, None, None, None,
                                     None, None], 1000, None
                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['1Y', '1Y', '1Y', '1Y', '1Y'],
                                    ['2Y', '3Y', '5Y', '6Q', '3M'],
                                    1000, None
                                    )

results = historical_returns_dollar('QQQ US', 'PR', None,
                                    ['1Y', '1Y', '1Y', '1Y', '1Y'],
                                    ['2Y', '3Y', '5Y', '6Q', '3M'],
                                    1000, None
                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['1Y', '2W', '6M', '3Q', '95D',
                                     'Inception', '30Y', '50Y'],
                                    [None, None, None, None, None, None,
                                     None, None], 1000, '1Y', '1Y'
                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['20Y'],
                                    [None], 1000, compounding_freq='2W'

                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['20Y'],
                                    [None], 1000, '6M', '2W'

                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['20Y'],
                                    [None], 1000, '3Q', '2M'

                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['20Y'],
                                    [None], 1000, '5Y', '2Q'

                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['20Y'],
                                    [None], 1000, '30W', '50D'

                                    )

results = historical_returns_dollar('SPY US', 'PR', None,
                                    ['20Y'],
                                    [None], 1000, '10Y', '3Y'

                                    )

results = historical_drawdowns('QQQ US', 'PR', ['2021-12-31', '3M', '6M',
                                              '1Y', '3Y', '5Y', '10Y',
                                              '15Y', '20Y', '30Y',
                                              'Inception', '2020-06-30'],
                             [None, None, None, None, None, None, None,
                              None, None,None, None, None],
                            [None, None, None, None, None, None,
                             None, None, None,None, None, None]
                             )


results = historical_sharpe_ratio('SPY US', 'PR', None,['1Y','2W','6M','3Q','95D','Inception','30Y','50Y'],
                                  [None, None, None, None, None, None,
                                   None, None]
                                  )
print(results)
results = historical_sharpe_ratio('SPY US', 'PR', None, ['1Y', '1Y', '1Y', '1Y', '1Y'],
                                  ['2Y','3Y','5Y','6Q','3M']
                                  )
print(results)
results = historical_sharpe_ratio('QQQ US', 'PR', None, ['1Y', '1Y', '1Y', '1Y', '1Y'],
                                  ['2Y','3Y','5Y','6Q','3M']
                                  )

print(results)
results = historical_sharpe_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.95,
                                  riskfree_rate=0.01
                                  )
print(results)
results = historical_sharpe_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.99,
                                  riskfree_rate=0.02
                                  )
print(results)
results = historical_sharpe_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.9,
                                  riskfree_rate=0.05
                                  )
print(results)
results = historical_sharpe_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.9999
                                  )
print(results)




results = historical_sortino_ratio('SPY US', 'PR', None,['1Y','2W','6M','3Q','95D','Inception','30Y','50Y'],
                                  [None, None, None, None, None, None,
                                   None, None]
                                  )
print(results)
results = historical_sortino_ratio('SPY US', 'PR', None, ['1Y', '1Y', '1Y', '1Y', '1Y'],
                                  ['2Y','3Y','5Y','6Q','3M']
                                  )
print(results)
results = historical_sortino_ratio('QQQ US', 'PR', None, ['1Y', '1Y', '1Y', '1Y', '1Y'],
                                  ['2Y','3Y','5Y','6Q','3M']
                                  )

print(results)
results = historical_sortino_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.95,
                                  riskfree_rate=0.01
                                  )
print(results)
results = historical_sortino_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.99,
                                  riskfree_rate=0.02
                                  )
print(results)
results = historical_sortino_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.9,
                                  riskfree_rate=0.05
                                  )
print(results)
results = historical_sortino_ratio('SPY US', 'PR', None,['1Y','2W','6M','2Q','95D','Inception'],
                                  [None, None, None, None, None, None],
                                  lambda_factor = 0.9999
                                  )
print(results)
