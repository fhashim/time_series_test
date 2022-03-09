from mvn_historical_drawdowns import historical_drawdowns

from Functions.mvn_historical_returns import historical_returns

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
                              None, None],
                             None, None
                             )

results = historical_returns('QQQ US', 'PR', None,
                             ['1Y', '1Y', '1Y', '1Y', '1Y'],
                             ['2Y', '3Y', '5Y', '6Q', '3M'],
                             None, None
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['1Y', '2W', '6M', '3Q', '95D',
                              'Inception', '30Y', '50Y'],
                             [None, None, None, None, None, None, None,
                              None],
                             'NA', None
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             None, '2W'
                             )

results = historical_returns('SPY US', 'PR', None,
                             ['20Y'],
                             [None],
                             '10Y', '3Y'
                             )

asset_code = 'SPY US'
price_type = 'PR'
period_start = '3Q'


asset_code = 'SPY US'
price_type = 'PR'
period_start = '20Y'
comp_freq = '2W'