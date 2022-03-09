from typing import Union

from Functions.mvn_historical_returns import historical_returns


def historical_returns_dollar(asset_code: str, price_type: str,
                              currency: str, period_start: list,
                              period_end: list,
                              amount: Union[float, int],
                              normalisation_freq: Union[str, None]
                              = None,
                              compounding_freq: str = '1Y'
                              ) -> dict:
    """
    :param asset_code:
    :param price_type:
    :param currency:
    :param period_start:
    :param period_end:
    :param normalisation_freq:
    :param compounding_freq:
    :param amount:
    :return: results:
    """
    results = historical_returns(asset_code, price_type, currency,
                                 period_start, period_end,
                                 normalisation_freq, compounding_freq)

    results['rate_of_return'] = [
        round(x * amount, 6) if not isinstance(x, str) else x for x in
        results['rate_of_return']]

    return results
