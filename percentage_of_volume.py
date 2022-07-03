from typing import Generator, Iterable, Dict, List
import datetime
import tqdm
from decimal import Decimal
import numpy as np
import pandas as pd
import plotly.express as px
from dateutil import parser as date_parser

from sklearn.model_selection import ParameterGrid, ParameterSampler
from scipy.stats import loguniform, uniform

from google.cloud import bigquery

from market_simulation import MarketSimulator, inmem_db, SHIFTING_PRECISION_FACTOR

bq = bigquery.Client()

def find_current_price(side: str) -> Decimal:
    if side == 'buy':
        query = '''
            SELECT MAX(price) AS price
            FROM order_book
            WHERE side = "buy"
        '''
    else:
        query = '''
            SELECT MIN(price) AS price
            FROM order_book
            WHERE side = "sell"
        '''

    price = pd.read_sql(query, inmem_db).iloc[0]['price']
    price = Decimal(int(price))
    price /= SHIFTING_PRECISION_FACTOR
    return price


def query_order_book(side: str,
                     from_price: Decimal,
                     to_price: Decimal,
                     ascending=True,
                     coerce_float: bool = False
                     ) -> pd.DataFrame:

    from_price = int(from_price * SHIFTING_PRECISION_FACTOR)
    to_price = int(to_price * SHIFTING_PRECISION_FACTOR)

    if ascending:
        ORDER_BY_FLAG = 'ASC'
    else:
        ORDER_BY_FLAG = 'DESC'

    query = f'''
    SELECT *
    FROM order_book
    WHERE side = ? AND price > ? AND price < ?
    ORDER BY price {ORDER_BY_FLAG}
    '''

    params = (side, from_price, to_price)

    df = pd.read_sql(query, inmem_db, params=params, coerce_float=coerce_float)
    df.loc[:, ['price', 'size']] /= SHIFTING_PRECISION_FACTOR
    return df

def PoV(
        symbol: str,
        side: str,
        start_execution_datetime: datetime.datetime,
        max_processing_time: int,
        percentage: float,
        acceptable_slippage: float,
        delay: int,
        target_order_amount: int
) -> Dict:

    end_execution_datetime = start_execution_datetime + \
        datetime.timedelta(seconds=max_processing_time)

    market_simulator = MarketSimulator(symbol=symbol,
                                       start_datetime=start_execution_datetime,
                                       end_datetime=end_execution_datetime
                                       )
    market_simulator.pre_simulate()

    reference_price = find_current_price(side=side)

    if side == 'buy':
        limit_price = reference_price * Decimal(1 + acceptable_slippage)
    else:
        limit_price = reference_price * Decimal(1 - acceptable_slippage)

    market_iterator = market_simulator.simulate_with_granularity(
        granularity=delay)

    remaining_order_amount = target_order_amount

    last_timestamp = None
    while remaining_order_amount > 0:

        timestamp = next(market_iterator, None)
        if timestamp is None:
            break

        current_price = find_current_price(side=side)

        if side == 'buy':
            order_book_df = query_order_book(side='sell',
                                             from_price=min(
                                                 current_price, reference_price),
                                             to_price=limit_price,
                                             ascending=True
                                             )
        else:
            order_book_df = query_order_book(side='buy',
                                             from_price=limit_price,
                                             to_price=max(
                                                 current_price, reference_price),
                                             ascending=False
                                             )

        total_amount_in_band = (
            order_book_df['price'] * order_book_df['size']).sum()
        order_amount = total_amount_in_band * percentage  # In USD

        remaining_order_amount = max(
            remaining_order_amount - order_amount, Decimal(0))

        last_timestamp = timestamp

    result = {
        'symbol': symbol,
        'percentage': percentage,
        'acceptable_slippage': acceptable_slippage,
        'delay': delay,
        'side': side,
        'target_order_amount': target_order_amount,
        'start_execution': start_execution_datetime.isoformat(timespec='microseconds'),
        'end_execution': last_timestamp.isoformat(timespec='microseconds'),
        'expected_end_execution': end_execution_datetime.isoformat(timespec='microseconds'),
        'reference_price': float(reference_price),
        'limit_price': float(limit_price),
        'remaining_order_amount': float(remaining_order_amount),
        'success': bool(remaining_order_amount == 0)
    }

    return result
