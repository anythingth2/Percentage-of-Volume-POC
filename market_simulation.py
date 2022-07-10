from typing import Generator, Iterable
import datetime
import tqdm
import decimal
import numpy as np
import pandas as pd
import plotly.express as px

import sqlite3
from google.cloud import bigquery


bq = bigquery.Client()
inmem_db = sqlite3.connect(":memory:")

SHIFTING_PRECISION_FACTOR = 10**8

def drop_order_book():
    query = '''DROP TABLE IF EXISTS order_book;'''
    inmem_db.execute(query)

def create_order_book_table():
    query = '''
    CREATE TABLE order_book (
        price BIGINT,
        side VARCHAR(8),
        size BIGINT,
        PRIMARY KEY (price)
    );
    '''
    cur = inmem_db.execute(query)


def find_initial_snapshot_datetime(symbol: str,
                                    start_execution_datetime: datetime.datetime) -> datetime.datetime:

    query = '''
    SELECT MAX(`timestamp`) AS `timestamp`
    FROM `trading_terminal_poc.coinbase_snapshot_timestamp`
    WHERE `symbol` = @symbol
            AND DATETIME(`timestamp`) BETWEEN @start_datetime AND @end_datetime
    '''

    start_datetime = (start_execution_datetime - datetime.timedelta(days=1))
    end_datetime = start_execution_datetime

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('symbol', 'STRING', symbol),
            bigquery.ScalarQueryParameter('start_datetime', 'DATETIME', start_datetime),
            bigquery.ScalarQueryParameter('end_datetime', 'DATETIME', end_datetime)
        ]
    )

    query_job = bq.query(query, job_config=job_config)

    result = query_job.result()
    if result.total_rows > 0:
        initial_snapshot_datetime = next(result).get('timestamp')
        return initial_snapshot_datetime
    else:
        raise NotImplementedError

def query_l2_order_book(symbol: str,
						start_datetime: datetime.datetime, 
						end_datetime: datetime.datetime) -> bigquery.table.RowIterator:
	query = '''
	WITH l2_order_book AS (
        SELECT `timestamp`,
                side,
                size,
                price
		FROM `trading_terminal_poc.coinbase_l2_order_book`
		WHERE `symbol` = @symbol AND
				`timestamp` BETWEEN @start_datetime AND @end_datetime 
	),
	norm_l2 AS (
		SELECT DATE_TRUNC(`timestamp`, SECOND) AS `timestamp_norm`,
				price,
				MAX(`timestamp`) AS `timestamp`
		FROM l2_order_book
		GROUP BY 1, 2
	)
	SELECT norm_l2.timestamp_norm,
			l2_order_book.side,
			norm_l2.price,
			l2_order_book.`size` AS `size`
	FROM norm_l2
	INNER JOIN l2_order_book USING (`timestamp`, price)
	ORDER BY norm_l2.timestamp_norm
	'''

	job_config = bigquery.QueryJobConfig(
		query_parameters=[
			bigquery.ScalarQueryParameter('symbol', 'STRING', symbol),
			bigquery.ScalarQueryParameter('start_datetime', 'DATETIME', start_datetime),
			bigquery.ScalarQueryParameter('end_datetime', 'DATETIME', end_datetime)
		]
	)

	query_job = bq.query(query, job_config=job_config)
	result = query_job.result()

	return result

def query_order_book(from_price: decimal.Decimal = None,
                        to_price: decimal.Decimal = None,
                        coerce_float: bool = False
                    ) -> pd.DataFrame:
    
    if from_price is not None:
        from_price = int(from_price * SHIFTING_PRECISION_FACTOR)
    
    if to_price is not None:
        to_price = int(to_price * SHIFTING_PRECISION_FACTOR)
        
    if from_price is not None and to_price is not None:
        query = '''
        SELECT *
        FROM order_book
        WHERE from_price > ? AND to_price < ?
        ORDER BY price
        '''

        params = (from_price, to_price)

    elif from_price is not None:
        query = '''
        SELECT *
        FROM order_book
        WHERE from_price > ? 
        ORDER BY price
        '''

        params = (from_price, )

    elif to_price is not None:
        query = '''
        SELECT *
        FROM order_book
        WHERE to_price < ?
        ORDER BY price
        '''

        params = (to_price, )
    else:
        query = '''
        SELECT *
        FROM order_book
        ORDER BY price
        '''
        params = None

    df = pd.read_sql(query, inmem_db, params=params, coerce_float=coerce_float)
    df.loc[:, ['price', 'size']] /= SHIFTING_PRECISION_FACTOR


    return df

def upsert_order_book(price: decimal.Decimal, side: str, size: decimal.Decimal):
    query = '''
    INSERT INTO order_book(price, side, size)
        VALUES (?, ?, ?)
        ON CONFLICT(price)
            DO UPDATE SET side = ?,
                            size = ?
    '''
    price = int(price * SHIFTING_PRECISION_FACTOR)
    size = int(size * SHIFTING_PRECISION_FACTOR)

    params = (price, side, size, side, size)

    cursor = inmem_db.cursor()
    cursor.execute(query, params)

def delete_order_book(price: decimal.Decimal):
    query = '''
    DELETE FROM order_book
    WHERE price = ?
    '''

    price = int(price * SHIFTING_PRECISION_FACTOR)
    params = (price, )

    cursor = inmem_db.cursor()
    cursor.execute(query, params)

def truncate_order_book():
    query = '''
    DELETE FROM order_book
    '''

    cursor = inmem_db.cursor()
    cursor.execute(query)

def query_median_price() -> float:
    query = '''
    SELECT MAX(price) AS buy_price
    FROM order_book
    WHERE side = "buy"
    '''
    buy_price_df = pd.read_sql(query, inmem_db)
    buy_price = buy_price_df.iloc[0]['buy_price']

    query = '''
    SELECT MIN(price) AS sell_price
    FROM order_book
    WHERE side = "sell"
    '''
    sell_price_df = pd.read_sql(query, inmem_db)
    sell_price = sell_price_df.iloc[0]['sell_price']

    median_price = (buy_price + sell_price) / 2
    median_price /= SHIFTING_PRECISION_FACTOR 
    return median_price

def query_snapshot_timestamp(symbol: str, 
                                start_datetime: datetime.datetime, 
                                end_datetime: datetime.datetime) -> pd.Series:
    query = '''
    SELECT `timestamp`
    FROM `trading_terminal_poc.coinbase_snapshot_timestamp`
    WHERE symbol = @symbol AND
            `timestamp` BETWEEN @start_datetime AND @end_datetime 
    ORDER BY `timestamp`
    '''

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('symbol', 'STRING', symbol),
			bigquery.ScalarQueryParameter('start_datetime', 'DATETIME', start_datetime),
			bigquery.ScalarQueryParameter('end_datetime', 'DATETIME', end_datetime)
        ]
    )

    query_job = bq.query(query, job_config=job_config)
    return query_job.to_dataframe()['timestamp']


class MarketSimulator:
    def __init__(self,
                    symbol: str,
                    start_datetime: datetime.datetime,
                    end_datetime: datetime.datetime
                ):
        self.symbol = symbol
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

        drop_order_book()
        create_order_book_table()

    def pre_simulate(self):

        initial_snapshot_datetime = find_initial_snapshot_datetime(self.symbol, self.start_datetime)
        order_book_result = query_l2_order_book(self.symbol, initial_snapshot_datetime, self.start_datetime)

        # Iterate over all record of order book in either same timestamp and next timestamp
        for row in tqdm.tqdm(order_book_result, total=order_book_result.total_rows):
            price = row.get('price')
            side = row.get('side')
            size = row.get('size')
            if size > 0:
                
                upsert_order_book(price=price,
                                    side=side,
                                    size=size
                )
            else:
                delete_order_book(price=price)

    def simulate(self) -> Iterable[datetime.datetime]:

        snapshot_timestamps = query_snapshot_timestamp(self.symbol, self.start_datetime, self.end_datetime)
        order_book_result = query_l2_order_book(self.symbol, self.start_datetime, self.end_datetime)

        current_datetime = None
        # Iterate over all record of order book in either same timestamp and next timestamp
        for row in tqdm.tqdm(order_book_result, total=order_book_result.total_rows):
            order_book_datetime = row.get('timestamp_norm')

            # Check new timeframe
            if current_datetime is not None:
                # Yield if start new timeframe
                if order_book_datetime > current_datetime:
                    yield current_datetime
                    current_datetime = order_book_datetime
            else:
                current_datetime = order_book_datetime

            # Does cursor reach new snapshot ?
            if len(snapshot_timestamps) > 0 and order_book_datetime > snapshot_timestamps.min():
                snapshot_timestamps.pop(snapshot_timestamps.index[0])
                truncate_order_book()

            price = row.get('price')
            side = row.get('side')
            size = row.get('size')
            if size > 0:
                
                upsert_order_book(price=price,
                                    side=side,
                                    size=size
                )
            else:
                delete_order_book(price=price)
        
        yield current_datetime
    
    def simulate_with_granularity(self, granularity: int) -> Iterable[datetime.datetime]:
        market_iterator = self.simulate()
        while True:
            for _ in range(granularity):
                current_datetime = next(market_iterator, None)
                if current_datetime is None:
                    break
            yield current_datetime
        

    def plot_bid_ask(self):

        median_price = query_median_price()
        order_book_df = query_order_book(coerce_float=True)
        order_book_df = order_book_df.loc[(order_book_df['price'] > median_price / 2) & (order_book_df['price'] < median_price * 3 / 2)]
        
        return px.histogram(order_book_df,
            x='size',
            y='price',
            orientation='h',
            color='side',
            histfunc='sum'
            )
            