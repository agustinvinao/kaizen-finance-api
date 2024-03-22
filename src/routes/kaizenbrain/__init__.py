from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer

from flask import Blueprint, jsonify, request
from tvDatafeed import TvDatafeed, Interval

from constants import api_path
from helpers import error_base

DB_HOST = '10.0.0.26'
DB_USER = 'timescaledb'
DB_PASS = '123456'
DB_NAME = 'kaizen_brain_development'

module_path = "kaizenbrain"
kaizenbrain = Blueprint("_".join(["routes", module_path]), __name__)
tv = TvDatafeed()

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}", echo=False)
metadata = MetaData()
Securities = Table('securities', metadata, autoload=True, autoload_with=engine)
Ticks = Table('ticks', metadata, autoload=True, autoload_with=engine)

def fetch_security(conn, symbol):
    return conn.execute(text(f"SELECT id FROM securities WHERE symbol='{symbol}'")).fetchall()

def insert_on_conflict_nothing(table, conn, keys, data_iter):
    # "a" is the primary key in "conflict_table"
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["symbol", "dt"])
    result = conn.execute(stmt)
    return result.rowcount

@kaizenbrain.errorhandler(500)
def error(e):
    error_base(e)

@kaizenbrain.route(
    f"/{api_path}/{module_path}/quotes/<exchange>/<symbol>", methods=["GET"]
)
def quotes(symbol, exchange):
    if request.method == "GET":
        bars = 5000
        df = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=Interval.in_1_hour,
            n_bars=bars,
            extended_session=True,
        )
        df = df.reset_index()
        df["dt"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df.set_index('datetime', inplace=True)

        
        # with engine.connect() as conn:
            # fetch security
            # result = fetch_security(conn, symbol)
            # if len(result) == 0:
            #     # security doesnt exists, insert
            #     insert_stmt = insert(Securities).values(name=symbol, symbol=symbol, exchange=exchange)
            #     do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['id'])
            #     conn.execute(do_nothing_stmt)
            #     # fetch security
            #     # result = fetch_security(conn, symbol)
            # security_id = result[0].id
            # df.loc[:, 'symbol'] = symbol
        # df.drop(columns=['symbol'], inplace=True)
        df['symbol'] = df['symbol'].apply(lambda x: x.split(':')[-1])
        
        df.to_sql('ticks', engine, if_exists='append', index=False, method=insert_on_conflict_nothing)
        
        return jsonify(df.to_dict(orient="records"))


