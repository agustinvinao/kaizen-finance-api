import logging
import sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.sql import text


class Storage:
    config: None
    engine: None

    # QUERIES beginning
    query_assets_by_timeframe_and_exchange = """SELECT a.symbol, a.exchange, etz.tz, ao.tf, 'ticks_' || ao.tf AS table, bars.default_bars
      FROM assets a
        JOIN assets_observability ao ON a.symbol = ao.symbol
        JOIN exchanges_tz etz ON a.exchange = etz.exchange
        JOIN (SELECT * FROM (
                VALUES ('4h'::timeframe, 28),('1d'::timeframe, 5),('1w'::timeframe, 5)
              ) AS t (tf, default_bars)
        ) bars ON ao.tf = bars.tf
      WHERE ao.tf = '{timeframe}'"""
    filter_by_exchanges = """ AND a.exchange IN ('{exchanges}')"""
    query_assets_by_timeframe_and_exchange_without_update = """SELECT a.symbol, a.exchange, etz.tz, ao.tf, 'ticks_' || ao.tf AS table, bars.default_bars
      FROM assets a
        JOIN last_ticks_{timeframe} lt ON	a.symbol = lt.symbol
        JOIN assets_observability ao ON lt.symbol = ao.symbol
        JOIN exchanges_tz etz ON a.exchange = etz.exchange
        JOIN (SELECT * FROM (
                  VALUES ('4h'::timeframe, 28),('1d'::timeframe, 5),('1w'::timeframe, 5)
                ) AS t (tf, default_bars)
          ) bars ON ao.tf = bars.tf
        JOIN (SELECT max(dt) AS max_dt FROM last_ticks_{timeframe}) lt_max ON true
      WHERE ao.tf = '{timeframe}' AND lt.dt < lt_max.max_dt"""
    query_asset_lookup = """SELECT a.symbol, a.exchange, bars.tf, 'ticks_' || bars.tf AS table, bars.default_bars
      FROM (SELECT * FROM (VALUES ('{}', '{}')) AS t (symbol, exchange)) AS a
          JOIN (SELECT * FROM (VALUES ('4h'::timeframe, 28),('1d'::timeframe, 5),('1w'::timeframe, 5)) AS t (tf, default_bars)) bars ON true """

    query_asset_by_symbol = """SELECT a.*, etz.tz FROM assets a JOIN exchanges_tz etz ON a.exchange = etz.exchange  WHERE symbol='{}'"""
    query_ticks = """SELECT * FROM ticks_{}"""
    # query_with_pivots = """SELECT t.symbol, t.dt, t.open, t.high, t.low, t.close, t.volume
    #                         FROM ticks_{timeframe} t"""
    #                         # WHERE t.symbol='PYPL'"""
    # QUERIES end

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.engine = create_engine(
            "postgresql://{}:{}@{}:5432/{}".format(
                config["DB_USER"],
                config["DB_PASSWORD"],
                config["DB_HOST"],
                config["DB_NAME"],
            ),
            echo=False,
        )
        logging.basicConfig(level=config["LOG_LEVEL"])
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(config["LOG_LEVEL"])
        if console not in logging.getLogger().handlers:
            logging.getLogger().addHandler(console)
        logging.getLogger().addHandler(console)

    def fetch_all(self, sql: str):
        return self.run_sql(sql).mappings().all()

    def fetch_one(self, sql: str):
        return self.run_sql(sql).mappings().first()

    def run_sql(self, sql: str):
        with self.engine.begin() as conn:
            return conn.execution_options(autocommit=True).execute(text(sql))

    def truncate_table(self, table_name: str):
        with self.engine.begin() as conn:
            return conn.execution_options(autocommit=True).execute(
                text(f"""TRUNCATE TABLE {table_name}""")
            )

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        method,
        if_exists: str = "append",
        index=False,
    ):
        result = df.to_sql(
            table,
            self.engine,
            if_exists=if_exists,
            index=index,
            method=method,
        )
        logging.debug("df to_sql result: {}".format(result))


def insert_on_conflict_nothing_assets(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
        .on_conflict_do_nothing(index_elements=["symbol"])
    )
    result = conn.execute(stmt)
    return result.rowcount


def insert_on_conflict_nothing_transactions(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table).values(data).on_conflict_do_nothing(index_elements=["id"])
    )
    result = conn.execute(stmt)
    return result.rowcount


def insert_on_conflict_nothing_ticks(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
        .on_conflict_do_nothing(index_elements=["symbol", "dt"])
    )
    result = conn.execute(stmt)
    return result.rowcount


def insert_on_conflict_update_ticks(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
        .on_conflict_do_update(index_elements=["symbol", "dt"])
    )
    result = conn.execute(stmt)
    return result.rowcount


def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = Insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)


def insert_on_conflict_update_waves(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
        .on_conflict_do_update(index_elements=["symbol", "dt"])
    )
    result = conn.execute(stmt)
    return result.rowcount
