DB_HOST = '10.0.0.26'
DB_USER = 'timescaledb'
DB_PASS = '123456'
DB_NAME = 'kaizen_brain_development'

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text

def run_sql(con, sql):
    return con.execution_options(autocommit=True).execute(text(sql)).mappings().all()
    
def truncate_table(con, table_name):
    return con.execution_options(autocommit=True).execute(text(f"""TRUNCATE TABLE {table_name}"""))

def get_engine():
    return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}", echo=False)

def insert_on_conflict_nothing_securities(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(
        index_elements=["symbol"]
    )
    result = conn.execute(stmt)
    return result.rowcount

def insert_on_conflict_nothing_transactions(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(
        index_elements=["id"]
    )
    result = conn.execute(stmt)
    return result.rowcount

def insert_on_conflict_nothing_ticks(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(
        index_elements=["symbol","dt"]
    )
    result = conn.execute(stmt)
    return result.rowcount

def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
    
