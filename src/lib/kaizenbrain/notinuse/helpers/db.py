from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text

def get_engine(user, password, host, database):
    return create_engine("postgresql://{}:{}@{}:5432/{}".format(
        user, password, host, database
    ), echo=False)

def fetch_all(conn, sql):
    return run_sql(conn, sql).mappings().all()

def fetch_one(conn, sql):
    return run_sql(conn, sql).mappings().first()

def run_sql(conn, sql):
    return conn.execution_options(autocommit=True).execute(text(sql))
    
def truncate_table(conn, table_name):
    return conn.execution_options(autocommit=True).execute(text(f"""TRUNCATE TABLE {table_name}"""))

def insert_on_conflict_nothing_assets(table, conn, keys, data_iter):
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
    
