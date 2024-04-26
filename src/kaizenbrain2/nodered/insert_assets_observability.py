from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text
import pandas as pd

def insert_on_conflict_nothing_ticks(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(
        index_elements=["symbol","tf"]
    )
    result = conn.execute(stmt)
    return result.rowcount

data = msg['payload']['rows']
engine = create_engine(f"postgresql://timescaledb:123456@timescaledb:5432/kaizen_brain_development", echo=False)
table = 'assets_observability'
df = pd.DataFrame(data=data)

df.reset_index(inplace=True)
df.drop(columns=['index'], inplace=True)
df.to_sql(table, engine, if_exists='append', index=False, method=insert_on_conflict_nothing_ticks)

msg['df'] = str(df)

return msg


