import glob
import os
from functools import wraps
from pathlib import Path

import duckdb
import typer

from utils import cols, with_res_logger

app = typer.Typer()


class G:
    conn = None

    @classmethod
    def set(cls, conn):
        cls.conn = conn

    @classmethod
    def get(cls):
        return cls.conn

    @classmethod
    def clear(cls):
        del cls.conn


def with_duckdb(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with duckdb.connect() as conn:
            G.set(conn)
            res = f(*args, **kwargs)
            G.clear()
            return res

    return wrapper


duckdb_types = {
    "int": "NUMERIC",
    "float": "NUMERIC",
    "cat": "TEXT",
    "str": "TEXT",
    "date": "DATE",
}


def load(year):
    conn = G.get()
    cols_stmt = ",".join(f"{k} {duckdb_types[v]}" for k, v in cols.items())
    conn.execute(f"CREATE TABLE lines ({cols_stmt})")
    p = Path(os.environ["DATA"]) / f"{year}.csv"
    if "*" in year:
        for f in glob.glob(str(p)):
            conn.execute(f"COPY lines FROM '{f}' ( HEADER TRUE );")
    else:
        conn.execute(f"COPY lines FROM '{p}' ( HEADER TRUE );")


@app.command()
@with_duckdb
@with_res_logger
def top_flop(year: str):
    load(year)
    conn = G.get()
    res = conn.execute(
        """
        SELECT
            code_postal,
            count(DISTINCT id_mutation) as mutations
        FROM lines
        GROUP BY code_postal
        ORDER BY mutations DESC
        """
    ).fetchall()
    print(res[:10])
    print(res[-10:])
