import os
import sys
from functools import wraps
from pathlib import Path

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession
from typer import Typer

from utils import cols, with_res_logger

app = Typer()


class G:
    session = None
    sc = None

    @classmethod
    def update(cls, session, sc):
        cls.session = session
        cls.sc = sc

    @classmethod
    def get(cls):
        return cls.session, cls.sc

    @classmethod
    def clear(cls):
        del cls.session
        del cls.sc


def with_spark(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with SparkSession.builder.appName("py-df-cmp").getOrCreate() as session:
            log_level = os.environ.get("LOG_LEVEL", None)
            if log_level is None:
                log_level = "ERROR"
            sc = session.sparkContext
            sc.setLogLevel(log_level)
            G.update(session, sc)
            print("### Begin output ###\n", file=sys.stderr)
            res = f(*args, **kwargs)
            print("\n### End output ###", file=sys.stderr)
            G.clear()
            return res

    return wrapper


spark_types = {
    "int": t.IntegerType,
    "float": t.FloatType,
    "cat": t.StringType,
    "str": t.StringType,
    "date": t.DateType,
}


def load(year):
    session, sc = G.get()
    schema = t.StructType()
    for k, v in cols.items():
        schema = schema.add(k, spark_types[v](), True)
    if year is None:
        return session.read.csv(
            str(Path(os.environ["DATA"]).absolute()), header=True, schema=schema
        )
    else:
        return session.read.csv(
            str((Path(os.environ["DATA"]) / f"{year}.csv").absolute()),
            header=True,
            schema=schema,
        )


@app.command()
@with_spark
@with_res_logger
def top_flop(year: str):
    df = (
        load(year)
        .select("code_postal", "id_mutation")
        .groupby("code_postal")
        .agg(f.count_distinct("id_mutation").alias("count"))
        .orderBy("count", ascending=False)
        .collect()
    )
    print(df[:10])
    print(df[-10:])
