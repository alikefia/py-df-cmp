import os
import sys
from pathlib import Path

import typer
import vaex as vx

from utils import with_res_logger

app = typer.Typer()


def load(year):
    return vx.open(Path(os.environ["DATA"]) / f"{year}.csv")


@app.command()
@with_res_logger
def top_flop(year: str):
    if (
        (sys.version_info.minor > 10)
        and (sys.version_info.minor < 7)
        and (sys.version_info.major == 3)
    ):
        raise Exception("Only python versions >=3.7,<3.11 are supported.")
    df = (
        load(year)
        .groupby("code_postal")
        .agg({"id_mutation": vx.agg.nunique("id_mutation")})
        .sort("id_mutation", ascending=False)
    )
    print(df.head(10))
    print(df.tail(10))
