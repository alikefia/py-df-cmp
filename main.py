#!/usr/bin/env python

import logging
import os
import time

log_level = os.environ.get("LOG_LEVEL", None)
if log_level is not None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

import typer  # noqa: E402

from app_dask import app as dask_app  # noqa: E402
from app_duckdb import app as duckdb_app  # noqa: E402
from app_explore import app as explore_app  # noqa: E402
from app_pandas import app as pandas_app  # noqa: E402
from app_polars import app as polars_app  # noqa: E402
from utils import with_res_logger  # noqa: E402

app = typer.Typer()


@app.callback()
def main(log_level: str = None):
    if log_level is not None:
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )


@app.command()
@with_res_logger
def wait(secs: int):
    time.sleep(secs)


app.add_typer(explore_app, name="explore")
app.add_typer(pandas_app, name="pandas")
app.add_typer(dask_app, name="dask")
app.add_typer(polars_app, name="polars")
app.add_typer(duckdb_app, name="duckdb")


if __name__ == "__main__":
    app()
