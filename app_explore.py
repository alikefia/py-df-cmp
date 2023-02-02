import fnmatch

import typer

from app_polars import load
from utils import cols

app = typer.Typer()


def col_from_pattern(pattern):
    col = [c for c in cols.keys() if fnmatch.fnmatch(c, pattern)]
    if len(col) == 0:
        print("no col candidate")
        return
    if len(col) > 1:
        print("many col candidates:")
        for c in col:
            print(f"  - {c}")
        return
    return col[0]


@app.command()
def col(pattern: str = "*"):
    col = col_from_pattern(pattern)
    if col is not None:
        print(f"full col name is: {col}")


@app.command()
def unique(year: str, pattern: str):
    col = col_from_pattern(pattern)
    if col is not None:
        print(f"unique values for {col}:")
        for (val,) in load(year, None).select(col).unique().collect().rows():
            print(f"  - {val}")
