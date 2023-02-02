import csv
import os
import resource
from collections import namedtuple
from datetime import datetime
from functools import wraps
from pathlib import Path

cols = {
    "id_mutation": "str",
    "date_mutation": "date",
    "numero_disposition": "int",
    "nature_mutation": "cat",
    "valeur_fonciere": "float",
    "adresse_numero": "str",
    "adresse_suffixe": "str",
    "adresse_nom_voie": "str",
    "adresse_code_voie": "str",
    "code_postal": "int",
    "code_commune": "str",
    "nom_commune": "str",
    "code_departement": "cat",
    "ancien_code_commune": "int",
    "ancien_nom_commune": "str",
    "id_parcelle": "str",
    "ancien_id_parcelle": "str",
    "numero_volume": "str",
    "lot1_numero": "str",
    "lot1_surface_carrez": "float",
    "lot2_numero": "str",
    "lot2_surface_carrez": "float",
    "lot3_numero": "str",
    "lot3_surface_carrez": "float",
    "lot4_numero": "str",
    "lot4_surface_carrez": "float",
    "lot5_numero": "str",
    "lot5_surface_carrez": "float",
    "nombre_lots": "int",
    "code_type_local": "int",
    "type_local": "cat",
    "surface_reelle_bati": "float",
    "nombre_pieces_principales": "int",
    "code_nature_culture": "cat",
    "nature_culture": "cat",
    "code_nature_culture_speciale": "cat",
    "nature_culture_speciale": "cat",
    "surface_terrain": "float",
    "longitude": "float",
    "latitude": "float",
}


Experience = namedtuple(
    "Experience", ("mod", "fn", "ctx", "tm", "u_cpu", "s_cpu", "max_mem")
)


def print_stats(e: Experience):
    print(
        f"""
Resources usage:
- exec time     : {e.tm}s
- user cpu time : {e.u_cpu}s
- sys cpu time  : {e.s_cpu}s
- max mem usage : {e.max_mem}GB
        """
    )


def log_stats(e: Experience):
    new = False
    if not Path(os.environ["RES"]).exists():
        new = True
    with open(os.environ["RES"], "a") as fd:
        writer = csv.writer(fd)
        if new is True:
            writer.writerow(Experience._fields)
        writer.writerow((e.mod, e.fn, e.ctx, e.tm, e.u_cpu, e.s_cpu, e.max_mem))


def with_res_logger(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        res = f(*args, **kwargs)
        end = datetime.now()
        me = resource.getrusage(resource.RUSAGE_SELF)
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        e = Experience(
            tm=f"{(end - start).total_seconds():.2f}",
            mod=f.__module__,
            fn=f.__name__,
            ctx="|".join(
                [str(arg) for arg in args] + [str(kwarg) for kwarg in kwargs.values()]
            ),
            u_cpu=f"{me.ru_utime + children.ru_utime:.2f}",
            s_cpu=f"{me.ru_stime + children.ru_stime:.2f}",
            max_mem=f"{(me.ru_maxrss + children.ru_maxrss)/(1024*1024*1024):.2f}",
        )
        log_stats(e)
        print_stats(e)
        return res

    return wrapper
