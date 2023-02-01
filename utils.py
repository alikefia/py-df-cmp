import resource
from datetime import datetime
from functools import wraps

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


def with_res_logger(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        res = f(*args, **kwargs)
        t = datetime.now() - start
        me = resource.getrusage(resource.RUSAGE_SELF)
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        u_cpu = me.ru_utime + children.ru_utime
        s_cpu = me.ru_stime + children.ru_stime
        max_mem = me.ru_maxrss + children.ru_maxrss
        print(
            f"""
Resource consumption:
- exec time     : {t.total_seconds():.2f}s
- user cpu time : {u_cpu:.2f}s
- sys cpu time  : {s_cpu:.2f}s
- max mem usage : {max_mem / (1024*1024*1024):.2f}GB
            """
        )
        return res

    return wrapper
