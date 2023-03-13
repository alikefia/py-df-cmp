@default:
    just --list

# format and lint scripts
@fmt:
    #!/usr/bin/env bash
    fold -s -w79 README.md | sponge README.md
    just --unstable --fmt
    isort *.py
    black *.py
    flake8 *.py

# download data (for one year)
@data-dl year:
    #!/usr/bin/env bash
    mkdir -p "${DATA}"
    curl -sSLo "${DATA}/{{ year }}.csv.gz" "https://files.data.gouv.fr/geo-dvf/latest/csv/{{ year }}/full.csv.gz"
    gzip -d "${DATA}/{{ year }}.csv.gz"

# download additional data (population)
@data-pop:
    #!/usr/bin/env bash
    mkdir -p "${DATA}"
    curl -sSLo "${DATA}/ensemble.zip" "https://www.insee.fr/fr/statistiques/fichier/4265429/ensemble.zip"
    unzip -d "${DATA}/population" "${DATA}/ensemble.zip"
    rm -rf "${DATA}/ensemble.zip"

# download data (all available data)
@data-dl-all:
    #!/usr/bin/env bash
    for y in 2017 2018 2019 2020 2021 2022; do
        just data-dl "$y"
    done
    just data-pop

# print csv line (10 lines around the target line)
@data-line file line:
    #!/usr/bin/env bash
    src="${DATA}/{{ file }}.csv"
    out="/tmp/{{ file }}-{{ line }}.csv"
    head -1 "${src}" > "${out}"
    head -$(({{ line }}+5)) "${src}" | tail -10 >> "${out}"
    open "${out}"

# list data files
@data-ls:
    echo && echo "Files sizes in MB" && BLOCKSIZE=1M ls -1s "${DATA}"

# count data lines
@data-lc:
    echo && printf "Total # of lines:" && cat ${DATA}/*.csv | wc -l

# print data information
@data-info: data-ls data-lc
