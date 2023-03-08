#!/bin/bash

# sleep between queries (in seconds)
SLEEPTIME=3

# upload CSV into temp table
function upload_csv() {
    local TABLENAME=$1
    local EPOCH=$2
    local TARGETDATASET=$3
    local INPUTFILE="${TABLENAME}-${EPOCH}.csv"
    if [ ! -e "$INPUTFILE" ]; then echo "input file $INPUTFILE missing!"; (exit 1); fi
    local SCHEMAFILE="./schema/${TABLENAME}.json"
    if [ ! -e "$SCHEMAFILE" ]; then echo "schema file $SCHEMAFILE missing!"; (exit 1); fi
    if [ -z "$BQ" ]; then echo "\$BQ undefined!"; (exit 1); fi
    if [ ! -e $(pwd)/dot.bigqueryrc ]; then echo "file dot.bigqueryrc missing!"; (exit 1); fi

    # prepare data: single line rows
    $SED -e ':a /, $/ { N; s/\n//g; ba; }' ${INPUTFILE} > upload.all

    # split file into chunks for uploading
    NLINES=$(wc -l upload.all | $SED -ne 's/^ *\([0-9]\+\) .*/\1/p;')
    if [ -z "$NLINES" -o $NLINES -le 0 ]; then echo "no data rows!"; (exit 1); fi
    if [ $NLINES -gt 80000 ]; then
        rm -f chunk-*
        split -l 50000 upload.all chunk-
        for Up in chunk-*; do
            ${BQ} load --bigqueryrc=$(pwd)/dot.bigqueryrc --ignore_unknown_values=false --replace=true t2 "${Up}" "${SCHEMAFILE}" 2> logs/${OUTNAME}-upload.err > logs/${OUTNAME}-upload.out
            copy_data ${TABLENAME} ${TARGETDATASET} 2> logs/${TABLENAME}-copy.err > logs/${TABLENAME}-copy.out
            rm -v ${Up}
        done
    else
        ${BQ} load --bigqueryrc=$(pwd)/dot.bigqueryrc --ignore_unknown_values=false --replace=true t2 "upload.all" "${SCHEMAFILE}" 2> logs/${OUTNAME}-upload.err > logs/${OUTNAME}-upload.out
        copy_data ${TABLENAME} ${TARGETDATASET} 2> logs/${TABLENAME}-copy.err > logs/${TABLENAME}-copy.out
    fi

    rm -v "${INPUTFILE}"
    rm -v upload.all
}

# copy data from temp table to real table (in target dataset)
function copy_data() {
    local TABLENAME=$1
    local TARGETDATASET=$2
    local Q="INSERT INTO ${TARGETDATASET}.${TABLENAME} \
             SELECT * FROM iog-data-analytics.db_sync.t2;"
    if [ -z "$BQ" ]; then echo "\$BQ undefined!"; (exit 1); fi

    ${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc --append_table=true --nouse_legacy_sql "${Q}"
}

# process the query for each epoch until zero records are returned
function process_epoch_f() {
    local fQUERY=$1
    local OUTNAME=$2
    local TARGETDATASET=$3
    local EPOCH=$4
    local NREAD=1
    if [ -z "$PSQL" ]; then echo "\$PSQL undefined!"; (exit 1); fi
    if [ -z "$SED" ]; then echo "\$SED undefined!"; (exit 1); fi
    while [ $NREAD -gt 0 ]; do
      NREAD=$(pg_query_to_csv "$($fQUERY $EPOCH)" "${OUTNAME}-${EPOCH}")
      echo "epoch ${EPOCH} returned: ${NREAD}"
      if [ $NREAD -gt 0 ]; then
        upload_csv ${OUTNAME} ${EPOCH} ${TARGETDATASET}
        sleep $SLEEPTIME
      fi
      EPOCH=$((EPOCH + 1))
    done
}

# run a query against PostgreSQL and output to CSV file
# returns: number of copied rows
function pg_query_to_csv() {
    local QUERY=$1
    local OUTNAME=$2
    if [ -z "$PSQL" ]; then echo "\$PSQL undefined!"; (exit 1); fi
    if [ -z "$SED" ]; then echo "\$SED undefined!"; (exit 1); fi
    res=$(${PSQL} -c "\\copy ( $QUERY ) to './${OUTNAME}.csv0' csv;" 2> logs/${OUTNAME}-pg.err)
    # keep rows on a single line
    cat ./${OUTNAME}.csv0 | $SED -e ':a /, $/ { N; s/\n//g; ba; }' > ./${OUTNAME}.csv
    rm ./${OUTNAME}.csv0
    # if transformation procedure is defined, run it
    case $(type -t transform_csv) in
      function) echo "transforming CSV ${OUTNAME}.csv with function 'transform_csv'" >>/dev/stderr
	        transform_csv ${OUTNAME}.csv ;;
	     *) echo "no function 'transform_csv' defined" >>/dev/stderr ;;
    esac

    echo ${res} | ${SED} -ne "s/.*COPY \\([0-9]\\+\\)/\\1/p"
}

# load a CSV file into a temporary table in BQ
# (the table is remade with the indicated schema)
function bq_load_csv() {
    local CSVNAME=$1
    local TBLNAME=$2
    local SCHEMA=$3
    local DATASET=$4
    local INPUTFILE="${CSVNAME}.csv"
    if [ ! -e "$INPUTFILE" ]; then echo "input file $INPUTFILE missing!"; (exit 1); fi
    local SCHEMAFILE="./schema/${SCHEMA}.json"
    if [ ! -e "$SCHEMAFILE" ]; then echo "schema file $SCHEMAFILE missing!"; (exit 1); fi
    if [ -z "$BQ" ]; then echo "\$BQ undefined!"; (exit 1); fi
    if [ ! -e $(pwd)/dot.bigqueryrc ]; then echo "file dot.bigqueryrc missing!"; (exit 1); fi
    if [ -z "$DATASET" ]; then echo "dataset id not passed in!"; (exit 1); fi
    ${BQ} load --bigqueryrc=$(pwd)/dot.bigqueryrc --dataset_id=${DATASET} --ignore_unknown_values=false --replace=true ${TBLNAME} "${INPUTFILE}" "${SCHEMAFILE}" 2> logs/${TBLNAME}-upload.err > logs/${TBLNAME}-upload.out
}
