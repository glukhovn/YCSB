#! /bin/bash

DATE=$(date +"%F_%T")
echo $DATE
./bin/ycsb $1 pgjsonb -s -P workloads/workload$2 -cp jdbc-binding/lib/postgresql-9.4.1212.jar -P pg.dat -threads $3 > pg_${1}_workload${2}_results_${DATE}
