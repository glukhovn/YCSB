#! /bin/bash

DATE=$(date +"%F_%T")
echo $DATE
./bin/ycsb $1 mongodb -s -P workloads/workload$2 -P mongo.dat -threads $3 > mongo_${1}_workload${2}_results_${DATE}
