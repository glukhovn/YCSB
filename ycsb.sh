#!/bin/bash

YCSB_HOME=$(dirname "$0")
YCSB_VERSION=0.13.0

YCSB_CLASSPATH=
for lib in ${YCSB_HOME}/jdbc-binding/lib/*.jar
do
	YCSB_CLASSPATH="${YCSB_CLASSPATH}:$lib"
done

warn()
{
	echo "$@" >&2
}

die()
{
	warn "$@"
	exit 1
}

ycsb()
{
	${YCSB_HOME}/bin/ycsb "$@" -cp "${YCSB_CLASSPATH}"
}

nth()
{
	shift $1
	echo "$1"
}

extract_stats()
{
	while read tag counter val
	do
		test "${tag:0:1}" = "[" &&	
		case "$tag $counter" in
			"[OVERALL], RunTime(ms),")				runtime=$val;;
			"[OVERALL], Throughput(ops/sec),")		throughput=$val;;
			
			"[READ], Operations,")					r_ops=$val;;
			"[READ], AverageLatency(us),")			r_lat_avg=$val;;
			"[READ], MinLatency(us),")				r_lat_min=$val;;
			"[READ], MaxLatency(us),")				r_lat_max=$val;;
			"[READ], 95thPercentileLatency(us),")	r_lat_95=$val;;
			"[READ], 99thPercentileLatency(us),")	r_lat_99=$val;;
			
			"[UPDATE], Operations,")				u_ops=$val;;
			"[UPDATE], AverageLatency(us),")		u_lat_avg=$val;;
			"[UPDATE], MinLatency(us),")			u_lat_min=$val;;
			"[UPDATE], MaxLatency(us),")			u_lat_max=$val;;
			"[UPDATE], 95thPercentileLatency(us),")	u_lat_95=$val;;
			"[UPDATE], 99thPercentileLatency(us),")	u_lat_99=$val;;

			"[INSERT], Operations,")				i_ops=$val;;
			"[INSERT], AverageLatency(us),")		i_lat_avg=$val;;
			"[INSERT], MinLatency(us),")			i_lat_min=$val;;
			"[INSERT], MaxLatency(us),")			i_lat_max=$val;;
			"[INSERT], 95thPercentileLatency(us),")	i_lat_95=$val;;
			"[INSERT], 99thPercentileLatency(us),")	i_lat_99=$val;;
		esac
	done < "$1"

	r_stats="$r_ops	$r_lat_avg	$r_lat_min	$r_lat_max	$r_lat_95	$r_lat_99"
	u_stats="$u_ops	$u_lat_avg	$u_lat_min	$u_lat_max	$u_lat_95	$u_lat_99"
	i_stats="$i_ops	$i_lat_avg	$i_lat_min	$i_lat_max	$i_lat_95	$i_lat_99"

	if test "$2" = "load"
	then
		echo "$runtime	$throughput	${i_stats}"
	else
		echo "$runtime	$throughput	${r_stats}	${u_stats}"
	fi

	false &&
	echo "
	runtime $runtime,
	throughput $throughput
		
	read ops $r_ops
	read lat avg $r_lat_avg
	read lat min $r_lat_min
	read lat max $r_lat_max
	read lat 95% $r_lat_95
	read lat 99% $r_lat_99
		
	update ops $u_ops
	update lat avg $u_lat_avg
	update lat min $u_lat_min
	update lat max $u_lat_max
	update lat 95% $u_lat_95
	update lat 99% $u_lat_99
	"
}

#write_stats $db $workload $cfg $clients $stats
write_stats()
{
	echo "$1	$2	$3	$4	$5"
}

extract_stats_dir()
{
	for f in $(ls  "$1"/run_*.out)
	do
		local stats=$(extract_stats "$f" "$2")
		IFS="_" read cmd db workload clients cfg <<< "$(basename "${f%.*}")"
		write_stats $db $workload $cfg $clients "$stats"
	done
}

ycsb_binding_jar()
{
	#${YCSB_HOME}/"$1"-binding/lib/"$1"-binding-${YCSB_VERSION}.jar
	echo "${YCSB_HOME}/$1/target/$1-binding-${YCSB_VERSION}-SNAPSHOT.jar"
}

create_table()
{
	case "$1" in
		jdbc-*|pgjsonb-*|mysqljson-*)
			local cp=${YCSB_CLASSPATH}
			for lib in ${YCSB_HOME}/${1%%-*}/target/dependency/*.jar
			do
				cp=$cp:$lib
			done
			cp=$(ycsb_binding_jar "${1%%-*}"):$(ycsb_binding_jar "jdbc-json"):${cp}
			shift
			echo java -cp "$cp" com.yahoo.ycsb.db.JdbcDBCreateTable "$@"
			java -cp "$cp" com.yahoo.ycsb.db.JdbcDBCreateTable "$@"
			;;

		mongodb-*)
			local mongo_url=$(grep 'mongodb.url=' ${YCSB_HOME}/config/"$1".dat)
			mongo_url=${mongo_url#mongodb.url=}
			echo "db.usertable.drop();" | mongo "${mongo_url}"
			;;
	esac
}

checkpoint()
{
	local cfg="${YCSB_HOME}/config/"$1".dat"

	case "$1" in
		jdbc-pg*|pgjsonb-*)
			local url=$(grep 'db.url=' "$cfg")
			local usr=$(grep 'db.user=' "$cfg")

			url=${url#*//}
			usr=${usr#db.user=}
			local db=${url#*/}
			db=${db%%\?*}
			url=${url%%/*}
			local host=${url%:*}
			local port=${url#*:}

			{
				#echo "CHECKPOINT;"
				echo "VACUUM usertable;"
			} | psql -h "$host" -p "$port" -d "$db" -U "$usr"
			;;

		jdbc-mysql*|mysqljson-*)
			;;

		mongodb-*)
			;;

		*)
			;;
	esac
}

run_workload()
{
	local date="$1"
	local cmd="$2"
	local db="$3"
	local workload="workload$4"
	local clients="$5"
	local name="${db}_${workload}_$clients"	
	local cfg=""
	local props="
			-P workloads/$workload
			-P config/$db.dat 
			-P config/default.dat
			"

	test "$YCSB_MAXRUNTIME" &&
	test "$cmd" = "run" &&
		props="$props -p maxexecutiontime=$YCSB_MAXRUNTIME"

	shift 5

	while test $# -gt 0
	do
		case "$1" in
		-p)
			shift
			props="$props -p $1"
			;;
		-*)
			die "unkown option '$1'"
			;;
		*)
			props="$props -P config/$1.dat"
			cfg="${cfg}_$1"
			;;
		esac
		shift
	done

	props="$props -p operationcount=$operations" #"$(expr $operations \* $clients)"

	local out_dir="results/${date}"
	local log_file="${out_dir}/${cmd}_${name}${cfg}.log"
	local out_file="${out_dir}/${cmd}_${name}${cfg}.out"

	mkdir -p "${out_dir}" || return
	test -L results/last && unlink results/last
	ln -s "$PWD/${out_dir}" results/last

	{
		test "$cmd" != "load" || create_table "$db" $props &&
		checkpoint "$db" &&
		ycsb $cmd "${db%-*}" $props -threads $clients -s -p exportfile="${out_file}"
	} > "${log_file}" 2>&1 ||
	{
		echo "FAILED, see ${log_file}"
		return 1
	}

	test "$cfg" || cfg="_default"
	
	local stats=$(extract_stats "${out_file}" "$cmd")
	throughput=$(nth 2 $stats)

	test "$cmd" = "load" && workload="load_$workload"

	write_stats "$db" "$workload" "${cfg:1}" "$clients" "$stats" >> "${out_dir}/stats"
}


dbs=${YCSB_DBS:=mongodb-dbtest pgjsonb-dbtest jdbc-dbtest-pg mysqljson-dbtest jdbc-dbtest-mysql}
workloads=${YCSB_WORKLOADS:=load_a run_a run_b run_c run_d load_e run_e run_f}
clients=${YCSB_CLIENTS:=1 2 4 6 8 12 16 24 32 48 64 96 128}
operations=${YCSB_OPS:=1000}

#dbs="jdbc-dbtest-pg"

generate_test_list()
{
	for db in $dbs
	do
		if true
		then
			for nclients in $clients
			do
				for workload in $workloads
				do
					local n=$nclients
					cfg="$YCSB_CFG"
					case "$workload" in
						load_*)	;; #n=32;;
					esac
					echo "${workload%%_*}  $db ${workload#*_} $n $cfg"
				done
			done
		else
			for workload in $workloads
			do
				local nclients_list=1

				case "$workload" in
				run_*)	nclients_list=$clients;;
				load_*)	nclients_list=$clients;; #32;;
				esac

				for nclients in ${nclients_list}
				do
					echo "${workload%%_*}  $db ${workload#*_} $nclients"
				done
			done
		fi
	done
}

run_tests()
{
	local tests=$(generate_test_list)
	local ntests=$(wc -l <<< "$tests")
	local date=$(date +"%F_%H%M%S")
	local i=0

	mkdir -p "results/${date}" || return

	echo "db	workload	config	clients	time	throughput\
	ops1	lat1avg	lat1min	lat1max	lat1p95	lat1p99\
	ops2	lat2avg	lat2min	lat2max	lat2p95	lat2p99" >> "results/${date}/stats" || return

	while read cmd db workload nclients cfg
	do
		i=$(expr $i + 1)
		printf "[%s] (%3d/$ntests) %s workload %s %-6s %3d: " \
				"$(date +"%F_%T")" "$i" "$db" "$workload" "$cmd" "$nclients"
		run_workload "$date" $cmd $db $workload $nclients $cfg "$@" && 
			{ LC_NUMERIC="C" printf "ok, %7.0f op/s\n" "$throughput" ; }
	done <<< "$tests"
}

ycsb_options=

parse_args()
{
	while test $# -gt 0
	do
		case "$1" in
		-t) shift; append_option maxexecutiontime="$1";;
		-m) shift; append_option measurementtype="$1";;
		-*)  die "unknown option '$1'";;
		esac
		shift
	done
}

trap exit SIGINT

#run_workload "$@"
#extract_stats "$@"
#parse_args "$@"
run_tests "$@"
#extract_stats_dir "$1"

