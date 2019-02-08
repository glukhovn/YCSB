#!/bin/sh

dir=${1:-.}
count=${2:-10}
out="$dir/customers.json"

fakeit -c "$count" directory "$dir" customers.yml

echo "Copying documents into single JSON $out ..."

echo -n > "$out"

for f in "$dir/customer:::"*".json"
do
	# replace line feed with spaces
	sed -i ':a;N;$!ba;s/\n/ /g' "$f"
	echo >> "$f"
	# append to output file
	cat "$f" >> "$out"
done
