#! /bin/bash

set -x 
set -e

for a in $(cat buckets_to_count.txt) ; do
	cp ./config.txt.skel ./config.txt
    gsed -i 's/%%%BUCKET%%%/'$a'/g' ./config.txt
    ./go-aws.git 2> $a.err > $a.out
done

