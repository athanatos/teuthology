#!/bin/bash

archive_dir=/home/samuelj/log_output/compound-`date +%y-%m-%d-%H:%M:%S`
echo "archive_dir is $archive_dir"
mkdir $archive_dir

for branch in `ls testbed/branches`
do
		for load in `ls testbed/loads`
		do
				echo "$branch-$load"
        ./clear.sh mymachines-perf -r;
				/home/samuelj/teuthology/virtualenv/bin/teuthology --archive "$archive_dir/$branch-$load" mymachines-perf testbed/base.yaml testbed/loads/$load testbed/branches/$branch;
	  done
done

