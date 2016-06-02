#!/bin/sh

#git stash
#git fetch origin
#git checkout temp
#git reset --hard origin/btrfs
#git stash pop
/home/samuelj/git-checkouts/teuthology/virtualenv/bin/teuthology --machine-type plana,burnupi,typica,mira --os-type ubuntu --lock --suite-path /home/samuelj/ceph-qa-suite2 --archive /home/samuelj/log_output3/`date +%y-%m-%d-%H:%M:%S` $1 $2 $3 $4 $5
