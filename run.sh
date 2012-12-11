#!/bin/sh

#git stash
#git fetch origin
#git checkout temp
#git reset --hard origin/btrfs
#git stash pop
/home/samuelj/teuthology/virtualenv/bin/teuthology --archive /home/samuelj/log_output/`date +%y-%m-%d-%H:%M:%S` $1 $2 $3 $4 $5
