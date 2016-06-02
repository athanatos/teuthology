#/bin/sh

./virtualenv/bin/teuthology-lock --list-targets --owner samuelj@rex004.front.sepia.ceph.com > /tmp/tmp
cat /tmp/tmp
sleep 5
./virtualenv/bin/teuthology-nuke --owner samuelj@rex004.front.sepia.ceph.com -t /tmp/tmp -u
