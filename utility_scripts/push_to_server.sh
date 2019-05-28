#!/bin/bash

. ../bashvars

if [ $# -ne 1 ]
then
echo "Specify a number from 0-4 for which host to connect"
exit
fi

h=${hosts[$1]}
rsync -vP "build/libs/${jarfile}" ubuntu@$h:~/
scp hanode2.config ubuntu@$h:~/
scp hanode3.config ubuntu@$h:~/
scp -r META-INF/ ubuntu@$h:~/
