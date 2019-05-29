#!/bin/bash

. ../bashvars
if [ $# -ne 1 ]
then
echo "Specify a number from 0-4 for which host to connect"
exit
fi

ssh ubuntu@${hosts[$1]} pkill -e java
ssh ubuntu@${hosts[$1]} java -jar "${jarfile}" -h chronos.dmi.unibas.ch -s 5cb445103522e | tee -a graphdb.log
