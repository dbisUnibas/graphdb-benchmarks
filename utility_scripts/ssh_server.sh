#!/bin/bash

hosts=('10.34.58.92' '10.34.58.93' '10.34.58.94' '10.34.58.95' '10.34.58.96')
if [ $# -lt 1 ]
then
echo "Specify a number from 0-4 for which host to connect"
exit
fi
h=${hosts[$1]}
shift
ssh ubuntu@$h "$@"
