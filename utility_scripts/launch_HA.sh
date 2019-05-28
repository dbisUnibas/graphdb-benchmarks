#!/bin/bash

. ../bashvars
echo "Cleaning and starting HA on ${hosts[1]}"
ssh ubuntu@${hosts[1]} pkill java
ssh ubuntu@${hosts[1]} rm -rf n4j/
ssh ubuntu@${hosts[1]} "java -cp ${jarfile} eu.socialsensor.main.HANode hanode2.config | tee -a HANode.log" &
echo "Cleaning and starting HA on ${hosts[2]}"
ssh ubuntu@${hosts[2]} pkill java
ssh ubuntu@${hosts[2]} rm -rf n4j/
ssh ubuntu@${hosts[2]} "java -cp ${jarfile} eu.socialsensor.main.HANode hanode3.config | tee -a HANode.log" &

echo "Starting chronos on ${hosts[0]}"
ssh ubuntu@${hosts[0]} pkill java
ssh ubuntu@${hosts[0]} "java -jar ${jarfile} -h chronos.dmi.unibas.ch -s 5cb445103522e | tee -a graphdb.log"
