#!/bin/bash

# need to run this command below
# chmod +x startup.sh

# Start the coordinator
./coordinator -p 9000 &

sleep 2

echo "STARTING TSD PROCESSES"
# Start the tsd processes
./tsd -c 1 -s 1 -h localhost -k 9000 -p 10000 &
./tsd -c 2 -s 1 -h localhost -k 9000 -p 10001 &
./tsd -c 3 -s 1 -h localhost -k 9000 -p 10002 &

sleep 1

./tsd -c 1 -s 2 -h localhost -k 9000 -p 10003 &
./tsd -c 2 -s 2 -h localhost -k 9000 -p 10004 &
./tsd -c 3 -s 2 -h localhost -k 9000 -p 10005 &


sleep 1

echo "STARTING SYNCHRONIZER PROCESSES"

./synchronizer -h localhost -k 9000 -p 9001 -i 1 &
./synchronizer -h localhost -k 9000 -p 9002 -i 2 &
./synchronizer -h localhost -k 9000 -p 9003 -i 3 &

sleep 1 

./synchronizer -h localhost -k 9000 -p 9004 -i 4 &
./synchronizer -h localhost -k 9000 -p 9005 -i 5 &
./synchronizer -h localhost -k 9000 -p 9006 -i 6 &
