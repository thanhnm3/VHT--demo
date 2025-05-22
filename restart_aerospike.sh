#!/bin/bash

set -e

sudo rm -f docker/aerospike_data/consumer_033.dat
sudo rm -f docker/aerospike_data/consumer_096.dat
sleep 2

sudo docker restart aerospike2