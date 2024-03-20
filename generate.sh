#!/bin/bash
mkdir -p ./data

for i in {1..10}
do
   dd if=/dev/urandom of=./data/random_data_${i}.bin bs=2M count=1
done

dd if=/dev/urandom of=./data/random_data_large.bin bs=20M count=1
dd if=/dev/urandom of=./data/random_data_small.bin bs=5M count=1
