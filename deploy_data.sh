#!/bin/bash
set -ex

mkdir -p /mnt/cephfs/dataset
wget https://skyhook-ucsc.s3.us-west-1.amazonaws.com/64MB.uncompressed.parquet
for i in {1..460}; do
  cp 64MB.uncompressed.parquet /mnt/cephfs/dataset/64MB.uncompressed.parquet.$i
done
