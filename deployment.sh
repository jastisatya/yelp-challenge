#!/bin/sh
set -e

cd docker/hcluster
docker build -t hcluster .
docker run -itd --name hcluster -h hcluster hcluster bash
hcluster_ip=$(docker inspect --format '{{ .Name }}: {{.NetworkSettings.IPAddress }}' $(docker ps -aq) | grep hcluster | awk '{print $2}')
cd ../hclient
docker build -t hclient .
#sed -i -e 's/{ip}/$cluster_ip/g' hosts
docker run -it --add-host="hcluster:$hcluster_ip"  -h hclient hclient bash
