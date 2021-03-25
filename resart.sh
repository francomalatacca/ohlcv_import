#!/bin/sh
echo stop all containers ...
sudo docker stop $(sudo docker ps -a -q)
echo remove all containers ...
sudo docker rm $(sudo docker ps -a -q)
echo done.
#echo creating the container
#sudo docker build -t ohlcv_import .
#echo running the container
#sudo docker run -it --rm ohlcv_import:latest
