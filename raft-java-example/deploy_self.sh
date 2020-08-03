#!/usr/bin/env bash

EXAMPLE_TAR=raft-java-example-1.9.0-deploy.tar.gz
ROOT_DIR=./env
mkdir -p ${ROOT_DIR}
cd ${ROOT_DIR}
pwd

mkdir example1
cd example1
pwd
cp -f ../../${EXAMPLE_TAR} .
tar -zxvf ${EXAMPLE_TAR}
chmod +x ./bin/*.sh
nohup sh bin/run_server.sh ./data "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8051:1" &
cd -

mkdir example2
cd example2
cp -f ../../${EXAMPLE_TAR} .
tar -zxvf ${EXAMPLE_TAR}
chmod +x ./bin/*.sh
nohup sh bin/run_server.sh ./data "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8052:2" &
cd -

mkdir example3
cd example3
cp -f ../../${EXAMPLE_TAR} .
tar -zxvf ${EXAMPLE_TAR}
chmod +x ./bin/*.sh
nohup sh bin/run_server.sh ./data "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8053:3" &
cd -

mkdir client
cd client
cp -f ../../${EXAMPLE_TAR} .
tar -zxvf ${EXAMPLE_TAR}
chmod +x ./bin/*.sh
cd -
