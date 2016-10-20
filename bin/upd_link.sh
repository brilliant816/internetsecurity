#!/bin/bash

if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` <internetsecurity_x.x.x>"
  exit 1
fi

source=$1

rm -rf internetsecurity
ln -s ${source} internetsecurity
chmod +x internetsecurity/bin/*