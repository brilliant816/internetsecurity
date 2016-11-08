#!/bin/bash

base_path=$(cd `dirname $0`; pwd)

java -cp ${base_path}/../lib/internetsecurity-1.0-SNAPSHOT-jar-with-dependencies.jar me.bayee.internetsecurity.r.RPredictJob