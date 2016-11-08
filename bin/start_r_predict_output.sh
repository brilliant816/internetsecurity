#!/bin/bash

base_path=$(cd `dirname $0`; pwd)

spark-submit \
--master "yarn" \
--deploy-mode "cluster" \
--class "me.bayee.internetsecurity.r.RPredictOutput" \
--name "RModel" \
${base_path}/../lib/internetsecurity-1.0-SNAPSHOT-jar-with-dependencies.jar /