#!/bin/bash

base_path=$(cd `dirname $0`; pwd)

spark-submit \
--master "yarn" \
--deploy-mode "cluster" \
--class "me.bayee.internetsecurity.model.ScanDetectionModel" \
--name "ScanDetectionModel" \
${base_path}/../lib/internetsecurity-1.0-SNAPSHOT-jar-with-dependencies.jar