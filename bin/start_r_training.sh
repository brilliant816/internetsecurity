#!/bin/bash

date_dir=$1
base_path=$(cd `dirname $0`; pwd)

printf "start r training data..."
sh ${base_path}/start_r_training_data.sh ${date_dir}
printf "finish r training data."

printf "start r training job..."
sh ${base_path}/start_r_training_job.sh
printf "finish r training job."