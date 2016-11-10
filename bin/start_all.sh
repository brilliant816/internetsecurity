#!/bin/bash

date_dir=$1
base_path=$(cd `dirname $0`; pwd)

printf "start r predict data..."
sh ${base_path}/start_r_predict_data.sh ${date_dir}
printf "finish r predict data."

printf "start r predict job..."
sh ${base_path}/start_r_predict_job.sh ${date_dir}
printf "finish r predict job."

printf "start r predict output..."
sh ${base_path}/start_r_predict_output.sh ${date_dir}
printf "finish r predict output."

printf "start attact detection model..."
sh ${base_path}/start_attact_detection_model.sh ${date_dir}
printf "finish attact detection model."

printf "start crawler detection model..."
sh ${base_path}/start_crawler_detection_model.sh ${date_dir}
printf "finish crawler detection model."

printf "start graph detection model..."
sh ${base_path}/start_graph_detection_model.sh ${date_dir}
printf "finish graph detection model."

printf "start scan detection model..."
sh ${base_path}/start_scan_detection_model.sh ${date_dir}
printf "finish scan detection model."

printf "all finished."