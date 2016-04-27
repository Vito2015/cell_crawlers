#!/usr/bin/env bash

today=`date +"%Y-%m-%d-%H-%M-%S"`

bash _run.sh 2>missingcell_position_error-${today}.txt 1>missingcell_position_results-${today}.json.txt
