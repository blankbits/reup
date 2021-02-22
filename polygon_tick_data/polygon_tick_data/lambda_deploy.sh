#!/bin/bash
set -x

# Config.
third_party_packages="polygon-api-client pyyaml"
lambda_source_files="polygon_tick_data.py polygon_tick_data_secrets.yaml"
lambda_function_name="polygon_tick_data"
lambda_test_event="lambda_event.json"

# Include common script.
. ../../common/lambda_deploy.sh
