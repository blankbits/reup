#!/bin/bash
set -x

# Config.
third_party_packages="numpy pandas pyyaml"
lambda_source_files="join_columns.py ../../common/reup_utils.py"
lambda_function_name="join_columns"
lambda_test_event="lambda_event.json"

# Include common script.
. ../../common/lambda_deploy.sh
