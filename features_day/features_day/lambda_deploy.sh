#!/bin/bash
set -x

# Config.
third_party_packages="numpy pandas"
lambda_source_files="features_day.py"
lambda_function_name="features_day"
lambda_test_event="lambda_event.json"

# Include common script.
. ../../common/lambda_deploy.sh
