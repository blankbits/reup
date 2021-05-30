#!/bin/bash
set -x

# Config.
third_party_packages="numpy pandas pytz pyyaml"
lambda_source_files="features_second.py ../../common/reup_utils.py"
lambda_function_name="features_second"
lambda_test_event="lambda_event.json"

# Include common script.
. ../../common/lambda_deploy.sh
