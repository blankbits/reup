#!/bin/bash
set -x

# Config.
third_party_packages="pandas polygon-api-client pytz pyyaml"
lambda_source_files="polygon_download.py polygon_download_secrets.yaml"
lambda_function_name="polygon_download"
lambda_test_event="polygon_download_event.json"

# Add third party packages to zip.
python3 -m venv v-env
source v-env/bin/activate
pip install wheel  # Needed for pyyaml install to succeed.
pip install $third_party_packages
deactivate
cd v-env/lib/python3.8/site-packages
zip -r9 ../../../../lambda_package.zip .
cd ../../../..
rm -r v-env

# Add lambda source files to zip.
zip -g lambda_package.zip $lambda_source_files

# Deploy lambda package, then clean up zip.
aws lambda update-function-code --function-name $lambda_function_name \
--zip-file fileb://lambda_package.zip
rm lambda_package.zip

# Test lambda function, then clean up local output.
aws lambda invoke --function-name $lambda_function_name \
--invocation-type Event --payload fileb://$lambda_test_event \
lambda_test_output.txt
rm lambda_test_output.txt
