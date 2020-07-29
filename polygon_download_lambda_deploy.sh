#!/bin/bash
set -x

# Config.
third_party_packages="pandas polygon-api-client pytz pyyaml"
lambda_source_files="polygon_download.py polygon_download_secrets.yaml"
lambda_function_name="polygon_download"
lambda_test_event="polygon_download_test_event.json"

# Create third party packages zip.
python3 -m venv v-env
source v-env/bin/activate
pip install wheel  # Needed for pyyaml install to succeed.
pip install $third_party_packages
deactivate
cd v-env/lib/python3.8/site-packages
zip -r9 ../../../../lambda_third_party.zip .
cd ../../../..
rm -r v-env

# Create lambda package zip.
cp lambda_third_party.zip lambda_package.zip
zip -g lambda_package.zip $lambda_source_files

# Update existing lambda function.
aws lambda update-function-code --function-name $lambda_function_name \
--zip-file fileb://lambda_package.zip

# Test lambda function.
aws lambda invoke --function-name $lambda_function_name \
--invocation-type Event --payload fileb://$lambda_test_event \
lambda_test_output.txt
