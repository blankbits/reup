# #!/bin/bash
# set -x

# # Config.
# third_party_packages="package_1 package_2"
# lambda_source_files="file_1.py file_2.py"
# lambda_function_name="name"
# lambda_test_event="lambda_event.json"

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
zip -gj lambda_package.zip $lambda_source_files

# Deploy lambda package, then clean up zip.
aws lambda update-function-code --function-name $lambda_function_name \
--zip-file fileb://lambda_package.zip
rm lambda_package.zip

# Test lambda function, then clean up local output.
aws lambda invoke --function-name $lambda_function_name \
--invocation-type Event --payload fileb://$lambda_test_event \
lambda_test_output.txt
rm lambda_test_output.txt
