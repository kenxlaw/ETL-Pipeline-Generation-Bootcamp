set -eu

stack_name=homebru-cf
deployment_bucket=homebru-dev-bucket

if [ -d ".deployment" ]; then rm -rf .deployment; fi

pip install --target ./.deployment/dependencies -r requirements.txt

cd ./.deployment/dependencies
zip -r ../lambda-package.zip .

cd ../../src
zip -gr ../.deployment/lambda-package.zip app
cd ..

aws cloudformation package --template-file cloudformation.yml --s3-bucket ${deployment_bucket} --output-template-file .deployment/cloudformation-packaged.yml

aws cloudformation deploy --stack-name ${stack_name}-lambda --template-file .deployment/cloudformation-packaged.yml --region eu-west-1 --capabilities CAPABILITY_IAM --parameter-overrides NamePrefix=${stack_name}