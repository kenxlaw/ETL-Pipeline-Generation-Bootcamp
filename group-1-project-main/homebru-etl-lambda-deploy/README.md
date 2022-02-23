## ETL Lambda

### About

Multiple Lambda functions, one of which creates a bucket, another which creates a trigger and adds permissions and another for processing the data using our previous ETL work

### Usage

How to Deploy

- Create an S3 deployment bucket to use for deployment
- Edit `deploy.sh` to change the variables for deployment bucket and stack_name name 
- Authenticate to AWS with `aws sso login`
- Set AWS_PROFILE value with `export AWS_PROFILE=<profilename>`
- Run deployment with `./deploy.sh` 

### What this stack Creates

- Python lambda function `ETLLambdaFunction` 
- Basic lambda role `LambdaFunctionRole` which allows the lambda function to execute and write logs, as well as create a new bucket, read and move files through another bucket.
