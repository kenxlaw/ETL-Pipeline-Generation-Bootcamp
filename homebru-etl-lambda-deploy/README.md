## IP API Lambda

### About

Lambda function which returns the current public IP address of the lambda function by making a request to a third-party IP on the public Internet

Documentation for the API is here [https://www.ipify.org/](https://www.ipify.org/)

### Usage

How to Deploy

- Create an S3 deployment bucket to use for deployment
- Edit `deploy.sh` to change the variables for deployment bucket and stack_name name 
- Authenticate to AWS with `aws sso login`
- Set AWS_PROFILE value with `export AWS_PROFILE=<profilename>`
- Run deployment with `./deploy.sh` 

### What this stack Creates

- Python lambda function `IpLambdaFunction` 
- Basic lambda role `IpLambdaFunctionRole` which allows the lambda function to execute and write logs