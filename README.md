# HomeBru

![Team][1]

[1]: https://i.redd.it/30ei98soetj61.jpg

Our *ETL* pipeline to visually represent sales data gathered from cafes around the UK. 

# Grafana link. 
```sh
http://34.245.201.99/ [Website No longer running after Program end]
```

## Features:
- [Grafana](https://grafana.com/) for Data Visualisations. 

- [Docker](https://www.docker.com/) for running a container within EC2.

- [GitHub Actions](https://github.com/features/actions) for automating the CI/CD deploy process online.

# AWS Services:

- [AWS Redshift](https://aws.amazon.com/redshift) for Data Warehousing.

- [AWS SQS](https://aws.amazon.com/sqs/) for Load Queue Messaging. 

- [AWS EC2](https://aws.amazon.com/ec2/features/) for running an instance to host Grafana site.

- [AWS SSM](https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-agent.html) for managing sensitive information.

- [AWS Lambda](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html) for running code to allow for the Extract, Transform and Load processes to function.

- [AWS Cloudformation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html) creates a 'stack' to set up our resources.

- [AWS Cloudwatch](https://aws.amazon.com/cloudwatch/features/) allows for monitoring performance and operational data in logs.

- [AWS IAM](https://aws.amazon.com/iam/features/?nc=sn&loc=2) attaches policy to grant access permissions.

# Grafana plugins for visualisation includes:
- [Redshift](https://grafana.com/grafana/plugins/grafana-redshift-datasource/?tab=installation) 
for pulling data from AWS
- [Cloudwatch](https://grafana.com/docs/grafana/latest/datasources/aws-cloudwatch/) for Lambda / EC2 Metrics
