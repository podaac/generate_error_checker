# error_checker

The error_checker program is an AWS Lambda function that periodically checks for any combiner or processor execution errors.

It specifically looks for quarantined files and error logs. If found, the error_checker creates a text file to submit to the pending_jobs queue to restart the entire Generate workflow for the files that produced errors.

Top-level Generate repo: https://github.com/podaac/generate

## aws infrastructure

The error_checker program includes the following AWS services:
- Lambda function to execute code deployed via zip file.
- Permissions that allow an EventBridge Schedule to invoke the Lambda function.
- IAM role and policy for Lambda function execution.
- EventBridge schedule that executes the Lambda function every 55 minutes.

## terraform 

Deploys AWS infrastructure and stores state in an S3 backend.

To deploy:
1. Initialize terraform: 
    ```
    terraform init -backend-config="bucket=bucket-state" \
        -backend-config="key=component.tfstate" \
        -backend-config="region=aws-region" \
        -backend-config="profile=named-profile"
    ```
2. Plan terraform modifications: 
    ```
    ~/terraform plan -var="environment=venue" \
        -var="prefix=venue-prefix" \
        -var="profile=named-profile" \
        -out="tfplan"
    ```
3. Apply terraform modifications: `terraform apply tfplan`