# BAMCIS AWS Athena User Metrics

This serverless function generates Athena tables for CloudTrail logs and Athena Query Executions. It polls the Athena API to generate data and save it to S3 partitioned by the billing period. The included query joins the query execution data with CloudTrail logs to append the IAM principal that ran each query. This enables cost attribution to IAM principals as well as investigation into other metrics such as long running queries in addition to inspection of the particular queries users ran.

## Table of Contents
- [Usage](#usage)
- [Revision History](#revision-history)

## Usage

The CloudFormation template deploys 2 functions. The first function runs 5 minutes past each hour. It queries the Athena API to list query execution Ids. It stops when it reaches the first Id that was returned from the previous run. This id is stored in a marker.txt file in S3. Thus, each function invocation does an incremental pull of only newly run query data. The function checks each query execution it evaluates has been completed or cancelled before writing it to S3. Queries that have not completed, been cancelled, or failed have their Ids stored in an S3 file. A second Lambda function runs every 30 minutes past each hour and retrieves the query execution ids that need to be retried. These Ids are not cleared from this queue until they have been completed and the final data written to S3.

The CloudFormation template also deploys the required Glue and Athena resources to be able to easily run the query to join the CloudTrail IAM principal information with each query execution.

## Revision History

### 1.0.0
Initial release of the application.