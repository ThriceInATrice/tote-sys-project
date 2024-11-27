######################
# Totesys ETL Project 
######################

## Summary

This project is an Extract, Transform, and Load pipeline which extracts data from a pre-created Totesys database, processes it according to a pre-defined schema, and finally loads it into a data warehouse. Terraform is used as our Infrastructure as Code tool to create resources quickly and efficiently. The full ERD for the OLTP totesys database is detailed [here](https://dbdiagram.io/d/6332fecf7b3d2034ffcaaa92). The overall structure of the resulting data warehouse is shown [here](https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca). A diagram which details the cloud infrastructure of this project can be seen [here][img](./cloud_infrastructure.png).

## Pre-Requisites and Set-Up 

1. AWS ACCOUNT: An AWS account is required to host the ETL pipeline. In order for other members to access the same project within your AWS account, you will need to allow each person access to your AWS account by creating an IAM user for each and each member must export AWS_ACCESS_KEY_ID=[Your AWS ACCESS KEY ID HERE] and export AWS_SECRET_ACCESS_KEY=[Your AWS SECRET ACCESS KEY HERE] into their terminal before running any Terraform commands. 

2. AWS console set up in AWS account: 
      + Create a Bucket which must have a unique name to store the terraform state in (e.g. ‘state-bucket-[the current month]-[the current date]’). 
      + Store Totesys database credentials in the Secrets Manager; enter your secret name and your key-value pairs to access your database.

3. do-not-destroy-terraform directory: Within this directory, initialise, plan and run terraform for the creation of five buckets. Once created, this terraform should not be 
  destroyed: 
      + Insert the name of your console created state bucket within this block of code in provider.tf:
     --------------------------------------------------------------------
      backend "s3" {
      bucket         = ["YOUR STATE BUCKET NAME HERE"]
      key            = "do-not-destroy/terraform.tfstate"
      region         = "eu-west-2"
      } 
    ----------------------------------------------------------------------
      + The ‘ingestion-bucket-' and ‘extraction-times-’ buckets will be used within the Extraction phase. 
      + The ‘transformed-data-’ and 'transformation-times-’ buckets will be used within the Transformation phase. 
      + The 'load-times-' bucket will be used in the Load phase.
  
4. main terraform directory: 
      + Within the main terraform directory insert the name of your console created state bucket within this block of code in provider.tf:
     --------------------------------------------------------------------
    backend "s3" {
    bucket         = ["YOUR STATE BUCKET NAME HERE"]
    dynamodb_table = "terraform-state-lock-dynamo"
    key            = "data-engineering-project-state/terraform.tfstate"
    region         = "eu-west-2"
    }
    ----------------------------------------------------------------------

## Running Terraform Apply in Main Terraform Directory

1. Ensure the following file paths exist to empty zip files from the root of the directory:
tote-sys-project/packages/ingestion_lambda/function.zip
tote-sys-project/packages/transform_lambda/function.zip
tote-sys-project/packages/load_lambda/function.zip
tote-sys-project/packages/layer_content.zip
2. Run terraform init, plan and apply to create:
+ Three Lambdas (Extract, Transform, Load)
+ One bucket ("code-bucket-")
+ One psychopg2 layer which is applied to all three Lambdas

## Create Step Functions Machine in AWS Console

Once all resources have been created in the do-not-destroy-terraform and the main terraform directory, arrange the following resources in the structure which can be seen [here][img](./state_machine.png). You will create an additional Glue resource in your AWS console which will extract all new data from the 'transformed_data' bucket, convert the data files into a parquet format, and load it under a 'parquets/' key back into the 'transformed_data' bucket. You can see the script for this [here][img](./glue_script.png) but this will be generated automatically by the correct s3 card arrangement in the glue visual editor.

## Monitoring

All three lambdas have permissions to create logs in Cloudwatch, with all necessary permissions attached via Terraform. Email alerts are sent with each deployment, with an additional alarm triggered by failing runs. A change to the source database will be reflected in the data warehouse within 30 minutes at most.

## Scheduler 
The Extraction Lambda is scheduled to run every half an hour, with all other processes triggered to run once the previous process has completed. Refer to the state machine for the order of execution [img](./state_machine.png). 

## Pre-Defined Schema

The following are the pre-defined schemas which determined how we remodelled our data for our 'transformed_data' bucket during the Transformation phase of the pipeline. You can find the ERDs for these star schemas:
 - ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
 - ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
 - ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

## History

Our warehouse contains a full history of all updates to the facts tables, making it possible to query either the current state of a row, or the full history
of how it has evolved.










