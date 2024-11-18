# test lambda has iam role which accesses ingestion s3 as a resource
# ingestion-bucket-20241111133940921900000001
variables {
    bucket_name = "ingestion-bucket-20241111133940921900000001"
}

run ingestion_s3 {
  command = plan
  

assert { 
  condition = contains(data.aws_iam_policy_document.s3_document.statement[0].resources, "arn:aws:s3:::ingestion-bucket-20241111133940921900000001/*")
  error_message = "extraction lambda does not have the correct permission to access the ingestion bucket"
}
}

# further tests to include:
# test lambda has the correct specific putobject action for ingestion s3 resource 
# format and validate ran on terraform