# test code bucket is made with correct prefix

variables {
code_bucket_prefix = "code-bucket"
}

run buckets {
  command = plan

assert {
  condition = startswith(aws_s3_bucket.code_bucket.bucket, "code_bucket")
  error_message = "code bucket did not start with the prefix ${var.code_bucket_prefix}"
}
}

# change commands to apply for test to work 
# ingestion-bucket-20241111133940921900000001

# test lambda has access to code bucket as a resource 
# test lambda has correct actions (putobject) for code bucket resource