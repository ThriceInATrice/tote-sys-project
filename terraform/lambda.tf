resource "aws_lambda_function" "ingestion_lambda" {
  function_name = "ingestion_lambda"
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "extract/function.zip"
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  role = aws_iam_role.lambda_role.arn
  handler = "extract.lambda_handler"
  runtime = "python3.12"
  timeout = 10
  depends_on = [ aws_s3_object.lambda_code ]
}

data "archive_file" "extract_lambda" {
  type = "zip"
  source_file = "${path.module}/../src/extract.py"
  output_path = "${path.module}/../packages/extract/function.zip"
}