resource "aws_lambda_function" "ingestion_lambda" {
  function_name = "ingestion_lambda"
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "ingestion_lambda/function.zip"
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  role = aws_iam_role.lambda_role.arn
  handler = "ingestion_lambda.lambda_handler"
  runtime = "python3.12"
  timeout = 10
  depends_on = [ aws_s3_object.lambda_code ]
}

data "archive_file" "extract_lambda" {
  type = "zip"
  source_dir = "${path.module}/../src/"
  output_path = "${path.module}/../packages/ingestion_lambda/function.zip"
}

resource "aws_lambda_layer_version" "psycopg2_layer" {
  layer_name          = "psycopg2_layer"
  compatible_runtimes = [var.python_runtime]
  filename = "${path.module}/../packages/layer_content.zip"


}