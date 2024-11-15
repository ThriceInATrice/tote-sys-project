resource "aws_lambda_function" "extract_lambda" {
  function_name = "extract_lambda"
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "${var.extract_lambda}/function.zip"
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  role = aws_iam_role.lambda_role.arn
  handler = "extract.lambda_handler"
  runtime = "python3.11"
  timeout = 10
  depends_on = [ aws_s3_object.lambda_code, aws_s3_object.layer_code ]
  layers = [ aws_lambda_layer_version.psycopg2_layer.arn ]
}

data "archive_file" "extract_lambda" {
  type = "zip"
  source_dir = "${path.module}/../src/extraction/"
  output_path = "${path.module}/../packages/ingestion_lambda/function.zip"
}

data "archive_file" "layer" {
    type = "zip"
    source_dir = "${path.module}/layer_files/"
    output_path = "${path.module}/../packages/layer_content.zip"
}

resource "aws_lambda_layer_version" "psycopg2_layer" {
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  # s3_bucket = "code-bucket20241112164137527200000004"
  s3_key = "${var.extract_lambda}/layer_content.zip"
  layer_name          = "psycopg2_layer"
  compatible_runtimes = [var.python_runtime]
  # filename = "${path.module}/../packages/layer_content.zip"
}

