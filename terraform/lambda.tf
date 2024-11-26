# Extract lambda

resource "aws_lambda_function" "extract_lambda" {
  function_name    = "extract_lambda"
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.extract_lambda}/function.zip"
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  handler          = "extract.lambda_handler"
  runtime          = "python3.11"
  timeout          = 60
  depends_on       = [aws_s3_object.lambda_code, aws_s3_object.layer_code]
  layers           = [aws_lambda_layer_version.psycopg2_pandas_layer.arn]
  memory_size      = 256
}

data "archive_file" "extract_lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../src/extraction/"
  output_path = "${path.module}/../packages/ingestion_lambda/function.zip"
}

data "archive_file" "layer" {
  type        = "zip"
  source_dir  = "${path.module}/layer_files/"
  output_path = "${path.module}/../packages/layer_content.zip"
}

resource "aws_lambda_layer_version" "psycopg2_pandas_layer" {
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  # s3_bucket = "code-bucket20241112164137527200000004"
  s3_key              = "${var.extract_lambda}/layer_content.zip"
  layer_name          = "psycopg2_pandas_layer"
  compatible_runtimes = [var.python_runtime]
  source_code_hash    = data.archive_file.layer.output_base64sha256
  # filename = "${path.module}/../packages/layer_content.zip"
}

# Transform lambda

resource "aws_lambda_function" "transform_lambda" {
  function_name    = "transform_lambda"
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.transform_lambda}/function.zip"
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  role             = aws_iam_role.transform_lambda_role.arn
  handler          = "process_data.lambda_handler"
  runtime          = "python3.11"
  timeout          = 120
  depends_on       = [aws_s3_object.transform_lambda_code, aws_s3_object.layer_code]
  # layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:18"]
  # layers           = [aws_lambda_layer_version.psycopg2_pandas_layer.arn] #, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:18"]
  layers      = [aws_lambda_layer_version.psycopg2_pandas_layer.arn, "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-pandas:15"]
  memory_size = 512
}

data "archive_file" "transform_lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../src/process_data/"
  output_path = "${path.module}/../packages/transform_lambda/function.zip"
}

# Load lambda

resource "aws_lambda_function" "load_lambda" {
  function_name    = "load_lambda"
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.load_lambda}/function.zip"
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  role             = aws_iam_role.load_lambda_role.arn
  handler          = "load_data_handler.lambda_handler"
  runtime          = "python3.11"
  timeout          = 120
  depends_on       = [aws_s3_object.load_lambda_code, aws_s3_object.layer_code]
  #depends_on       = [aws_s3_object.layer_code]

  # layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:18"]
  # layers           = [aws_lambda_layer_version.psycopg2_pandas_layer.arn] #, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:18"]
  layers      = [aws_lambda_layer_version.psycopg2_pandas_layer.arn]     
  #layers      = [aws_lambda_layer_version.psycopg2_pandas_layer.arn, "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-pandas:15"]
  memory_size = 512
}

data "archive_file" "load_lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../src/load/"
  output_path = "${path.module}/../packages/load_lambda/function.zip"
}


