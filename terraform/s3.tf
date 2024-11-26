resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "${var.extract_lambda}/function.zip"
  source = "${path.module}/../packages/ingestion_lambda/function.zip"
  etag   = filemd5("${path.module}/../packages/ingestion_lambda/function.zip")
}

# transform lambda

resource "aws_s3_object" "transform_lambda_code" {
  bucket     = aws_s3_bucket.code_bucket.id
  key        = "${var.transform_lambda}/function.zip"
  source     = "${path.module}/../packages/transform_lambda/function.zip"
  etag       = filemd5("${path.module}/../packages/transform_lambda/function.zip")
  depends_on = [data.archive_file.transform_lambda]
}

resource "aws_s3_object" "layer_code" {
  bucket     = aws_s3_bucket.code_bucket.id
  key        = "${var.extract_lambda}/layer_content.zip"
  source     = "${path.module}/../packages/layer_content.zip"
  etag       = filemd5("${path.module}/../packages/layer_content.zip")
  depends_on = [data.archive_file.layer]
}

# load lambda

resource "aws_s3_object" "load_lambda_code" {
  bucket     = aws_s3_bucket.code_bucket.id
  key        = "${var.load_lambda}/function.zip"
  source     = "${path.module}/../packages/load_lambda/function.zip"
  etag       = filemd5("${path.module}/../packages/load_lambda/function.zip")
  depends_on = [data.archive_file.load_lambda]
}
