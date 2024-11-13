resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket"
}

resource "aws_s3_object" "lambda_code" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "${var.extract_lambda}/function.zip"
  source = "${path.module}/../packages/ingestion_lambda/function.zip"
  etag = filemd5("${path.module}/../packages/ingestion_lambda/function.zip")
}

resource "aws_s3_object" "layer_code" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "${var.extract_lambda}/layer_content.zip"
  source = "${path.module}/../packages/layer_content.zip"
  etag = filemd5("${path.module}/../packages/layer_content.zip")
}