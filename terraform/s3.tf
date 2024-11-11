resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "${var.extract_lambda}/function.zip"
  source = "${path.module}/../packages/extract/function.zip"
  etag = filemd5("${path.module}/../packages/extract/function.zip")
}