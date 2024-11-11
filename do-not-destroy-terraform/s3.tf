resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = "ingestion-bucket-"
}

resource "aws_s3_bucket" "extraction_times" {
  bucket_prefix = "extraction-times-"
}
