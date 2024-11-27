resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = "ingestion-bucket-"
}

resource "aws_s3_bucket" "extraction_times" {
  bucket_prefix = "extraction-times-"
}

resource "aws_s3_bucket" "transformed_data" {
  bucket_prefix = "transformed-data-"
}

resource "aws_s3_bucket" "data_transformation_times" {
  bucket_prefix = "transformation-times-"
}

resource "aws_s3_bucket" "loaded_extractions" {
  bucket_prefix = "loaded-extractions-"
}