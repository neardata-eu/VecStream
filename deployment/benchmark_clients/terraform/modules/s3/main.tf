resource "aws_s3_bucket" "this" {
  bucket        = var.bucket_name
  force_destroy = true

  tags = var.tags
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_availability_zone" "express" {
  name = var.availability_zone
}

resource "aws_s3_directory_bucket" "express" {
  bucket = "${var.express_bucket_name}--${var.availability_zone_abbreviation}--x-s3"

  location {
    name = var.availability_zone_abbreviation
  }

  tags = var.tags
}