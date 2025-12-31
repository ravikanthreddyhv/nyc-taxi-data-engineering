provider "aws" { region = var.aws_region }

module "s3" {
  source      = "../../modules/s3"
  bucket_name = var.s3_bucket_name
}

module "lambda" {
  source   = "../../modules/lambda"
  name     = var.lambda_name
  handler  = "lambda_src/handler.lambda_handler"
  runtime  = "python3.12"
  zip_path = var.lambda_zip_path
  env_vars = {
    ENV    = "dev"
    BUCKET = module.s3.bucket_name
  }
}

module "rds" {
  source              = "../../modules/rds"
  db_identifier       = var.db_identifier
  db_name             = var.db_name
  master_username     = var.master_username
  master_password     = var.master_password
  vpc_id              = var.vpc_id
  subnet_ids          = var.subnet_ids
  allowed_cidrs       = var.allowed_cidrs
  publicly_accessible = true
}
