variable "aws_region"      { type = string }
variable "s3_bucket_name"  { type = string }
variable "lambda_name"     { type = string }
variable "lambda_zip_path" { type = string }

variable "db_identifier"   { type = string }
variable "db_name"         { type = string }
variable "master_username" { type = string }
variable "master_password" { type = string, sensitive = true }

variable "vpc_id"          { type = string }
variable "subnet_ids"      { type = list(string) }
variable "allowed_cidrs"   { type = list(string) }
