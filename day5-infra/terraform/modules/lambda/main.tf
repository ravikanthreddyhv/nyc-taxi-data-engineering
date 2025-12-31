resource "aws_iam_role" "lambda_role" {
  name = "${var.name}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "basic" {
  role      = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "this" {
  function_name = var.name
  role          = aws_iam_role.lambda_role.arn
  handler       = var.handler
  runtime       = var.runtime
  filename      = var.zip_path

  environment {
    variables = var.env_vars
  }
}

output "lambda_name" { value = aws_lambda_function.this.function_name }
output "lambda_arn"  { value = aws_lambda_function.this.arn }
