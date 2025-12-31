resource "aws_db_subnet_group" "this" {
  name       = "${var.db_identifier}-subnet"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "db_sg" {
  name        = "${var.db_identifier}-sg"
  vpc_id      = var.vpc_id
  description = "Allow PostgreSQL access"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "this" {
  identifier          = var.db_identifier
  engine              = "postgres"
  engine_version      = var.engine_version
  instance_class      = var.instance_class
  allocated_storage   = var.allocated_storage
  db_name             = var.db_name
  username            = var.master_username
  password            = var.master_password
  port                = 5432

  publicly_accessible = var.publicly_accessible
  skip_final_snapshot = true

  vpc_security_group_ids = [aws_security_group.db_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.this.name
}

output "db_endpoint" { value = aws_db_instance.this.address }
