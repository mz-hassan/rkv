# L3: 3 server VMs + 1 client VM for the N1/multi-AZ runs. `terraform init && terraform apply -var key_name=...`
terraform {
  required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }
}
provider "aws" { region = var.region }

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

data "aws_availability_zones" "az" { state = "available" }

resource "aws_security_group" "rkv" {
  name_prefix = "rkv-"
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "server" {
  count                  = 3
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  availability_zone      = var.multi_az ? data.aws_availability_zones.az.names[count.index % length(data.aws_availability_zones.az.names)] : data.aws_availability_zones.az.names[0]
  vpc_security_group_ids = [aws_security_group.rkv.id]
  root_block_device {
    volume_type = "gp3"
    volume_size = 50
  }
  tags = { Name = "rkv-server-${count.index + 1}" }
}

resource "aws_instance" "client" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  availability_zone      = data.aws_availability_zones.az.names[0]
  vpc_security_group_ids = [aws_security_group.rkv.id]
  tags                   = { Name = "rkv-client" }
}
