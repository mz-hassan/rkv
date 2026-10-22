variable "region" {
  type    = string
  default = "us-east-1"
}
variable "key_name" {
  type        = string
  description = "existing EC2 key pair name"
}
variable "instance_type" {
  type    = string
  default = "m6i.xlarge"
}
variable "ssh_cidr" {
  type        = string
  description = "CIDR allowed to ssh, e.g. your.ip/32"
}
variable "multi_az" {
  type    = bool
  default = true
}
