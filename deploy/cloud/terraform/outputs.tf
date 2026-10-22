output "servers" { value = aws_instance.server[*].private_ip }
output "client" { value = aws_instance.client.public_ip }
output "run" {
  value = "HOSTS=\"${join(" ", aws_instance.server[*].private_ip)}\" CLIENT=${aws_instance.client.private_ip} SSH_KEY=<key.pem> deploy/cloud/n1.sh all"
}
