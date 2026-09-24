output "public_ip" {
  description = "Public IP address of the instance"
  value       = aws_eip.this.public_ip
}

output "private_ip" {
  description = "Private IP address of the instance"
  value       = aws_instance.this.private_ip
}

output "instance_id" {
  description = "Instance ID"
  value       = aws_instance.this.id
}

output "security_group_id" {
  description = "Security group ID"
  value       = aws_security_group.this.id
}

output "iam_role_name" {
  description = "IAM role name (for attaching additional policies)"
  value       = aws_iam_role.this.name
}

output "iam_instance_profile_arn" {
  description = "IAM instance profile ARN"
  value       = aws_iam_instance_profile.this.arn
}