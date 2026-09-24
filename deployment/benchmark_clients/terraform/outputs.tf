output "client_public_ip" {
  description = "Public IP of the benchmark client EC2 instance"
  value       = try(module.ec2_client[0].public_ip, null)
}

output "client_instance_id" {
  description = "Instance ID of the benchmark client EC2 instance"
  value       = try(module.ec2_client[0].instance_id, null)
}

output "client_security_group_id" {
  description = "Security group ID of the benchmark client"
  value       = try(module.ec2_client[0].security_group_id, null)
}

output "s3_bucket_name" {
  description = "S3 bucket name for plain S3 benchmarks"
  value       = try(module.s3[0].bucket_name, null)
}

output "express_bucket_name" {
  description = "S3 Express One Zone bucket name"
  value       = try(module.s3[0].express_bucket_name, null)
}

output "ssh_command" {
  description = "SSH command to connect to the client instance"
  value       = try("ssh -i bench-key-${terraform.workspace}.pem ubuntu@${module.ec2_client[0].public_ip}", null)
}
