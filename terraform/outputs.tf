output "bronze_bucket" {
	value = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
	value = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
	value = aws_s3_bucket.gold.bucket
}

output "athena_results_bucket" {
	value = aws_s3_bucket.athena_results.bucket
}

output "athena_workgroup" {
	value = aws_athena_workgroup.urban_pulse.name
}

