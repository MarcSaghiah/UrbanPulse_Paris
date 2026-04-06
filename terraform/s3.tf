resource "aws_s3_bucket" "bronze" {
	bucket = var.bronze_bucket
	tags   = local.tags
}

resource "aws_s3_bucket" "silver" {
	bucket = var.silver_bucket
	tags   = local.tags
}

resource "aws_s3_bucket" "gold" {
	bucket = var.gold_bucket
	tags   = local.tags
}

resource "aws_s3_bucket" "athena_results" {
	bucket = var.athena_results_bucket
	tags   = local.tags
}

resource "aws_athena_workgroup" "urban_pulse" {
	name = "urban-pulse-paris"

	configuration {
		enforce_workgroup_configuration    = true
		publish_cloudwatch_metrics_enabled = true

		result_configuration {
			output_location = "s3://${var.athena_results_bucket}/"
		}
	}

	tags = local.tags
}

