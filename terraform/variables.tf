variable "aws_region" {
	type    = string
	default = "eu-west-3"
}

variable "project_name" {
	type    = string
	default = "urban-pulse-paris"
}

variable "bronze_bucket" {
	type = string
}

variable "silver_bucket" {
	type = string
}

variable "gold_bucket" {
	type = string
}

variable "athena_results_bucket" {
	type = string
}

