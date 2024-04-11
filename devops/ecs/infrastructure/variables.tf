# data "aws_caller_identity" "current" {}

locals {
  env        = terraform.workspace == "default" ? "dev" : terraform.workspace
  project    = "starknet-lending-interface"
  team       = "data-engineering"
  account_id = "522495932155" #TODO: Remove this hardcode and use data.aws_caller_identity.current.account_id
}

variable "ecr_repo_url" {
  type        = string
  description = "URI of the ECR repository"
  default     = "522495932155.dkr.ecr.us-east-1.amazonaws.com/starknet-openblocklabs/starknet-lending-interface"
}

variable "ecr_repo_image_tag" {
  type        = string
  description = "Tag of the ECR repository"
  default     = "latest"
}

variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "us-east-1"
}
