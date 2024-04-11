terraform {
  backend "s3" {
    bucket = "obl-terraform-state"
    key    = "tf-worskpaces/starknet-lending-interface/terraform.tfstate"
    region = "us-east-1"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # docker = {
    #   source  = "kreuzwerker/docker"
    #   version = "2.15.0"
    # }
  }
}
