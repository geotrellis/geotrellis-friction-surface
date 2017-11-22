variable "region" {
    type        = "string"
    description = "AWS Region"
    default     = "us-east-1"
}

variable "bs_bucket" {
    type        = "string"
    description = "S3 Bucket containing the boostrap script (e.g. bucket if the whole path is s3://bucket/containing/bootstrap)"
    default    = "geotrellis-test"
}

variable "bs_prefix" {
  type        = "string"
  description = "The prefix of the location of the bootstrap script (e.g. containing/bootstrap if the whole path is s3://bucket/containing/bootstrap)"
  default     = "geotrellis-friction-surface"
}

variable "key_name" {
    type        = "string"
    description = "The name of the EC2 secret key (primarily for SSH access)"
}

variable "s3_uri" {
    type        = "string"
    description = "Where EMR logs will be sent"
    default     = "s3n://geotrellis-test/terraform-logs/"
}

variable "worker_count" {
    type        = "string"
    description = "The number of worker nodes"
    default     = "1"
}

variable "emr_service_role" {
  type        = "string"
  description = "EMR service role"
  default     = "EMR_DefaultRole"
}

variable "emr_instance_profile" {
  type        = "string"
  description = "EMR instance profile"
  default     = "EMR_EC2_DefaultRole"
}

variable "bid_price" {
  type        = "string"
  description = "Bid Price"
  default     = "0.07"
}
