resource "aws_emr_cluster" "emr-spark-cluster" {
  name          = "geotrellis-friction-surface Ingest"
  applications  = ["Hadoop", "Spark", "Ganglia"]
  log_uri       = "${var.s3_uri}"
  release_label = "emr-5.10.0"
  service_role  = "${var.emr_service_role}"

  ec2_attributes {
    instance_profile = "${var.emr_instance_profile}"
    key_name         = "${var.key_name}"

    emr_managed_master_security_group = "${aws_security_group.security-group.id}"
    emr_managed_slave_security_group  = "${aws_security_group.security-group.id}"
  }

  instance_group {
    bid_price      = "${var.bid_price}"
    instance_count = 1
    instance_role  = "MASTER"
    instance_type  = "m3.xlarge"
    name           = "ingest-master"
  }

  instance_group {
    bid_price      = "${var.bid_price}"
    instance_count = "${var.worker_count}"
    instance_role  = "CORE"
    instance_type  = "m3.2xlarge"
    name           = "ingest-core"
  }

  # bootstrap_action {
  #   path = "s3://${var.bs_bucket}/${var.bs_prefix}/bootstrap.sh"
  #   name = "patch configuration"
  # }
}

output "emr-id" {
  value = "${aws_emr_cluster.emr-spark-cluster.id}"
}

output "emr-master" {
  value = "${aws_emr_cluster.emr-spark-cluster.master_public_dns}"
}
