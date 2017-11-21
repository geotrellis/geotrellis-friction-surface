resource "aws_security_group" "security-group" {
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  ingress {
    from_port   = "22"
    to_port     = "22"
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = "${var.jmx_port}"
    to_port     = "${var.jmx_port}"
    protocol    = "tcp"
    cidr_blocks = ["${var.jmx_ip}/32"]
  }

  ingress {
    from_port   = "${var.rmi_port}"
    to_port     = "${var.rmi_port}"
    protocol    = "tcp"
    cidr_blocks = ["${var.jmx_ip}/32"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}
