packer {
  required_plugins {
    amazon = {
      version = ">= 1.2.8"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

locals {
  timestamp = regex_replace(timestamp(), "[- TZ:]", "")
}

source "amazon-ebs" "al2023" {
  ami_name                    = "efa-testing-${local.timestamp}"
  instance_type               = "c8gn.16xlarge"
  region                      = "us-east-2"
  associate_public_ip_address = true
  security_group_id           = "sg-0607eb302432bde47"
  subnet_id                   = "subnet-0b9a94edfaa489790"
  ssh_keypair_name            = "id_ed25519"
  ssh_username                = "ec2-user"
  ssh_agent_auth              = true

  source_ami_filter {
    filters = {
      name                = "al2023-ami-2023*-kernel-6.18-arm64"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["137112412989"]
  }

  launch_block_device_mappings {
    device_name           = "/dev/xvda"
    volume_size           = 128
    volume_type           = "gp3"
    delete_on_termination = true
  }

}

build {
  sources = [
    "source.amazon-ebs.al2023"
  ]

  provisioner "shell" {
    script = "efa-testing.provision.sh"
  }
}
