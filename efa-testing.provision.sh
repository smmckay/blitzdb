#!/bin/bash -ex

cd /tmp

sudo blkid
sudo lsblk
df -m

sudo yum install -y git gcc
curl -O https://efa-installer.amazonaws.com/aws-efa-installer-1.47.0.tar.gz
tar xf aws-efa-installer-1.47.0.tar.gz
pushd aws-efa-installer
sudo ./efa_installer.sh -y -n
popd
rm -rf aws-efa-installer*

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o install_rustup.sh
chmod +x install_rustup.sh
./install_rustup.sh -y
rm install_rustup.sh

cd /home/ec2-user
git clone https://github.com/smmckay/blitzdb /home/ec2-user/blitzdb
