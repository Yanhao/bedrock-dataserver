# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.provider "libvirt" do |vb|
    vb.memory = "2048"
  end
  config.vm.box = "debian/stretch64"
  config.vm.box_check_update = false

  #config.ssh.insert_key = false

  config.vm.provision "shell", inline: <<-SHELL
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/debian stretch main contrib non-free" > /etc/apt/sources.list
    apt-get update
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen
    locale-gen
    apt-get -y install vim build-essential cmake pkg-config nfs-kernel-server
  SHELL
  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # NOTE: This will enable public access to the opened port
  # config.vm.network "forwarded_port", guest: 80, host: 8080

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine and only allow access
  # via 127.0.0.1 to disable public access
  # config.vm.network "forwarded_port", guest: 80, host: 8080, host_ip: "127.0.0.1"

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  #config.vm.network "private_network", ip: "192.168.33.10"


  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  # config.vm.network "public_network"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  #config.vm.synced_folder ".", "/vagrant", type: "nfs"

end
