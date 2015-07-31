### Prerequsites:

On the host, equivalent or greater:

* VirtualBox 4.3
  * Note that with Ubuntu and Debian, you will always want to install the system packages
    as we attach storage and command behavior is wildly inconsistent between
    versions.
* Vagrant 1.7.4
* Ansible 1.9.2
  * install with pip; you'll want to install `python-pip` and `python-dev` on
    ubuntu machines, then `sudo pip install ansible`.
  * The make tooling in this repository will install it for you if it is not
    already installed. If you are not root, it may fail to perform this
    operation. The solution to this problem is to install ansible
    independently as described above.
* build-essential
* golang 1.4.x

Your guests will configure themselves.

### Usage instructions

Be sure to start the environment with `make start` before you continue with
these steps. You must have working vagrant, virtualbox, and ansible.

You will also want to `make ssh` to ssh into the `mon0` VM to follow along.

1. Start the volmaster with the sample `volmaster.json` file. It should live in
   `/etc/volmaster.json`. Start it by typing `volmaster /etc/volmaster.json` in
   the guest or by typing `make run-volmaster` from the host.
1. Start the volplugin with the tenant name `tenant1`: `volplugin tenant1` on
   the guest, or `make run-volplugin` from the host.
1. `make ssh` in, and execute docker with the appropriate volume driver:
   * `docker run  -it --volume-driver tenant1 -v tmp:/mnt ubuntu`
1. You should have a volume on `/mnt` pointing at a `/dev/rbd#` device. Exit
   the shell to unmap the device.

### Build Instructions

```
# builds and provisions VMs
$ make start

# tears down VMs.
$ make stop

# provisions VMs with ansible
$ make provision

# ssh into the monitor host for volplugin testing
$ make ssh

# build the binaries in the guest
$ make build

# install ansible on the host (required for vagrant)
$ make install-ansible

# run the unit tests
$ make test

# start the volplugin on the monitor host and hang (for logging)
$ make run-volplugin

# start the volplugin on the local host
$ make volplugin-start

# start the volmaster on the monitor host and hang (for logging)
$ make run-volmaster

# start the volmaster on the local host
$ make volmaster-start
```