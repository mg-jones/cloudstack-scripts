#!/usr/bin/env python2.7
'''Migrate a VM from one compute node to another
'''

import argparse
import ConfigParser
import getpass
import logging
import os
import re
import socket
import sys
import time
import xmltodict

import CloudStack
import paramiko


def parse_arguments():
    '''Parse arguments/options'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("vmname", type=str, help="The FQDN of the migrating VM")
    parser.add_argument("computenode", type=str, help="Destination compute node")
    parser.add_argument("--nocompress", action='store_true',
                        help="Turn off archiving/compression in rsync")
    parser.add_argument("--hostname", help="Migrate with a new hostname")
    parser.add_argument("--nodestroy", action='store_true',
                        help="Do not destroy the original VM")
    parser.add_argument("--debug", action='store_true',
                        help="Turn on debugging")
    args = parser.parse_args()
    return args


class ComputeNode(object):
    '''Connection to source node and methods for interaction'''

    def __init__(self, hostip):
        '''Connect to remote server'''
        self.hostip = hostip
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(self.hostip, username='root', timeout=20)
        except paramiko.SSHException:
            password = getpass.getpass(prompt="Please enter password for %s: " % hostip)
            self.ssh.connect(self.hostip, username='root', password=password, timeout=20)

    def get_volume_name(self, vminstance):
        '''dumpxml of instance-name and parse source file path'''
        # command = "virsh dumpxml %s |grep 'source file='" % vminstance
        command = "virsh dumpxml %s" % vminstance
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'virsh dumpxml': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to parse for image file: %s", error)
            sys.exit(2)
        output = stdout.read()
        xmldict = xmltodict.parse(output)
        alldisks = []
        try:
            for disk in xmldict['domain']['devices']['disk']:
                if disk['@device'] == 'disk':
                    alldisks.append(disk)
        except KeyError:
            logging.exception("Unable to image file from VM xml")
            sys.exit(2)
        if len(alldisks) > 1:
            print '''
            Found more than one image file, probably an attached disk.
            This node must be manually migrated, the ability to handle extra
            volumes is not integrated into this script.
            '''
            sys.exit(4)
        else:
            storagefile = alldisks[0]['source']['@file']
            return storagefile

    def get_volume_type(self, storagefile):
        '''Determine image file type (qcow2 or raw)'''
        command = "qemu-img info %s |grep 'file format'" % storagefile
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'qemu-img info': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to determine image type: %s", error)
            sys.exit(2)
        output = stdout.read()
        query = re.compile("file format: (.*)")
        vol_type = query.search(output).group(1)
        return vol_type

    def get_backing_file(self, storagefile):
        '''Parse backing file path for qcow2 image'''
        command = "qemu-img info %s |grep 'backing file'" % storagefile
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'qemu-img info': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to determine backing file: %s", error)
            sys.exit(2)
        output = stdout.read()
        query = re.compile(r"backing file: (.*) \(.*$")
        query2 = re.compile("backing file: (.*)")
        search = query.search(output)
        if not search:
            search = query2.search(output)
        if not search:
            logging.error("Unable to parse backing file")
            sys.exit(2)
        backing_file = search.group(1).split('/')[5]
        return backing_file

    def convert_image(self, storagefile, ori_type, new_format, nodestroy):
        '''Convert image format'''
        if nodestroy:
            opt = "cp"
        else:
            opt = "mv"
        command_copy = "cd /var/lib/libvirt/images; %s %s %s.ori" % (opt, storagefile, storagefile)
        command_conv = "cd /var/lib/libvirt/images; \
            qemu-img convert -f %s -O %s %s.ori %s" % (ori_type,
                                                       new_format,
                                                       storagefile,
                                                       storagefile)
        try:
            _, stdout, stderr = self.ssh.exec_command(command_copy)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise Exception("Error moving image file: %s" % stderr.read())
            _, stdout, stderr = self.ssh.exec_command(command_conv)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'qemu-img convert': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to convert image file: %s", error)
            sys.exit(2)
        return

    def tar_volume(self, vmname, storagefile):
        '''tar raw sparse image'''
        imagetar = vmname + '.tgz'
        filename = storagefile.split('/')[5]
        command = "cd /var/lib/libvirt/images; bsdtar -cf %s %s" % (imagetar, filename)
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'bsdtar -cf': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to archive image file: %s", error)
            sys.exit(2)
        return imagetar

    def clean_file(self, filename):
        '''Delete a remote file'''
        command = "cd /var/lib/libvirt/images; rm -f %s " % filename
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'rm -f': %s" % stderr.read())
        except RuntimeError, error:
            logging.warn("Failed to cleanup archive file: %s", error)

    def untar_volume(self, imagetar):
        '''Extract tar archive of raw image file'''
        command = "cd /var/lib/libvirt/images; tar -xSf %s" % imagetar
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'tar -xSf': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to extract image archive: %s", error)
            sys.exit(2)
        return

    def rsync_volume(self, new_host_ip, imagefile, nocompress):
        '''rsync image archive to destination compute host'''
        imagepath = '/var/lib/libvirt/images/' + imagefile
        if nocompress:
            command = "rsync -v -e 'ssh -i /root/.ssh/id_rsa_compute' \
            --progress %s root@%s:%s" % (imagepath, new_host_ip, imagepath)
        else:
            command = "rsync -avz -e 'ssh -i /root/.ssh/id_rsa_compute' \
                --progress %s root@%s:%s" % (imagepath, new_host_ip, imagepath)
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            while not stdout.channel.exit_status_ready():
                if stdout.channel.recv_ready():
                    output = stdout.channel.recv(1024).strip()
                    sys.stdout.write("\r%s" % output)
                    sys.stdout.flush()
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'rynsc': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to rsync image archive: %s", error)
            sys.exit(2)
        print ''
        return

    def copy_dhclient(self, oldfile, newfile):
        '''Copy dhclient leases between images'''
        out_command = "virt-copy-out -a %s /var/lib/dhcp/dhclient.eth0.leases \
            /tmp" % oldfile
        in_command = "virt-copy-in -a %s /tmp/dhclient.eth0.leases /var/lib/dhcp" % newfile
        try:
            _, stdout, stderr = self.ssh.exec_command(out_command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise Exception("Error with 'virt-copy-out': %s" % stderr.read())
            _, stdout, stderr = self.ssh.exec_command(in_command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'virt-copy-in': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to copy dhcp lease file: %s", error)
            sys.exit(2)
        return

    def copy_hostname(self, oldfile, newfile):
        '''Copy /etc/hostname between images'''
        out_command = "virt-copy-out -a %s /etc/hostname /tmp" % oldfile
        in_command = "virt-copy-in -a %s /tmp/hostname /etc" % newfile
        try:
            _, stdout, stderr = self.ssh.exec_command(out_command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise Exception("Error with 'virt-copy-out': %s" % stderr.read())
            _, stdout, stderr = self.ssh.exec_command(in_command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'virt-copy-in': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to copy hostname file: %s", error)
            sys.exit(2)
        return

    def replace_volume(self, oldfile, newfile):
        '''Overwrite a file'''
        command = "mv -f %s %s" % (newfile, oldfile)
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError("Error with 'mv -f': %s" % stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to replace volume file: %s", error)
            sys.exit(2)
        return

    def close(self):
        '''Close the SSH connection'''
        self.ssh.close()
        return


class VirtualMachine(object):
    '''Connection to a VM and methods for interaction'''

    def __init__(self, vmip):
        '''Connect to remote server'''
        self.vmip = vmip
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(self.vmip, username='root', timeout=20)
        except paramiko.SSHException:
            password = getpass.getpass(prompt="Please enter password for %s: " % vmip)
            self.ssh.connect(self.vmip, username='root', password=password, timeout=20)

    def run_remote_command(self, command):
        '''Run a remote command'''
        try:
            _, stdout, stderr = self.ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError(stderr.read())
        except RuntimeError, error:
            logging.exception("Failed to run remote command: %s", error)
            sys.exit(2)
        return

    def close(self):
        '''Close the SSH connection'''
        self.ssh.close()
        return


def wait_for_ssh(ipaddress):
    '''Wait for the node to return an ssh connection'''
    while True:
        try:
            socket.create_connection((ipaddress, 22), 1)
            return
        except socket.error:
            print ".",
            sys.stdout.flush()
            time.sleep(1)


def main():
    '''Main process that handles arguments, builds CloudStack object,
    and calls the methods to migrate the virtual
    '''
    logging.basicConfig(level=logging.INFO)
    config = ConfigParser.RawConfigParser()
    config.read(os.path.expanduser('~/.cloud.cfg'))
    args = parse_arguments()
    vmname = args.vmname
    computenode = args.computenode
    nocompress = args.nocompress
    newhostname = args.hostname
    debug = args.debug
    nodestroy = args.nodestroy
    if nodestroy and not newhostname:
        logging.error("Incompatible options, cannot keep old VM without providing a new hostname & IP")
        sys.exit(1)
    cloud = CloudStack.cloud(vmname, config)
    if newhostname:
        newcloud = CloudStack.cloud(newhostname, config)
        newzoneid = newcloud.fetchZone(newcloud.ZONE)['id']
    # Query for VM
    vms = cloud.fetch_vms(vmname)
    if not vms:
        logging.error("No VM found matching '%s' in CloudStack, quitting", vmname)
        sys.exit(1)
    if len(vms) > 1:
        logging.error("Too many VMs found matching '%s'", vmname)
        sys.exit(1)
    oldvm = vms[0]
    # Query for host info (compute nodes)
    if newhostname:
        hostlist = newcloud.listHosts(name=computenode, zoneid=newzoneid).get('host', [])
    else:
        hostlist = cloud.listHosts(name=computenode, zoneid=oldvm['zoneid']).get('host', [])
    if not hostlist:
        logging.error("Unable to find a valid host matching %s", computenode)
        sys.exit(2)
    else:
        new_host = hostlist[0]
    hostlist = cloud.listHosts(name=oldvm['hostname'], zoneid=oldvm['zoneid']).get('host', [])
    if not hostlist:
        logging.error("Failed to find the VM's host, please make sure the VM is running")
        sys.exit(2)
    else:
        old_host = hostlist[0]
    # Get volume name, stop machine
    sourcecompute = ComputeNode(old_host['ipaddress'])
    storagefile = sourcecompute.get_volume_name(oldvm['instancename'])
    print "Stopping VM",
    request = cloud.stopVirtualMachine(id=oldvm['id'])
    cloud.wait_for_job(request['jobid'])
    # Determine volume type and convert if necessary
    print "Determining volume type"
    vol_type = sourcecompute.get_volume_type(storagefile)
    ## DEFAULT VOLUME TYPES PER VERSION
    img_map = {'4.4.2': 'raw',
               '4.9.3.0': 'qcow2'
              }
    agent_vers = new_host['version']
    imageformat = img_map.get(agent_vers, '')
    if not imageformat:
        logging.error("Unable to determine output image format for agent version %s", agent_vers)
        sys.exit(2)
    if imageformat and vol_type != imageformat:
        print "... type is '%s', converting to '%s'" % (vol_type, imageformat)
        sourcecompute.convert_image(storagefile, vol_type, imageformat, nodestroy)
    # Archive image file and rsync to destination host
    print "Migrating root volume. Please be patient, this will take a few minutes."
    print "... archiving image file"
    imagetar = sourcecompute.tar_volume(oldvm['name'], storagefile)
    print "... rsyncing image archive"
    sourcecompute.rsync_volume(new_host['ipaddress'], imagetar, nocompress)
    sourcecompute.clean_file(imagetar)
    # Destroy old VM
    if not nodestroy:
        oldqcow = storagefile + '.ori'
        sourcecompute.clean_file(oldqcow)
        print "Destroying old VM",
        request = cloud.destroyVirtualMachine(id=oldvm['id'])
        cloud.wait_for_job(request['jobid'])
        sec_wait = 60
        while sec_wait >= 0:
            sys.stdout.write("\rWait for expunge (%d seconds) " % sec_wait)
            sys.stdout.flush()
            sec_wait -= 1
            time.sleep(1)
    else:
        print "Restarting old VM",
        request = cloud.startVirtualMachine(id=oldvm['id'])
        cloud.wait_for_job(request['jobid'])
    sourcecompute.close()
    # Rebuild VM on destination host
    print "\nDeploying new VM",
    if newhostname:
        cloud = CloudStack.cloud(newhostname, config)
        vmname = newhostname
        ######## CONTINUE #########
    account, shortname, domain, application = cloud.parseHostname(vmname)
    template = oldvm['templatename']
    templateid = cloud.fetchTemplate(template)['id']
    if not templateid:
        print "Unable to lookup template ID for %s" % template
        sys.exit(2)
    req_dict = {
        'templateid': templateid,
        'account': account,
        'name': shortname,
        'hostid': new_host['id']
    }
    if newhostname:
        domainid = cloud.fetchDomain(domain)['id']
        req_dict['domainid'] = domainid
        req_dict['networkids'] = cloud.fetchNetwork(domainid, "Application")['id']
        req_dict['zoneid'] = cloud.fetchZone(cloud.ZONE)['id']
        vmsize = oldvm['serviceofferingname']
        req_dict['serviceofferingid'] = cloud.fetchServiceOffering(vmsize)['id']
    else:
        req_dict['ipaddress'] = oldvm['nic'][0]['ipaddress']
        req_dict['domainid'] = oldvm['domainid']
        req_dict['networkids'] = oldvm['nic'][0]['networkid']
        req_dict['zoneid'] = oldvm['zoneid']
        req_dict['serviceofferingid'] = oldvm['serviceofferingid']
    if debug:
        print req_dict
    request = cloud.deployVirtualMachine(**req_dict)
    cloud.waitForJob(request['jobid'])
    print "Node '%s' has been rebuilt on '%s'" % (vmname, new_host['name'])
    vms = cloud.fetchVMs(vmname)
    newvm = vms[0]
    # Get new volume name, replace with copy
    destcompute = ComputeNode(new_host['ipaddress'])
    print "Setting up migrated volume",
    tmpfile = destcompute.get_volume_name(newvm['instancename'])
    request = cloud.stopVirtualMachine(id=newvm['id'])
    cloud.waitForJob(request['jobid'])
    destcompute.untar_volume(imagetar)
    # Replace network files if IP/hostname changed
    if newhostname:
        print "... copying dhcp leases"
        destcompute.copy_dhclient(tmpfile, storagefile)
        print "... validating hostname"
        destcompute.copy_hostname(tmpfile, storagefile)
    destcompute.replace_volume(tmpfile, storagefile)
    # Restart new VM with the copied image file
    print "Starting new VM",
    request = cloud.startVirtualMachine(id=newvm['id'])
    req_job = cloud.waitForJob(request['jobid'])
    ipaddress = req_job['jobresult']['virtualmachine']['nic'][0]['ipaddress']
    print "Waiting for node to ssh",
    wait_for_ssh(ipaddress)
    # Start chef-client
    print "\nStarting Chef"
    new_vm = VirtualMachine(ipaddress)
    new_vm.run_chef()
    new_vm.close()
    # Cleanup
    destcompute.clean_file(imagetar)
    destcompute.close()
    print "Completed"


if __name__ == '__main__':
    main()
