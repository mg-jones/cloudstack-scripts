#!/usr/bin/env python2.7
'''A script to provision a new virtual instance within the Cloudstack infrastructure.
The Cloudstack Manager and Zone are defined based on the hostname paramater passed,
which is evaluated for domain and environment. The corresponding config options from
.cloud.cfg are matched, and an API call made to the CS Manager.
'''

import argparse
import ConfigParser
import getpass
import logging
import os
import socket
import sys
import time
import urllib2

import CloudStack
import paramiko


def parse_arguments(config):
    '''Parse arguments and options'''
    sizes = CloudStack.cloudsizes(config)
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("hostname",
                        help="The FQDN of the host you are building")
    parser.add_argument("size", help="\n".join(sizes))
    parser.add_argument("--ipaddress", help="Force an IP address")
    parser.add_argument("--computenode",
                        help="Force a build on a specified compute node")
    parser.add_argument("--templatename",
                        help="Specify a template name")
    parser.add_argument("--affinitygroup",
                        help="Add the VM to the provided Affinity Group")
    args = parser.parse_args()
    return args


def assign_affinity(cloudstack, groupname, account, domainid):
    '''Create an affinity group, or return the existing affinity group name'''
    allgroups = cloudstack.list_affinity_groups(listall='true').get('affinitygroup', [])
    for group in allgroups:
        if group['name'] == groupname:
            print "Assigning to existing affinity group: ID - {}".format(group['id'])
            return
    grouptype = 'host anti-affinity'
    print "Creating new affinity group: {}".format(groupname)
    groupcreate = cloudstack.create_affinity_group(name=groupname, type=grouptype,
                                                   account=account, domainid=domainid)
    cloudstack.wait_for_job(groupcreate['jobid'])
    return


def build_node(cloudstack, vmname, computenode, size, ipaddress, template, affinitygroup):
    '''Build a node, deploying a job request to the manager, waiting
    for completion, then returning the IP
    '''
    account = cloudstack.account
    domainid = cloudstack.fetch_domain(cloudstack.domain)['id']
    networkid = cloudstack.fetch_network(domainid, "Application")['id']
    zoneid = cloudstack.fetch_zone(cloudstack.zone)['id']
    serviceid = cloudstack.fetch_service_offering(size)['id']
    # Evaluate the template
    try:
        templateid = cloudstack.fetch_template(template)['id']
    except Exception:
        logging.error('No such template - %s', template)
        logging.error('Available templates:')
        for tmpls in cloudstack.list_available_templates():
            logging.error('- %s', tmpls['name'])
        sys.exit(1)
    # Standard request parameters
    req = {'account': account,
           'name': vmname,
           'domainid': domainid,
           'zoneid': zoneid,
           'networkids': networkid,
           'serviceofferingid': serviceid,
           'templateid': templateid
          }
    # Force ip address
    if ipaddress:
        req['ipaddress'] = ipaddress
    # Assign optional affinity group
    if affinitygroup:
        assign_affinity(cloudstack, affinitygroup, account, domainid)
        req['affinitygroupnames'] = affinitygroup
    # Force compute host
    if computenode:
        hostid = cloudstack.fetch_host(computenode, zoneid)['id']
        if not hostid:
            logging.error("No compute host found matching %s", computenode)
            sys.exit(2)
        req['hostid'] = hostid
        print "Forcing build on {}".format(computenode)
    # Request the build
    print "Waiting for the node to be provisioned",
    request = cloudstack.deployVirtualMachine(**req)
    req_job = cloudstack.wait_for_job(request['jobid'])
    print "Node is built"
    ipaddress = req_job['jobresult']['virtualmachine']['nic'][0]['ipaddress']
    return ipaddress


def wait_for_ssh(ipaddr):
    '''Wait for the node to return an ssh connection'''
    while True:
        try:
            socket.create_connection((ipaddr, 22), 1)
            print ""
            time.sleep(2)
            return
        except socket.error:
            print ".",
            sys.stdout.flush()
            time.sleep(1)


class VirtualMachine(object):
    '''Connection to a VM and methods for interaction'''

    def __init__(self, vmip):
        '''Connect to remote server'''
        self.vmip = vmip
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(self.vmip, username='root', timeout=45)
        except paramiko.SSHException:
            password = getpass.getpass(prompt="Please enter password for %s: " % vmip)
            self.ssh.connect(self.vmip, username='root', password=password, timeout=20)

    def run_command(self, command):
        '''Run a remote command on the VM'''
        _, stdout, stderr = self.ssh.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            logging.error("Failure on remote command: %s", stderr.read())
            sys.exit(2)
        return

    def close(self):
        '''Close ssh connection'''
        self.ssh.close()
        return


def main():
    '''Parse the options, build the node, and configure through Chef'''
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    # Parse the config and script parameters
    user_config = ConfigParser.ConfigParser()
    user_config.read(os.path.expanduser("~/.cloud.cfg"))
    args = parse_arguments(user_config)
    hostname = args.hostname
    size = args.size
    ipaddress = args.ipaddress
    computenode = args.computenode
    templatename = args.templatename
    affinitygroup = args.affinitygroup
    # Return the proper Cloudstack connection (which Manager/Zone to use)
    cloudstack = CloudStack.cloud(hostname, user_config)
    # Define template
    if templatename:
        template = templatename
    else:
        template = user_config.get("Global", "DefaultTemplate")
    # Build the node
    vmname = CloudStack.HostName.vm_name
    try:
        node_ip = build_node(cloudstack, computenode, vmname, size,
                             ipaddress, template, affinitygroup)
    except urllib2.HTTPError, err:
        logging.exception("Failed to request node build: %s", err)
        sys.exit(1)
    # Use node_ip + VirtualMachine to run a remote command
    # For instance, connect to Puppet/Chef, run some post-provisioning script, etc
    print "Completed"


if __name__ == '__main__':
    main()
