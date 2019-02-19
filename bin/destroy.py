#!/usr/bin/env python2.7
'''Destroy a CloudStack virtual node. Creates a CloudStack object and
calls the destroyNode method.
'''

import argparse
import ConfigParser
import logging
import os
import sys

import CloudStack


def parse_arguments():
    '''Parse arguments/options'''
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", help="The FQDN of the host to destroy")
    parser.add_argument("--force", action="store_true",
                        help="Delete without confirming")
    args = parser.parse_args()
    return args


def destroy_node(cloudstack, hostname, force):
    '''Destroy a node, prompting for user confirmation'''
    print "Searching for", hostname
    hosts = cloudstack.fetch_vms(hostname)
    if hosts:
        if len(hosts) > 1:
            print "More than 1 hosts found:", ", ".join([h['name'] for h in hosts])
            print "Destroy cancelled, please narrow your search"
            sys.exit(2)
        else:
            host = hosts[0]
            print "Found matching host: %s (ID=%s)" % (host['name'], host['id'])
    else:
        logging.error("No VMs found with the name %s", hostname)
        sys.exit(2)
    if force is True:
        request = cloudstack.destroyVirtualMachine(id=host['id'])
        cloudstack.wait_for_job(request['jobid'])
    else:
        confirm = raw_input("Would you like to destroy %s (ID=%s)[%s]? y/n :" %
                            (host['name'], host['id'], host['domain']))
        if confirm.lower() == 'y':
            request = cloudstack.destroyVirtualMachine(id=host['id'])
            cloudstack.wait_for_job(request['jobid'])
        else:
            print "Destroy cancelled"
            sys.exit(1)


def main():
    '''Main process, handle arguments, create CloudStack object and destroy node'''
    logging.basicConfig(level=logging.INFO)
    config = ConfigParser.RawConfigParser()
    config.read(os.path.expanduser('~/.cloud.cfg'))
    args = parse_arguments()
    hostname = args.hostname
    force = args.force
    cloudstack = CloudStack.cloud(hostname, config)
    destroy_node(cloudstack, hostname, force)
    print "Completed"


if __name__ == '__main__':
    main()
