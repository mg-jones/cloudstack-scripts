#!/usr/bin/env python2.7
'''Query and display CloudStack values for a given VM
'''

import argparse
import ConfigParser
import logging
import os
import sys

import CloudStack


def parse_arguments():
    '''Parse arguments/options'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("hostname", type=str, help="The FQDN of the query VM")
    args = parser.parse_args()
    return args


def main():
    '''Main process that handles arguments, builds CloudStack object,
    and calls the methods to search and display the host
    '''
    logging.basicConfig(level=logging.INFO)
    config = ConfigParser.RawConfigParser()
    config.read(os.path.expanduser('~/.cloud.cfg'))
    args = parse_arguments()
    hostname = args.hostname
    cloudstack = CloudStack.cloud(hostname, config)
    vms = cloudstack.fetch_vms(hostname)
    if not vms:
        logging.error("Did not find VM matching %s", hostname)
        sys.exit(2)
    elif len(vms) > 1:
        print "Found multiple VMs matching '%s'" % hostname
    for virtm in vms:
        for key, value in virtm.iteritems():
            print "%s: %s" % (key, value)


if __name__ == '__main__':
    main()
