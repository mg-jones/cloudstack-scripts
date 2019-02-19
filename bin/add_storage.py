#!/usr/bin/env python2.7
'''Add an extra storage volume to a CloudStack node. The script builds a new
volume of type "Datadisk" according to the provided size parameter, and attaches it
to the provided host. This volume can be formatted and mounted as normal.
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
    parser.add_argument("hostname", type=str, help="The FQDN of the VM requiring storage")
    parser.add_argument("size", type=int, help="Size in GB")
    args = parser.parse_args()
    return args


def main():
    '''Main process that handles arguments, builds CloudStack object,
    and calls the methods to create the volume
    '''
    logging.basicConfig(level=logging.INFO)
    config = ConfigParser.ConfigParser()
    config.read(os.path.expanduser('~/.cloud.cfg'))
    args = parse_arguments()
    hostname = args.hostname
    size = str(args.size)
    cloudstack = CloudStack.cloud(hostname, config)
    vms = cloudstack.fetch_vms(hostname)
    if len(vms) > 1:
        logging.error("Too many VMs found")
        sys.exit(1)
    virtm = vms[0]
    host_id = virtm['hostid']
    host = cloudstack.listHosts(id=host_id)['host'][0]
    # Check for existing storage pools
    storagepools = cloudstack.listStoragePools(ipaddress=host['ipaddress']).get('storagepool', [])
    if storagepools:
        storagepool = storagepools[0]
    if host['name'] != storagepool.get('tags'):
        print "Adding a tag to the storage pool"
        cloudstack.updateStoragePool(id=storagepool['id'], tags=host['name'])
    diskoffering = cloudstack.listDiskOfferings(name=host['name']).get('diskoffering', '')
    if not diskoffering:
        print "Creating a disk offering for", host['name']
        diskoffering = cloudstack.createDiskOffering(displaytext=host['name'], name=host['name'],
                                                     customized='true', storagetype='local',
                                                     tags=host['name']).get('diskoffering', [])
    else:
        diskoffering = diskoffering[0]
    # Create the new volume and attach to the VM
    print "Requesting a volume of %sGB" % size,
    request = cloudstack.createVolume(name=hostname + '-data', diskOfferingId=diskoffering['id'],
                                      size=size, zoneId=virtm['zoneid'], account=virtm['account'],
                                      domainid=virtm['domainid'])
    result = cloudstack.wait_for_job(request['jobid'])['jobresult']
    volume = result['volume']
    print "Volume %s[%s] is created" % (volume['name'], volume['id'])
    print "Mounting volume to node",
    request = cloudstack.attachVolume(id=volume['id'], virtualmachineid=virtm['id'])
    cloudstack.wait_for_job(request['jobid'])
    print "Completed"


if __name__ == '__main__':
    main()
