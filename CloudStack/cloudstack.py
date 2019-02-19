'''Utilities module for interacting with the CloudStack API.
Contains a function for parsing the hostname and assigning the correct
config (.cloud.cfg) section, as well as a function for returning
available CloudStack virtual sizes.
Contains one class (CloudStack) which inherits methods from the cs.CloudStack
    -- fetchVMs(fqdn) Fetch all VMs matching the FQDN provided
    -- fetchStoragePool(ip) List all storage pools attached to IP
    -- getSizes() List sizes available for provisioning
'''

import sys
import time

from ConfigParser import (NoSectionError, NoOptionError)
from urllib2 import HTTPError

from cs import CloudStack as CStack
from CloudStack.hostname import HostName


def cloud(fqdn, config):
    '''Parse a hostname and assign the corresponding config section,
    returning a CloudStack object
    '''
    hostname = HostName(fqdn)
    env = hostname.cs_env
    if not env:
        raise Exception('Could not identify cloudstack environment from fqdn')
    if not config.has_section(env):
        raise Exception('.cloud.cfg does not have the required env: {}'.format(env))
    return CloudStack(config.get(env, 'apiurl'), config.get(env, 'apikey'),
                      config.get(env, 'secret'), config.get(env, 'zone'),
                      config.get(env, 'account'), config.get(env, 'domain'))


def cloudsizes(config):
    '''Return available CloudStack virtual sizes'''
    secs = config.sections()
    secs.remove('Global')
    sizes = []
    for env in secs:
        sizes.append('---{} Sizes---'.format(env))
        try:
            apiurl = config.get(env, 'apiurl')
            apikey = config.get(env, 'apikey')
            secret = config.get(env, 'secret')
            zone = config.get(env, 'zone')
            account = config.get(env, 'account')
            domain = config.get(env, 'domain')
        except (NoSectionError, NoOptionError):
            continue
        if all([apiurl, apikey, secret, zone]):
            cloudstack = CloudStack(apiurl, apikey, secret, zone, account, domain)
            try:
                sizes.extend(cloudstack.get_sizes())
            except HTTPError:
                continue
    return sizes


class CloudStack(CStack):
    '''A CloudStack object for interaction through the API, inherits from the
    cs.CloudStack library
    '''
    def __init__(self, url, key, secret, zone, account, domain):
        super(CloudStack, self).__init__(endpoint=url, key=key, secret=secret)
        self.zone = zone
        self.account = account
        self.domain = domain

    def fetch_domain(self, domain):
        '''Retrieve domain from CS'''
        domains = self.listDomains(listall='true').get('domain', [])
        for dom in domains:
            if domain in dom['name']:
                return dom
        return None

    def fetch_network(self, domain, network):
        '''Retrieve network from CS'''
        networks = self.listNetworks(domainid=domain).get('network', [])
        for net in networks:
            if network in net['name']:
                return net
        return None

    def fetch_zone(self, zone):
        '''Retrieve zone from CS'''
        zones = self.listZones(listall='true').get('zone', [])
        for zon in zones:
            if zone in zon['name']:
                return zon
        return None

    def fetch_service_offering(self, name):
        '''Retrieve service offering from CS'''
        services = self.listServiceOfferings(listall='true').get('serviceoffering', [])
        for svc in services:
            if svc['name'] == name:
                return svc
        return None

    def fetch_host(self, name, zoneid):
        '''Retrieve host from CS'''
        short_name = name.split(".")[0]
        hosts = self.listHosts(listall='true', zoneid=zoneid).get('host', [])
        for hst in hosts:
            if hst['name'] == name or hst['name'] == short_name:
                return hst
        return None

    def fetch_template(self, template):
        '''Retrieve template from CS'''
        for fltr in ('featured', 'self', 'self-executable', 'executable', 'community'):
            templates = self.listTemplates(listall='true', templatefilter=fltr).get('template', [])
            for tmpl in templates:
                if tmpl['name'] == template:
                    return tmpl
        return None

    def list_available_templates(self):
        '''List all available VM templates'''
        alltemplates = []
        for fltr in ('featured', 'self', 'self-executable', 'executable', 'community'):
            templates = self.listTemplates(listall='true', templatefilter=fltr).get('template', [])
            alltemplates.append(templates)
        return alltemplates

    def fetch_vms(self, fqdn):
        '''Return a list of VMs matching with the provided FQDN'''
        hostname = HostName(fqdn)
        zoneid = self.fetchZone(self.ZONE)['id']
        domainid = self.fetchDomain(self.DOMAIN)['id']
        host = hostname.name
        vms = self.listVirtualMachines(listall='true', zoneid=zoneid,
                                       domainid=domainid).get('virtualmachine', [])
        hosts = []
        for virtm in vms:
            if virtm['name'] == host:
                hosts.append(virtm)
        return hosts

    def fetch_storage_pool(self, ipaddress):
        '''Return storage pools attached to the provided IP'''
        pools = self.listStoragePools(listall='true').get('storagepool', [])
        for pool in pools:
            if pool['ipaddress'] == ipaddress:
                return pool
        return None

    def wait_for_job(self, jobid):
        '''Wait for a job to finish'''
        while True:
            jobquery = self.queryAsyncJobResult(jobid=jobid)
            if jobquery['jobstatus'] == 0:
                print ".",
                sys.stdout.flush()
                time.sleep(5)
                continue
            else:
                print ""
                if jobquery['jobresult'].get("errorcode"):
                    print jobquery['jobresult']['errortext']
                    sys.exit(1)
                return jobquery

    def get_sizes(self):
        '''Return the formatted list of available CloudStack virtual sizes'''
        packages = ["CloudStack Sizes:"]
        for package in sorted(self.listServiceOfferings().get('serviceoffering', []),
                              key=lambda k: "%02d %02d" % (k['cpunumber'], k['memory'] / 1024)):
            packages.append("  %-40s %d Core(s), %dGB" %
                            (package['name'], package['cpunumber'], package['memory'] / 1024))
        return packages

    def get_volumes(self, hostname, zoneid):
        '''Returns a list of volumes for a compute node.'''
        short_hostname = hostname.split(".")[0]
        hostid = self.fetchHost(hostname, zoneid)['id']
        volumes = []
        for volume in self.listVolumes(listall='true', isrecursive='true',
                                       zoneid=zoneid, hostid=hostid).get('volume', []):
            storage = volume.get('storage', None)
            if short_hostname in storage or hostname in storage:
                volumes.append(volume)
        return volumes

    def start_vm(self, vmid):
        '''Request a VM start'''
        self.startVirtualMachine(id=vmid)
        return None
