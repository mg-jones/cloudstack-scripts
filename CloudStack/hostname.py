'''Build a hostname with defined properties from a FQDN'''

class HostName(object):
    '''Represents a properly formatted hostname'''

    envs = ('prod', 'stage', 'dev')
    # site domains, ex] .com, .net, .customdomain
    sites = ('sea', 'seastg', 'fkb', 'joy')

    def __init__(self, fqdn):
        self.fqdn = fqdn
        self.name, _, self.domain = self.fqdn.partition('.')

    @property
    def vm_name(self):
        '''Return short vm name'''
        return self.name

    @property
    def base_name(self):
        '''Host domain base'''
        # ex] example.com, otherexample.net
        domain_parts = self.domain.split('.')
        if len(domain_parts) >= 2:
            return domain_parts[-2]
        return ''

    @property
    def cs_env(self):
        '''Host CloudStack environment'''
        # Same as site
        return self.site

    @property
    def site(self):
        '''Host site'''
        domain_parts = self.domain.split('.')
        if not domain_parts:
            return ''
        elif domain_parts[-1] in self.sites:
            return domain_parts[-1]
        else:
            return ''

    @property
    def env(self):
        '''Host environment'''
        name = self.name[:-3]
        for env in self.envs:
            if name.endswith(env):
                return env
        return ''
