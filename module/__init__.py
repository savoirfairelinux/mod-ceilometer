"""
Shinken mod-ceilometer Module.
"""

from .module import CeilometerBroker
from shinken.log import logger

properties = {
    'daemons': ['broker'],
    'type': 'ceilometer_perfdata',
    'external': False,
}


# Called by the plugin manager to get a broker
def get_instance(mod_conf):
    logger.info(
        "[ceilometer broker] Get a ceilometer data module for plugin %s"
        % mod_conf.get_name()
    )
    instance = CeilometerBroker(mod_conf)
    return instance
