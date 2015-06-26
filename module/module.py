#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# Copyright (C) 2014 - Savoir-Faire Linux inc.
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.

import json
import threading

from shinken.basemodule import BaseModule
from shinken.log import logger
from shinken.misc.perfdata import PerfDatas
from ceilometerclient import client as ceil_client


# Class for the Ceilometer Broker
# Get broks and send them to Ceilometer
class CeilometerBroker(BaseModule):

    def __init__(self, modconf):

        BaseModule.__init__(self, modconf)

        self.auth_url = getattr(modconf, 'auth_url', '')
        self.username = getattr(modconf, 'username', '')
        self.password = getattr(modconf, 'password', '')
        self.tenant_name = getattr(modconf, 'tenant_name', '')

        self._lock = threading.Lock()
        self.buffer = []
        self.ticks = 0
        self.tick_limit = int(getattr(modconf, 'tick_limit', '300'))

        #  This is used to store the OpenStack ressource id of the hosts
        self.host_config = {}

    def extend_buffer(self, other):
        with self._lock:
            self.buffer.extend(other)

    # Called by Broker so we can do init stuff
    # Conf from arbiter!
    def init(self):
        logger.info(
            "[ceilometer broker] Initializing the connection to Ceilometer"
        )

        self.cclient = ceil_client.get_client(2,
                                              os_username=self.username,
                                              os_password=self.password,
                                              os_tenant_name=self.tenant_name,
                                              os_auth_url=self.auth_url)

    def _get_metering_metadata(self, instance_metadata):
        """Given an instance metadata dict, returns metering_metadata"""
        metering_metadata = {}

        for k, v in instance_metadata.items():
            if k.startswith("metering."):
                metering_metadata[k] = v

        return metering_metadata

    def get_check_result_samples(self, perf_data, timestamp, instance_id, tags={}):
        """
        :param perf_data: Perf data of the brok
        :param timestamp: Timestamp of the check result
        :param tags: Tags for the point
        :return: List of perfdata points
        """
        samples = []
        metrics = PerfDatas(perf_data).metrics

        for e in metrics.values():
            sample = {
                'counter_name': "SURVEIL_" + self.illegal_char.sub('_', e.name),
                'counter_type': 'gauge',
                'resource_id': instance_id,
                'counter_unit': 'unknown',
            }

            field_mappings = [
                ('value', 'counter_volume'),
                ('unit', 'counter_unit'),
            ]
            for field_name, sample_field_name in field_mappings:
                value = getattr(e, field_name, None)
                if value is not None:
                    sample[sample_field_name] = value

            sample['resource_metadata'] = tags

            samples.append(sample)

        return samples

    def manage_initial_host_status_brok(self, b):
        data = b.data
        host_name = data['host_name']
        instance_id = data['customs'].get('_OS_INSTANCE_ID', None)
        if instance_id is not None:
            instance_metadata = json.loads(
                data['customs'].get(
                    '_OS_INSTANCE_METADATA',
                    '{}'
                )
            )

            metering_metadata = self._get_metering_metadata(instance_metadata)

            self.host_config[host_name] = {
                '_OS_INSTANCE_ID': instance_id,
                'metering_metadata': metering_metadata,
            }

    # A service check result brok has just arrived,
    # we UPDATE data info with this
    def manage_service_check_result_brok(self, b):
        data = b.data
        host_name = data['host_name']

        if host_name in self.host_config:
            tags = self.host_config[host_name]['metering_metadata']
            tags.update(
                {
                    "host_name": host_name,
                    "service_description": data['service_description'],
                }
            )

            post_data = self.get_check_result_samples(
                perf_data=b.data['perf_data'],
                timestamp=b.data['last_chk'],
                instance_id=self.host_config[host_name]['_OS_INSTANCE_ID'],
                tags=tags
            )

            try:
                logger.debug("[ceilometer broker] Generated samples: %s" % str(post_data))
            except UnicodeEncodeError:
                pass

            self.extend_buffer(post_data)

    # A host check result brok has just arrived, we UPDATE data info with this
    def manage_host_check_result_brok(self, b):
        data = b.data
        host_name = data['host_name']

        if host_name in self.host_config:
            tags = self.host_config[host_name]['metering_metadata']
            tags.update({"host_name": host_name})

            post_data = self.get_check_result_samples(
                perf_data=b.data['perf_data'],
                timestamp=b.data['last_chk'],
                instance_id=self.host_config[host_name]['_OS_INSTANCE_ID'],
                tags=tags
            )

            try:
                logger.debug("[ceilometer broker] Generated samples: %s" % str(post_data))
            except UnicodeEncodeError:
                pass

            self.extend_buffer(post_data)

    def hook_tick(self, brok):

        if self.ticks >= self.tick_limit:
            with self._lock:
                buffer = self.buffer
                self.buffer = []
            logger.error(
                "[ceilometer broker] Buffering ticks exceeded. "
                "Freeing buffer, lost %d entries" % len(buffer)
            )
            self.ticks = 0

        if len(self.buffer) > 0:
            with self._lock:
                samples_to_send = self.buffer
                self.buffer = []
            try:
                try:
                    logger.debug("[ceilometer broker] Writing samples: %s" % str(samples_to_send))
                except UnicodeEncodeError:
                    pass

                #  Send all samples
                for sample in samples_to_send:
                    self.cclient.samples.create(**sample)
                    samples_to_send.remove(sample)

            except Exception as e:
                self.ticks += 1

                #  Sample might not exist at this point
                try:
                    logger.error("[ceilometer broker] Could not send sample: %s" % sample)
                except Exception:
                    pass

                logger.error("[ceilometer broker] %s" % e)
                logger.error(
                    "[ceilometer broker] Sending data Failed. "
                    "Buffering state : %s / %s"
                    % (self.ticks, self.tick_limit)
                )
                with self._lock:
                    samples_to_send.extend(self.buffer)
                    self.buffer = samples_to_send
            else:
                self.ticks = 0
