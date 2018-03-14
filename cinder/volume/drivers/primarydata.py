# Copyright (c) 2017 Ed Balduf.  All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""
Volume driver for Primary Data DataSphere NFS storage.
"""

import copy
import math
import os
import re
import shutil
import threading
import time
import json

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units
import six
from six.moves import urllib

from cinder import context
from cinder import exception
from cinder.i18n import _, _LE, _LI, _LW
from cinder.image import image_utils
from cinder import utils
from cinder.volume import driver
from cinder.volume.drivers import nfs
from cinder.volume import qos_specs
from cinder.volume import utils as volume_utils
from cinder.volume import volume_types

from pd_client import configuration as pd_config
from pd_client.api_client import ApiClient as pd_ApiClient
import pd_client.apis as pd_apis

datasphere_opts = [
    cfg.StrOpt('datasphere_mgmt_ip',
               help='DataSphere metadata engine IP address.'),
    cfg.StrOpt('datasphere_username',
               default='',
               help='User name for the DataSphere admin.'),
    cfg.StrOpt('datasphere_password',
               default='',
               help='Password for the DataSphere admin.',
               secret=True),
    cfg.BoolOpt('datasphere_debug',
               default=False,
               help='Debug log the API calls.'),
    cfg.BoolOpt('datasphere_debug_api_calls',
               default=False,
               help='Debug log the API calls.'),
    cfg.BoolOpt('datasphere_verify_ssl_cert',
               default=True,
               help='Verify the SSL certificate, or not.')]

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


@six.add_metaclass(utils.TraceWrapperWithABCMetaclass)
class PrimaryDataNfsDriver(
#                      driver.ManageableVD,
#                      driver.CloneableImageVD,
                      driver.SnapshotVD,
                      nfs.NfsDriver):
    """Base class for Primary Data NFS driver for DataSphere."""

    VENDOR = 'PrimaryData Inc.'
    DRIVER_NAME = 'PrimaryData_DataSphere'
    VERSION = '1.0.0'
    DATASPHERE_API_VERSION = 'v1.2'
    DATASPHERE_API_PATH = '/mgmt/%s/rest/' % DATASPHERE_API_VERSION
    DEFAULT_BACKEND_NAME = 'DataSphere'
    DATASPHERE_PDFS_PATH = '/mnt/pdfs'

    def __init__(self, *args, **kwargs):
        self._execute = None
        self._context = None
        self._app_version = kwargs.pop("app_version", "unknown")
        super(PrimaryDataNfsDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(datasphere_opts)
        self.backend_name = self.host.split('@')[1]
        self.debug = self.configuration.safe_get('datasphere_debug')

    def do_setup(self, context):
        super(PrimaryDataNfsDriver, self).do_setup(context)
        self._context = context

        self._default_path = self.configuration.safe_get("datasphere_path_prefix")
        #self._backend_name = (self.configuration.safe_get('share_backend_name')
        #                      or CONF.share_backend_name
        #                      or DEFAULT_BACKEND_NAME)
        self._hostname = self.configuration.safe_get('datasphere_mgmt_ip')
        pd_config.username = self.configuration.safe_get('datasphere_username')
        pd_config.password = self.configuration.safe_get('datasphere_password')
        pd_config.verify_ssl = self.configuration.safe_get('datasphere_verify_ssl_cert')
        pd_config.debug = self.configuration.safe_get('datasphere_debug_api_calls')
        pd_config.host = 'https://%s%s' % (self.configuration.safe_get('datasphere_mgmt_ip'),
                                            self.DATASPHERE_API_PATH)
        token = pd_config.get_basic_auth_token()
        pd_config.api_client = pd_ApiClient()
        pd_config.api_client.set_default_header('Authorization',token)

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        share = snapshot.volume.provider_location.rsplit(':')[1]
        if 'data-portal' in share:
            # on the portal the path is /mnt/data-portal so strip it off
            share = share.rsplit(os.sep)[3:]
        else:
            # on DataSphere the path simply has a '/' at the beginning
            share = share.rsplit(os.sep)[1:]
        base_path = os.path.join(self.DATASPHERE_PDFS_PATH, *share )
        src_path = os.path.join(base_path, 'volume-' + snapshot.volume.id )
        dst_path = os.path.join(base_path, snapshot.name )
        pd_apis.filesnapshots_api.FilesnapshotsApi().clone(src_path, dst_path,_preload_content=False).data
        # work around bug whereby clone doesn't get a copy of permissions
        self._execute('chmod', '0660', dst_path, run_as_root=True)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        try:
            self._delete_file(snapshot.volume_id, snapshot.name)
        except:
            LOG.debug('Snapshot %(snapshot)s not found when attempting to delete.',
                      {'snapshot': snapshot.name})
        return

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot.

        If volume_type extra specs includes 'replication: <is> True'
        the driver needs to create a volume replica (secondary),
        and setup replication between the newly created volume and
        the secondary volume.
        """

        src_provider = snapshot.volume.provider_location
        share = src_provider.rsplit(':')[1]
        volume.provider_location = src_provider
        qos_specs = self._get_volume_qos_specs(volume)
        objective_uuid = self._get_objective_uuid(qos_specs['PD-objective'])
        if 'data-portal' in share:
            # on the portal the path is /mnt/data-portal so strip it off
            share = share.rsplit(os.sep)[3:]
        else:
            # on DataSphere the path simply has a '/' at the beginning
            share = share.rsplit(os.sep)[1:]
        base_path = os.path.join(self.DATASPHERE_PDFS_PATH, *share )
        src_path = os.path.join(base_path, snapshot.name )
        dst_path = os.path.join(base_path, 'volume-' + volume.id )
        pd_apis.filesnapshots_api.FilesnapshotsApi().clone(src_path, dst_path,_preload_content=False).data
        # work around bug whereby clone doesn't get a copy of permissions
        self._execute('chmod', '0660', dst_path, run_as_root=True)

        self._set_objective(*share, file=os.path.join(os.sep, 'volume-' + volume.id ) ,objective_uuid=objective_uuid)
        LOG.debug('Checking file for resize')
        new_size = volume.size
        local_path = self.local_path(volume)
        self._set_rw_permissions(local_path)
        if self._is_file_size_equal(local_path, new_size):
            return { 'provider_location' : src_provider }
        else:
            LOG.info('Resizing file to %sG', new_size)
            image_utils.resize_image(local_path, new_size,
                                     run_as_root=self._execute_as_root)
            if self._is_file_size_equal(local_path, new_size):
                return { 'provider_location' : src_provider }
            else:
                raise exception.InvalidResults(
                    _('Resizing image file failed.'))

    def _delete_file(self, volume_id, file_name):
        volume = self.db.volume_get(self._context, volume_id)
        nfs_share = volume.provider_location

        path = os.path.join(self._get_mount_point_for_share(nfs_share),
                            file_name)
        try:
            self._try_execute('ls', path)
        except processutils.ProcessExecutionError:
            LOG.debug('File %(file_path)s not found when attempting to delete '
                      'from share %(share)s',
                      {'file_path': path})
            return
        self._delete(path)

    def _is_file_size_equal(self, path, size):
        """Checks if file size at path is equal to size."""
        data = image_utils.qemu_img_info(path,
                                         run_as_root=self._execute_as_root)
        virt_size = data.virtual_size / units.Gi
        if virt_size == size:
            return True
        else:
            return False

    def _get_volume_qos_specs(self, volume):
        """Provides extra specs associated with volume."""
        ctxt = context.get_admin_context()
        type_id = volume.get('volume_type_id')
        if type_id is None:
            return {}
        volume_type = volume_types.get_volume_type(ctxt, type_id)
        if volume_type is None:
            return {}
        qos_id = volume_type.get('qos_specs_id', None)
        if qos_id is None:
            return None
        return qos_specs.get_qos_specs(ctxt, qos_id)['specs']

    def _get_objective_uuid(self, name):
        objectives = json.loads(pd_apis.objectives_api.ObjectivesApi().list(_preload_content=False).data)
        for objective in objectives:
            if objective['name'] == name:
                return objective['uoid']['uuid']

    def _set_objective(self, share, file, objective_uuid):
        # note file here is path relative to the share root
        pd_apis.shares_api.SharesApi().profile_set(identifier=share, path=file, objective_identifier=objective_uuid)
