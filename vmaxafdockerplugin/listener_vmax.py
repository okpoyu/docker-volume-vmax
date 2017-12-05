import json
import os
import sys

from flask import Flask
from flask import request
from oslo_config import cfg
from oslo_log import helpers
from oslo_log import log as logging

from vmaxafdockerplugin import fileutil
from config import setupcfg
from vmaxafdockerplugin import vmax_plugin
from vmaxafdockerplugin.volume_ops import volume_ops

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
DOMAIN = "VMAX_Driver"
vmax_config_file = "/etc/vmax/vmax.conf"
vmax_plugin_dir = "/usr/lib/docker/plugins/"
vmax_plugin_file = vmax_plugin_dir + "vmaxAF.json"

logging.register_options(CONF)

listener = Flask(DOMAIN)

CONF.register_opts(setupcfg.host_opts)

try:
    vmax_config_file = sys.argv[1]
except IndexError:
    if not os.path.isfile(vmax_config_file):
        LOG.error('Configuration file vmax.conf not found. Please create file '
                  'using vmax.conf.sample...terminating')
        sys.exit(1)

CONFIG_FILE = os.path.abspath(vmax_config_file)
CONFIG = ['--config-file', CONFIG_FILE]

CONF(CONFIG)
backend_conf_list = []
backend_dict = {}

if not os.path.exists(vmax_plugin_dir):
    os.makedirs(vmax_plugin_dir)
filename = os.path.abspath(vmax_plugin_file)
lines = ['{', '\"Name\": \"vmaxAF\",', (
    '\"Addr\": \"http://127.0.0.1:%s\"' % CONF.listener_port_number), '}']
with open(filename, 'w+') as f:
    f.write('\n'.join(lines))
    f.seek(0)


class Configuration(object):
    def __init__(self, volume_opts, config_group=None):
        """Initialize configuration.
        This takes care of grafting the implementation's config
        values into the config group
        """
        self.config_group = config_group

        # set the local conf so that __call__'s know what to use
        if self.config_group:
            self._ensure_config_values(volume_opts)
            self.local_conf = CONF._get(self.config_group)
        else:
            self.local_conf = CONF

    def _ensure_config_values(self, volume_opts):
        CONF.register_opts(volume_opts, group=self.config_group)

    def append_config_values(self, volume_opts):
        self._ensure_config_values(volume_opts)

    def safe_get(self, value):
        try:
            return self.__getattr__(value)
        except cfg.NoSuchOptError:
            return None

    def __getattr__(self, value):
        return getattr(self.local_conf, value)


if CONF.enabled_backends:
    if not (CONF.default_backend and
            CONF.default_backend in CONF.enabled_backends):
        CONF.set_override(name='default_backend',
                          override=CONF.enabled_backends[0]),
    for backend in filter(None, CONF.enabled_backends):
        backend_conf_list.append(Configuration(
            setupcfg.volume_opts, config_group=backend))
logging.setup(CONF, DOMAIN)
for backend_conf in backend_conf_list:
    array = backend_conf.safe_get('array')
    u4v_ip = backend_conf.safe_get('rest_server_ip')
    user = backend_conf.safe_get('rest_user_name')
    password = backend_conf.safe_get('rest_password')
    protocol = backend_conf.safe_get('storage_protocol')
    vmax = vmax_plugin.VmaxAf(u4v_ip, user, password, array=array,
                              protocol=protocol)
    backend_dict[backend_conf.safe_get('volume_backend_name')] = vmax


@listener.route('/Plugin.Activate', methods=['POST'])
def activate():
    LOG.info('Plugin Activate')
    return json.dumps({u"Implements": [u"VolumeDriver"]})


@listener.route('/VolumeDriver.List', methods=['POST'])
def list_volumes():
    LOG.info('List request')
    volume_list = []
    volumes_local = volume_ops.get_volume_list()
    for vol in volumes_local:
        volume = {'Name': vol}
        volume_list.append(volume)
    response = json.dumps({u"Err": '', u"Volumes": volume_list})
    LOG.info('List Response ' + response)
    return response


@listener.route('/VolumeDriver.Get', methods=['POST'])
def get():
    err = ''
    request_data = log_input('Get', request)
    volume_name = request_data['Name']
    target_host_name = request.remote_addr
    volume_info = volume_ops.get_volume(volume_name)
    if volume_info:
        mountpoint = volume_ops.get_mount_path(volume_name, target_host_name)
        data = {'Name': volume_name,
                'Mountpoint': mountpoint,
                'Status': {}}

        response = json.dumps({u"Err": err, u"Volume": data})
        LOG.debug('Get Response = {0}'.format(response))
    else:
        err = ("Get volume %s failed, unable to find volume", volume_name)
        LOG.error(err)
        response = json.dumps({u"Err": err})
    return response


@listener.route('/VolumeDriver.Create', methods=['POST'])
def create():
    """
    1. Check if the given volume name exists, if not, create the volume;
    2. Check if the given volume has been exported to this current host,
       if not, export it to the host, then mark it as exported to the host.
    The two steps have to be separated since for the second host maybe only
    the second step is needed.

    Returns: The error message needed by Docker daemon.
    """
    request_data = log_input('Create', request)
    LOG.debug('Create request.Opts = {0}'.format(request_data['Opts']))
    volume_name = request_data['Name']
    volume_opts = request_data['Opts']
    target_host_name = request.remote_addr
    LOG.debug('Target host address = {0}'.format(target_host_name))
    valid_volume_create_opts = ['size', 'backend-name']
    if not volume_name:
        msg = (
            "create volume failed, error : name not provided %s",
            volume_name)
        LOG.error(msg)
        return json.dumps({u"Err": msg})
    # 1. Check if the given volume name exists, if not, create the volume.
    existing_volume = volume_ops.get_volume(volume_name)
    if not existing_volume:
        if volume_opts:
            for key, value in volume_opts.items():
                if key not in valid_volume_create_opts:
                    msg = (('create volume failed, error is: '
                            '%(key)s is not a valid option. Valid options '
                            'are: %(valid)s') %
                           {'key': key,
                            'valid': valid_volume_create_opts, })
                    LOG.error(msg)
                    return json.dumps({u"Err": msg})
                else:
                    if key == 'backend-name':
                        if value in CONF.enabled_backends:
                            volume_opts[key] = value
                        else:
                            msg = (('create volume failed, error is: '
                                    '%(value)s is not a valid backend. '
                                    'Valid options are: %(valid)s') %
                                   {'value': value,
                                    'valid': CONF.enabled_backends, })
                            LOG.error(msg)
                            return json.dumps({u"Err": msg})
        else:
            volume_opts = {}
        if 'size' not in volume_opts:
            LOG.debug(
                "Volume Size NOT specified, using default")
            volume_opts['size'] = CONF.default_volume_size
        if 'backend-name' not in volume_opts:
            LOG.debug(
                "Volume backend NOT specified, using default")
            volume_opts['backend-name'] = CONF.default_backend
        group_conf = None

        for backend_config in backend_conf_list:
            if (backend_config.safe_get('volume_backend_name') == (
                    volume_opts['backend-name'])):
                group_conf = backend_conf
                break
        if group_conf is not None:
            volume_opts['service_level'] = group_conf.safe_get('service_level')
            volume_opts['workload'] = group_conf.safe_get('workload')
            volume_opts['srp'] = group_conf.safe_get('srp')
        vmax = backend_dict[volume_opts['backend-name']]
        res = vmax.create_volume(volume_name, volume_opts)
        if res['volume_identifier'] == volume_name:
            LOG.info("Volume create successful ", res)
            volume = {'name': volume_name,
                      'volume_id': res['volumeId'],
                      'wwn': res['wwn'],
                      'formatted': False,
                      'exported': {},
                      'mounted': {},
                      'parameters': {},
                      'backend-name': volume_opts['backend-name']}
            volume_ops.set_volume(volume_name, volume)
            return json.dumps({u"Err": ''})
        else:
            msg = res
            return json.dumps({u"Err": msg})


@listener.route('/VolumeDriver.Path', methods=['POST'])
def path():
    """
    Query the mount path of the volume on the target host and return.

    Returns: Mount path needed by Docker daemon.
    """

    request_data = log_input('Path', request)
    volume_name = request_data['Name']
    target_host_name = request.remote_addr
    LOG.debug('Target host address = {0}'.format(target_host_name))
    mount_path = volume_ops.get_mount_path(volume_name, target_host_name)
    return json.dumps({u"Err": None, u"Mountpoint": mount_path})


@listener.route('/VolumeDriver.Capabilities', methods=['POST'])
def capabilities():
    LOG.debug('Capabilities request')
    scope = 'local'
    data = {
        "Capabilities": {
            "Scope": scope
        }
    }
    response = json.dumps(data)
    return response


@listener.route('/VolumeDriver.Mount', methods=['POST'])
def mount():
    """
    Check if the given volume has been mounted to this current host, if not,
    mount it to the host, when doing so also check if the volume is
    formatted, if not, use "format" option in the "mount" operation. When
    the mount operation succeeds, mark it as mounted to the host.

    Returns: Mount path needed by Docker daemon.
    """
    request_data = log_input('Mount', request)
    volume_name = request_data['Name']
    target_host_name = request.remote_addr
    LOG.debug('Target host address = {0}'.format(target_host_name))
    volume = volume_ops.get_volume(volume_name)
    mount_path = volume_ops.get_mount_path(volume_name, target_host_name)
    disk_device = None
    if mount_path:
        # If the volume is already mounted to the target host, just increase
        # counter.
        volume['mounted'][target_host_name]['count'] += 1
        volume_ops.set_volume(volume_name, volume)
    else:
        # Else it means it's the first time to mount the volume to the target
        vmax = backend_dict[volume['backend-name']]
        group_conf = None
        for backend_config in backend_conf_list:
            if (backend_config.safe_get('volume_backend_name') == (
                    volume['backend-name'])):
                group_conf = backend_conf
                break
        target_ip_list = vmax.attach_volume(
            volume_name, volume["volume_id"], group_conf)
        if not target_ip_list and vmax.protocol.lower() == 'iscsi':
            error_msg = "Error mounting volume."
            LOG.error(error_msg)
            return json.dumps({u"Err": error_msg})
        mount_point = CONF.mount_path + volume_name
        volume_id = volume['volume_id']
        symm_id = group_conf.safe_get('array')
        if vmax.protocol.lower() == 'iscsi':
            for target_ip in target_ip_list:
                LOG.debug('Target ip:%s', target_ip)
                disk_device = fileutil.get_vmax_device_path(
                    symm_id, volume_id, target_ip)
                if disk_device:
                    break
            if disk_device is None:
                error_msg = "Volume could not be discoved on host"
                return json.dumps({u"Err": error_msg})
        else:
            disk_device = fileutil.get_vmax_device_path(symm_id, volume_id, "")
        # Check if filesystem exists, create one if not
        if fileutil.has_filesystem(disk_device) is False:
            LOG.debug('File system does not exist on %s', disk_device)
            if vmax.protocol.lower() == 'iscsi':
                fileutil.create_filesystem(disk_device, 'ext4')
            else:
                fileutil.create_filesystem(disk_device, 'ext3')
        else:
            msg = ('Found File system on %s', disk_device)
            LOG.debug(msg)
        # Create mountpoint
        fileutil.mkdir_for_mounting(mount_point)
        # Mount
        fileutil.mount_dir(disk_device, mount_point)
        # Update record
        volume['formatted'] = True
        volume['mounted'][target_host_name] = {
            'mount_point': mount_point, 'count': 1}
        volume_ops.set_volume(volume_name, volume)
        mount_path = volume_ops.get_mount_path(volume_name, target_host_name)
        LOG.info("Volume Mount successful. Mount Path from data file %s",
                 mount_path)
    return json.dumps({u"Err": '', u"Mountpoint": mount_path})


@listener.route('/VolumeDriver.Unmount', methods=['POST'])
def unmount():
    """
    Unmount the volume from the target host.

    Returns: The error message needed by Docker daemon.
    """
    request_data = log_input('UnMount', request)
    volume_name = request_data['Name']
    umount_request_id = request_data['ID']
    target_host_name = request.remote_addr
    LOG.debug('Target host address = {0}'.format(target_host_name))
    volume = volume_ops.get_volume(volume_name)
    if volume['mounted'][target_host_name]:
        mount_path = volume_ops.get_mount_path(volume_name, target_host_name)
        if volume['mounted'][target_host_name]['count'] > 1:
            # There are multiple mounts so just decrement the count of mounts
            volume['mounted'][target_host_name]['count'] -= 1
            volume_ops.set_volume(volume_name, volume)
            LOG.debug(
                'Mount count for host %s decremented by one, Request ID: %s',
                target_host_name, umount_request_id)
            return json.dumps({u"Err": ''})
        elif volume['mounted'][target_host_name]['count'] == 1:
            # Unmount  it
            fileutil.umount_dir(mount_path)
            # remove directory
            fileutil.remove_dir(mount_path)
            # detach volume
            vmax = backend_dict[volume['backend-name']]
            group_conf = None
            for backend_config in backend_conf_list:
                if (backend_config.safe_get('volume_backend_name') == (
                        volume['backend-name'])):
                    group_conf = backend_conf
                    break
            vmax.detach_volume(volume_name, volume["volume_id"], group_conf)
            # Udate record in data.json
            del volume['mounted'][target_host_name]
            volume_ops.set_volume(volume_name, volume)
            LOG.debug('Mount removedfor  host %s, Request ID: %s',
                      target_host_name, umount_request_id)
            return json.dumps({u"Err": ''})
    else:
        return json.dumps({u"Err": ''})


@listener.route('/VolumeDriver.Remove', methods=['POST'])
def remove():
    """
    1. Unexport the volume from the target host;
    2. Check if the current target host is the last target host of this volume,
    if yes, remove the volume.

    Returns: The error message needed by Docker daemon.
    """
    request_data = log_input('Remove', request)
    volume_name = request_data['Name']
    target_host_name = request.remote_addr
    LOG.debug('Target host address = {0}'.format(target_host_name))

    volume = volume_ops.get_volume(volume_name)
    vmax = backend_dict[volume['backend-name']]
    msg = ''
    if volume:
        res = vmax.remove_volume(volume_name, volume_id=volume["volume_id"])
        if res:
            volume_ops.remove_volume(volume_name)
            LOG.info("Volume %s removed successfully", volume_name)
        else:
            msg = "Unable to remove volume"
    else:
        msg = 'Volume does not exist in local records'
    if msg:
        LOG.error(msg)
    return json.dumps({u"Err": msg})


def log_input(operation, req):
    LOG.info('In VolumeDriver.%(operation)s', {'operation': operation})
    request_data = req.get_json(force=True)
    LOG.debug("%(operation)s request = %(request_data)s.",
              {'operation': operation, 'request_data': request_data})
    LOG.debug('%(operation)s request.Name = %(request_data)s',
              {'operation': operation, 'request_data': request_data['Name']})
    if operation.lower() in ['mount', 'unmount']:
        LOG.debug('%(operation)s request.ID = %(request_data)s',
                  {'operation': operation, 'request_data': request_data['ID']})

    return request_data


@helpers.log_method_call
def main():
    LOG.info('Starting server...')
    LOG.info('Listening on port: ' + str(CONF.listener_port_number))
    listener.run('0.0.0.0', CONF.listener_port_number, debug=CONF.debug)


if __name__ == '__main__':
    main()
