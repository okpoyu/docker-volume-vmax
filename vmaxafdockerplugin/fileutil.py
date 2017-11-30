from sh import blkid
from sh import mkfs
from sh import mkdir
from sh import mount
from sh import umount
import subprocess
from sh import rm
from sh import iscsiadm
import os
import pyudev
import six
from oslo_log import log as logging

from twisted.python.filepath import FilePath

LOG = logging.getLogger(__name__)


def has_filesystem(path):
    try:
        #ToDo Parse the output to get exact information, example
        #/dev/sdj: UUID="0cb38451-c366-46e8-a7a4-5e19bd9257f0" VERSION="1.0" TYPE="ext4" USAGE="filesystem"
        if blkid("-p", "-u", "filesystem", path) == '':
            return False
    except:
        #msg = 'Error getting filesystem existatnce'
        #LOG.error(msg)
        return False

    return True


def create_filesystem(path, fs_type):
    try:
        mkfs("-t", fs_type, "-F", path)
    except:
        LOG.error("Create file system failed")
    return True


def mkdir_for_mounting(path):
    #path = FilePath(path)
    if os.path.isdir(path):
        msg = ("Path already exists, no action taken: %s", path)
        LOG.debug(msg)
        return path
    else:
        try:
            mkdir("-p", path)
        except:
            LOG.error("Make directory failed exception is :%s", path)

        return path


def mount_dir(src, tgt):
    try:
        mount(src, tgt)
    except:

        msg = ('Mount exception is : Mount failure')
        LOG.error(msg)

    return True


def umount_dir(tgt):

    result = subprocess.Popen(["mountpoint", "-q", tgt])

    # we must explictly wait for the process to finish.
    # Otherwise, we do not get the correct result
    result.wait()
    if result.returncode == 0:
        try:
            umount("-l", tgt)
        except Exception as ex:
            msg = (('exception is : %s'), six.text_type(ex))
            LOG.error(msg)
    return True


def remove_dir(tgt):
    path = FilePath(tgt)
    if path.exists:
        try:
            rm("-rf", tgt)
        except:
            msg = (('exception is : %s'))
            LOG.error(msg)
            #raise exception.HPEPluginRemoveDirException(reason=msg)
    return True


def remove_file(tgt):
    path = FilePath(tgt)
    if path.exists:
        try:
            rm(tgt)
        except:
            msg = (('exception is : %s'))
            LOG.error(msg)
            #raise exception.HPEPluginRemoveDirException(reason=msg)
    return True


def get_vmax_device_path(symm_id, device_id, target):
    if target:
        _login_to_target(target)
        _rescan_scsi_bus()
    encoded_str = ""
    ret_path = None
    for c in device_id:
        encoded_str += c.encode("hex")
    context = pyudev.Context()
    for device in context.list_devices(MAJOR='8'):
        lun_naa = device.get('ID_SERIAL_SHORT')
        if device.device_type == 'disk' and lun_naa:
            ret_dev = lun_naa[-10:]
            ret_symm = lun_naa[8:-12]
            if str(ret_dev) == encoded_str and ret_symm == symm_id:
                ret_path = device.device_node
                break
    return ret_path


def _rescan_scsi_bus():
    """
        Rescan for new  iSCSI devices

    """
    try:
        iscsiadm("-m", "session", "--rescan")
    except:
        LOG.error("iscsiadm rescan: error")


def _login_to_target(target):

    try:
        iscsiadm("-m", "discovery", "-t", "sendtargets", "-p", target)
    except:
        LOG.error("iscsiadm discovery: error")
    try:
        iscsiadm("-m", "node", "-l")
    except:
        LOG.warn("iscsiadm login failure, initiator may be already logged in" )


def get_initiator():
    try:
        output = subprocess.check_output(
            ["sudo", "cat", "/etc/iscsi/initiatorname.iscsi"])
        for l in output.split('\n'):
            if l.startswith('InitiatorName='):
                return l[l.index('=') + 1:].strip()
    except subprocess.CalledProcessError as e:
        LOG.error("Unable to get Iqn:\n", e.returncode)


def get_wwpns():
    try:
        wwpns = []
        output = subprocess.check_output(["systool", "-c", "fc_host", "-v"])
        for l in output.split('\n'):
            if l.strip().startswith('port_name'):
                wwpns.append(l[l.index('x') + 1:-1])
        return wwpns
    except subprocess.CalledProcessError as e:
        print("Unable to get WWPNS:\n", e.returncode)