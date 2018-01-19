import hashlib
import platform
import random

import six
import time
import PyU4V
from PyU4V.utils import exception as pyU4V_exception
from oslo_log import log as logging

import exception
import fileutil

LOG = logging.getLogger(__name__)

ISCSI = 'iscsi'
FC = 'fc'
ARRAY = 'array'
SLO = 'service_level'
WORKLOAD = 'workload'
SRP = 'srp'
DEVICE_ID = 'device_id'
VOL_NAME = 'volume_name'
SG_NAME = 'storagegroup_name'
MV_NAME = 'maskingview_name'
IG_NAME = 'init_group_name'
PARENT_SG_NAME = 'parent_sg_name'
PORTGROUPNAME = 'port_group_name'
CONNECTOR = 'connector'


class VmaxAf:
    """
    This class does provisioning operations using PyU4V 
    PyU4V.conf contains Unisphere configuration details
    """

    def __init__(self, u4v_ip=None, user=None, password=None, port=8443,
                 sg=None, array=None, protocol=ISCSI):
        self.user = user
        self.password = password
        self.U4V = u4v_ip
        self.port = port
        self.sg_id = sg
        self.array = array

        if protocol is None or protocol.lower() not in [ISCSI, FC]:
            self.protocol = ISCSI
        else:
            self.protocol = protocol
        self.REST = PyU4V.RestFunctions(
            username=user, password=password, server_ip=u4v_ip, port=port,
            array_id=array, verify=False)

    def remove_volume(self, volume_name, volume_id):
        """
        Args:
            volume_name: The volume to be removed.
            target_host_name: The target host of the volume(
            This is not yet implemented)
        Returns: The volume info object being removed or Error
        :param volume_name:
        :param volume_id:
        """
        volume_info = self.REST.get_volume(volume_id)
        if volume_info is None:
            msg = ('VMAX device ID for volume ' +
                   volume_name + ' Could not be found')
            LOG.msg(msg)
            return volume_name

        # Remove volume from SG
        LOG.debug(volume_info)
        if volume_info['num_of_storage_groups'] > 0:
            for sg_id in volume_info['storageGroupId']:
                LOG.debug(
                    "volume %s belongs to storage group %s "
                    "removing from SG", volume_id, sg_id)
                self.REST.remove_vol_from_storagegroup(sg_id, volume_id)

        LOG.info("Deleting volume  with volume_id: %s", volume_id)
        volume_info = self.REST.get_volume(volume_id)
        if volume_info['num_of_storage_groups'] == 0:
            try:
                self.REST.deallocate_volume(volume_id)
                self.REST.rename_volume(volume_id, None)
            except Exception as e:
                LOG.debug('Deallocate volume failed with %(e)s.'
                          'Attempting delete.', {'e': e})
                try:
                    self.REST.delete_volume(volume_id)
                except Exception:
                    pass

        return True

    def find_ips(self, port_group):
        ips = []
        ports = self.REST.get_ports_from_pg(port_group)
        for port in ports:
            LOG.debug(port)
            ip = self._get_ip(port)
            ips.extend(ip)
        return ips

    def _get_ip(self, port):
        """Get ip and iqn from the director port.

        :param port: the director port on the array
        :returns: ip_and_iqn - dict
        """
        ip_list = []

        ip_addresses, iqn = self.REST.get_iscsi_ip_address_and_iqn(port)
        if ip_addresses:
            for ip in ip_addresses:
                ip_list.append(ip)
        return ip_list

    def create_volume(
            self, volume_name, volume_opts):
        """Create a volume.

        :param volume_name: the volume name
        :param volume_opts: extra specifications
        :returns: int -- return code
        :returns: dict -- volume_dict
        :raises: VolumeBackendAPIException:
        """
        is_valid_slo, is_valid_workload = self.verify_slo_workload(
            volume_opts[SLO], volume_opts[WORKLOAD])

        if not is_valid_slo or not is_valid_workload:
            exception_message = (
                ("Either SLO: %(slo)s or workload %(workload)s is invalid. "
                 "Examine previous error statement for valid values.")
                % {'slo': volume_opts[SLO],
                   'workload': volume_opts[WORKLOAD]})
            raise exception.VMAXPluginException(exception_message)

        LOG.debug("Create Volume: %(volume)s  Srp: %(srp)s Array: %(array)s "
                  "Size: %(size)s",
                  {'volume': volume_name, 'srp': volume_opts['srp'],
                   'array': self.array, 'size': volume_opts['size']})

        storagegroup_name = self.get_or_create_default_storage_group(
            volume_opts[SRP], volume_opts[SLO], volume_opts[WORKLOAD])
        try:
            _, _, device_id = self.REST.create_volume_from_sg_return_dev_id(
                volume_name, storagegroup_name, volume_opts['size'])
        except Exception:
            # if the volume create fails, check if the
            # storage group needs to be cleaned up
            exception_message = ("Create volume failed. Checking if "
                                 "storage group cleanup necessary...")
            num_vol_in_sg = self.REST.get_num_vols_in_sg(storagegroup_name)
            if num_vol_in_sg == 0:
                LOG.debug("There are no volumes in the storage group "
                          "%(sg_id)s. Deleting storage group.",
                          {'sg_id': storagegroup_name})
                self.REST.delete_sg(storagegroup_name)
            raise exception.VMAXPluginException(exception_message)

        volume_info = self.REST.get_volume(device_id)
        LOG.debug("Volume info is %s" % volume_info)

        return volume_info

    def verify_slo_workload(self, slo, workload):
        is_valid_slo = False
        is_valid_workload = False
        valid_workloads = self.REST.get_workload_settings(self.array)
        valid_slos = self.REST.get_slo_list()

        if (workload in valid_workloads) or (workload is None):
            is_valid_workload = True
        else:
            LOG.debug(("Workload: %s is not valid. Valid values are "
                      "%s") % (workload, valid_workloads))
        if (slo in valid_slos) or (slo is None):
            is_valid_slo = True
        else:
            LOG.debug(("SLO: %s is not valid. Valid values are "
                       "%s" % (slo, valid_slos)))
        return is_valid_slo, is_valid_workload

    def get_or_create_default_storage_group(self, srp, slo, workload):
        """Get or create a default storage group.

        :param srp: the SRP name
        :param slo: the SLO
        :param workload: the workload
        :returns: storagegroup_name
        :raises: VolumeBackendAPIException
        """
        storagegroup, storagegroup_name = (
            self.get_vmax_default_storage_group(srp, slo, workload))
        if storagegroup is None:
            self.REST.create_storage_group(
                srp, storagegroup_name, slo, workload)
        else:
            # Check that SG is not part of a masking view
            LOG.debug("Using existing default storage group")
            masking_views = self.REST.get_masking_views_from_storage_group(
                storagegroup_name)
            if masking_views:
                exception_message = (
                    ("Default storage group %(sg_name)s is part of masking "
                     "views %(mvs)s. Please remove it from all masking views")
                    % {'sg_name': storagegroup_name,
                       'mvs': masking_views})
                raise exception.VMAXPluginException(exception_message)

        return storagegroup_name

    def get_vmax_default_storage_group(self, srp, slo, workload):
        """Get the default storage group.

        :param srp: the pool name
        :param slo: the SLO
        :param workload: the workload
        :returns: the storage group dict (or None), the storage group name
        """
        if slo and workload:
            prefix = ("DK-%(srpName)s-%(slo)s-%(workload)s"
                      % {'srpName': srp, 'slo': slo, 'workload': workload})

        else:
            prefix = "DK-no_SLO"

        storage_group_name = ("%(prefix)s-SG" % {'prefix': prefix})
        storagegroup = self.REST.get_storage_group(storage_group_name)
        return storagegroup, storage_group_name

    def attach_volume(self, volume_name, device_id, group_conf):
        target_ip_list = []
        masking_view_dict = self._populate_masking_dict(
            volume_name, device_id, group_conf)
        _, default_sg_name = self.get_vmax_default_storage_group(
            masking_view_dict[SRP], masking_view_dict[SLO],
            masking_view_dict[WORKLOAD])
        error_message = self.get_or_create_masking_view(
            masking_view_dict, default_sg_name)
        if not error_message and self.protocol.lower() == ISCSI:
            target_ip_list = self.find_ips(masking_view_dict[PORTGROUPNAME])
        return target_ip_list

    def get_or_create_masking_view(self, masking_view_dict, default_sg_name):
        """Retrieve an existing masking view or create a new one.

        :param masking_view_dict: the masking view dict
        :param default_sg_name: the name of the default sg
        :returns: error message
        """
        masking_view_name = masking_view_dict[MV_NAME]

        masking_view_details, _ = self.REST.get_masking_view(masking_view_name)
        if not masking_view_details:
            error_message = self._create_new_masking_view(
                masking_view_dict, masking_view_name, default_sg_name)

        else:
            storagegroup_name, error_message = (
                self._validate_existing_masking_view(
                    masking_view_dict, masking_view_name, default_sg_name))

        return error_message

    def _create_new_masking_view(
            self, masking_view_dict, masking_view_name, default_sg_name):
        """Create a new masking view.

        :param masking_view_dict: the masking view dict
        :param masking_view_name: the masking view name
        :param default_sg_name: the name of the default sg
        :returns: error_message
        """
        init_group_name = masking_view_dict[IG_NAME]
        parent_sg_name = masking_view_dict[PARENT_SG_NAME]
        storagegroup_name = masking_view_dict[SG_NAME]
        connector = masking_view_dict[CONNECTOR]
        port_group_name = masking_view_dict[PORTGROUPNAME]
        LOG.debug("Port Group in masking view operation: %(port_group_name)s.",
                  {'port_group_name': port_group_name})
        # get or create parent sg
        error_message = self._get_or_create_storage_group(
            masking_view_dict, parent_sg_name, parent=True)
        if error_message:
            return error_message

        # get or create child sg
        error_message = self._get_or_create_storage_group(
            masking_view_dict, storagegroup_name)
        if error_message:
            return error_message

        portgroup = self.REST.get_portgroups(portgroup_id=port_group_name)
        if portgroup is None:
            msg = ("Cannot get port group: %(portgroup)s from the array "
                   "%(array)s. Portgroups must be pre-configured - please "
                   "check the array."
                   % {'portgroup': port_group_name, 'array': self.array})
            error_message = msg
            LOG.error(error_message)
            return error_message

        init_group_name, error_message = (self._get_or_create_initiator_group(
            init_group_name, connector))
        if error_message:
            return error_message

        # Only after the components of the MV have been validated,
        # move the volume from the default storage group to the
        # masking view storage group. This is necessary before
        # creating a new masking view.
        error_message = self._move_vol_from_default_sg(
            masking_view_dict[DEVICE_ID],
            masking_view_dict[VOL_NAME], default_sg_name,
            storagegroup_name)
        if error_message:
            return error_message

        error_message = self._check_add_child_sg_to_parent_sg(
            storagegroup_name, parent_sg_name)
        if error_message:
            return error_message
        try:
            self.REST.create_masking_view_existing_components(
                port_group_name, masking_view_name, storagegroup_name,
                host_name=init_group_name)
        except Exception as e:
            error_message = ("Exception creating masking view Exception "
                             "received was %(e)s" % six.text_type(e))
        return error_message

    def _get_or_create_initiator_group(self, init_group_name, connector):
        """Retrieve or create an initiator group.

        :param init_group_name: the name of the initiator group
        :returns: name of the initiator group -- string, msg
        """
        initiator_names = self.find_initiator_names(connector)
        msg = None
        LOG.debug("The initiator name(s) are: %(initiatorNames)s.",
                  {'initiatorNames': initiator_names})
        found_init_group = self._find_initiator_group(initiator_names)

        # If you cannot find an initiator group that matches the connector
        # info, create a new initiator group.
        if found_init_group is None:
            self.REST.create_host(
                init_group_name, initiator_list=initiator_names, async=True)
            LOG.debug("Created new initiator group name: %(init_group_name)s.",
                      {'init_group_name': init_group_name})
            found_init_group = init_group_name
        else:
            LOG.debug("Using existing initiator group name: "
                      "%(init_group_name)s.",
                      {'init_group_name': found_init_group})

        if found_init_group is None:
            msg = ("Cannot get or create initiator group: "
                   "%(init_group_name)s. "
                   % {'init_group_name': init_group_name})
            LOG.error(msg)

        return found_init_group, msg

    def find_initiator_names(self, connector):
        """Check the connector object for initiators(ISCSI) or wwpns(FC).

        :param connector: the connector object
        :returns: list -- list of found initiator names
        :raises: VolumeBackendAPIException
        """
        found_initiator_names = []
        name = 'initiator name'
        if self.protocol.lower() == ISCSI and connector['initiator']:
            found_initiator_names.append(connector['initiator'])
        elif self.protocol.lower() == FC:
            if 'wwpns' in connector and connector['wwpns']:
                for wwn in connector['wwpns']:
                    found_initiator_names.append(wwn)
                name = 'world wide port names'
            else:
                msg = (("FC is the protocol but wwpns are not supplied by "
                        "OpenStack."))
                #LOG.error(msg)
                raise exception.VMAXPluginException(msg)

        if not found_initiator_names:
            msg = ("Error finding %(name)s." % {'name': name})
            #LOG.error(msg)
            raise exception.VMAXPluginException(msg)

        LOG.debug("Found %(name)s: %(initiator)s.",
                  {'name': name,
                   'initiator': found_initiator_names})

        return found_initiator_names

    def _find_initiator_group(self, initiator_names):
        """Check to see if an initiator group already exists.

        NOTE:  An initiator/wwn can only belong to one initiator group.
        If we were to attempt to create one with an initiator/wwn that is
        already belonging to another initiator group, it would fail.
        :param initiator_names: the list of initiator names
        :returns: initiator group name -- string or None
        """
        ig_name = None
        init_list = self.REST.get_in_use_initiator_list_from_array()
        for initiator in initiator_names:
            found_init = [init for init in init_list if initiator in init]
            if found_init:
                ig_name = self.REST.get_initiator_group_from_initiator(
                    found_init[0])
                break
        return ig_name

    def _move_vol_from_default_sg(
            self, device_id, volume_name, default_sg_name,
            dest_storagegroup):
        """Get the default storage group and move the volume.

        :param device_id: the device id
        :param volume_name: the volume name
        :param default_sg_name: the name of the default sg
        :param dest_storagegroup: the destination storage group
        :returns: msg
        """
        msg = None
        check_vol = self.REST.is_volume_in_storagegroup(
            device_id, default_sg_name)
        if check_vol:
            num_vol_in_sg = self.REST.get_num_vols_in_sg(default_sg_name)
            LOG.debug("There are %(num_vol)d volumes in the "
                      "storage group %(sg_name)s.",
                      {'num_vol': num_vol_in_sg,
                       'sg_name': default_sg_name})
            _, status_code = self.REST.move_volume_between_storage_groups(
                device_id, default_sg_name, dest_storagegroup)

            if num_vol_in_sg == 1:
                num_vol = self.REST.get_num_vols_in_sg(default_sg_name)
                count = 0
                while num_vol == 1 and count < 6:
                    time.sleep(5)
                    num_vol = self.REST.get_num_vols_in_sg(default_sg_name)
                    count += 1

                if num_vol < 1:
                    # Last volume in the storage group - delete sg.
                    self.REST.delete_sg(default_sg_name)

        else:
            LOG.warning(
                "Volume: %(volume_name)s does not belong "
                "to default storage group %(default_sg_name)s.",
                {'volume_name': volume_name,
                 'default_sg_name': default_sg_name})
            msg = self._check_adding_volume_to_storage_group(
                device_id, dest_storagegroup, volume_name)

        return msg

    def _check_adding_volume_to_storage_group(
            self, device_id, storagegroup_name, volume_name):
        """Check if a volume is part of an sg and add it if not.

        :param device_id: the device id
        :param storagegroup_name: the storage group name
        :param volume_name: volume name
        :returns: msg
        """
        msg = None
        if self.REST.is_volume_in_storagegroup(device_id, storagegroup_name):
            LOG.debug("Volume: %(volume_name)s is already part "
                      "of storage group %(sg_name)s.",
                      {'volume_name': volume_name,
                       'sg_name': storagegroup_name})
        else:
            try:
                self.REST.add_existing_vol_to_sg(
                    storagegroup_name, device_id, async=True)
            except Exception as e:
                msg = ("Exception adding volume %(vol)s to %(sg)s. "
                       "Exception received was %(e)s."
                       % {'vol': volume_name, 'sg': storagegroup_name,
                          'e': six.text_type(e)})
                LOG.error(msg)
        return msg

    def _get_or_create_storage_group(
            self, masking_view_dict, storagegroup_name, parent=False):
        """Get or create a storage group for a masking view.

        :param masking_view_dict: the masking view dict
        :param storagegroup_name: the storage group name
        :param parent: flag to indicate if this a parent storage group
        :returns: msg -- string or None
        """
        msg = None
        srp = masking_view_dict[SRP]
        workload = masking_view_dict[WORKLOAD]
        if parent:
            slo = None
        else:
            slo = masking_view_dict[SLO]
        storagegroup = self.REST.get_storage_group(storagegroup_name)
        if storagegroup is None:
            storagegroup = self.REST.create_storage_group(
                srp, storagegroup_name, slo, workload)

        if storagegroup is None:
            msg = ("Cannot get or create a storage group: "
                   "%(storagegroup_name)s for volume %(volume_name)s."
                   % {'storagegroup_name': storagegroup_name,
                      'volume_name': masking_view_dict[VOL_NAME]})
            LOG.error(msg)

        return msg

    def _validate_existing_masking_view(
            self, masking_view_dict,
            masking_view_name, default_sg_name):
        """Validate the components of an existing masking view.

        :param masking_view_dict: the masking view dict
        :param masking_view_name: the masking view name
        :param default_sg_name: the default sg name
        :returns: storage_group_name -- string, msg -- string
        """
        storage_group_name, msg = self._check_existing_storage_group(
            masking_view_name, default_sg_name, masking_view_dict)
        if not msg:
            portgroup_name = self.REST.get_element_from_masking_view(
                masking_view_name, portgroup=True)
            portgroup = self.REST.get_portgroups(portgroup_name)
            if portgroup is None:
                msg = ("Cannot get port group: %(portgroup)s from the array "
                       "%(array)s. Portgroups must be pre-configured - please "
                       "check the array."
                       % {'portgroup': portgroup_name, 'array': self.array})
                LOG.error(msg)
            else:
                ig_from_mv = self.REST.get_element_from_masking_view(
                    masking_view_name, host=True)
                if ig_from_mv is None:
                    msg = ("Cannot get initiator group: %(ig_name)s "
                           "in masking view %(masking_view_name)s."
                           % {'ig_name': ig_from_mv,
                              'masking_view_name': masking_view_name})
                    LOG.error(msg)

        return storage_group_name, msg

    def _check_existing_storage_group(
            self, masking_view_name, default_sg_name, masking_view_dict):
        """Check if the masking view has the child storage group.

        Get the parent storage group associated with a masking view and check
        if the required child storage group is already a member. If not, get
        or create the child storage group.
        :param masking_view_name: the masking view name
        :param default_sg_name: the default sg name
        :param masking_view_dict: the masking view dict
        :returns: storage group name, msg
        """
        msg = None
        child_sg_name = masking_view_dict[SG_NAME]
        parent_sg_name = masking_view_dict[PARENT_SG_NAME]
        sg_from_mv = self.REST.get_element_from_masking_view(
            masking_view_name, storagegroup=True)

        storagegroup = self.REST.get_storage_group(sg_from_mv)

        if not storagegroup:
            msg = ("Cannot get storage group: %(sg_from_mv)s "
                   "from masking view %(masking_view)s."
                   % {'sg_from_mv': sg_from_mv,
                      'masking_view': masking_view_name})
            LOG.error(msg)
        else:
            check_child = self.REST.is_child_sg_in_parent_sg(
                child_sg_name, parent_sg_name)
            child_sg = self.REST.get_storage_group(child_sg_name)
            # Ensure the child sg can be retrieved
            if check_child and not child_sg:
                msg = ("Cannot get child storage group: %(sg_name)s "
                       "but it is listed as child of %(parent_sg)s"
                       % {'sg_name': child_sg_name,
                          'parent_sg': parent_sg_name})
                LOG.error(msg)
            elif check_child and child_sg:
                LOG.debug("Retrieved child sg %(sg_name)s from %(mv_name)s",
                          {'sg_name': child_sg_name,
                           'mv_name': masking_view_name})
            else:
                msg = self._get_or_create_storage_group(
                    masking_view_dict, child_sg_name)
            if not msg:
                msg = self._move_vol_from_default_sg(
                    masking_view_dict[DEVICE_ID],
                    masking_view_dict[VOL_NAME], default_sg_name,
                    child_sg_name)
            if not msg and not check_child:
                msg = self._check_add_child_sg_to_parent_sg(
                    child_sg_name, parent_sg_name)

        return child_sg_name, msg

    def _check_add_child_sg_to_parent_sg(
            self, child_sg_name, parent_sg_name):
        """Check adding a child storage group to a parent storage group.

        :param child_sg_name: the name of the child storage group
        :param parent_sg_name: the name of the parent storage group
        :returns: error_message or None
        """
        msg = None
        if self.REST.is_child_sg_in_parent_sg(
                child_sg_name, parent_sg_name):
            LOG.debug("Child sg: %(child_sg)s is already part "
                      "of parent storage group %(parent_sg)s.",
                      {'child_sg': child_sg_name,
                       'parent_sg': parent_sg_name})
        else:
            try:
                self.REST.add_child_sg_to_parent_sg(
                    child_sg_name, parent_sg_name)
            except Exception as e:
                msg = ("Exception adding child sg %(child_sg)s to "
                       "%(parent_sg)s. Exception received was %(e)s"
                       % {'child_sg': child_sg_name,
                          'parent_sg': parent_sg_name,
                          'e': six.text_type(e)})
                LOG.error(msg)
        return msg

    def detach_volume(self, volume_name, device_id, group_conf):
        move = False
        storagegroup_names = self.get_storage_groups_from_volume(
            device_id)
        if storagegroup_names:
            if len(storagegroup_names) == 1:
                move = True
            for sg_name in storagegroup_names:
                self.remove_volume_from_sg(
                    device_id, volume_name, sg_name, group_conf, move)
        if move is False:
            self.add_volume_to_default_storage_group(
                device_id, volume_name, group_conf)

    def get_storage_groups_from_volume(self, device_id):
        """Returns all the storage groups for a particular volume.

        :param device_id: the volume device id
        :returns: storagegroup_list
        """
        sg_list = []
        vol = self.REST.get_volume(device_id)
        if vol and vol.get('storageGroupId'):
            sg_list = vol['storageGroupId']
        num_storage_groups = len(sg_list)
        LOG.debug("There are %(num)d storage groups associated "
                  "with volume %(deviceId)s.",
                  {'num': num_storage_groups, 'deviceId': device_id})
        return sg_list

    def remove_volume_from_sg(
            self, device_id, vol_name, storagegroup_name, group_conf,
            move=False):
        """Remove a volume from a storage group.

        :param group_conf:
        :param device_id: the volume device id
        :param vol_name: the volume name
        :param storagegroup_name: the storage group name
        :param move: flag to indicate if move should be used instead of remove
        """
        masking_list = self.REST.get_masking_views_from_storage_group(
            storagegroup_name)
        if not masking_list:
            LOG.debug("No masking views associated with storage group "
                      "%(sg_name)s", {'sg_name': storagegroup_name})

            # Make sure volume hasn't been recently removed from the sg
            if self.REST.is_volume_in_storagegroup(
                    device_id, storagegroup_name):
                num_vol_in_sg = self.REST.get_num_vols_in_sg(storagegroup_name)
                LOG.debug("There are %(num_vol)d volumes in the "
                          "storage group %(sg_name)s.",
                          {'num_vol': num_vol_in_sg,
                           'sg_name': storagegroup_name})

                if num_vol_in_sg == 1:
                    # Last volume in the storage group - delete sg.
                    self._last_vol_in_sg(
                        device_id, vol_name, storagegroup_name, move,
                        group_conf)
                else:
                    # Not the last volume so remove it from storage group
                    self._multiple_vols_in_sg(
                        device_id, storagegroup_name, vol_name, move,
                        group_conf)
            else:
                LOG.debug("Volume with device_id %(dev)s is no longer a "
                          "member of %(sg)s.",
                          {'dev': device_id, 'sg': storagegroup_name})

        else:
            # Need to lock masking view when we are locking the storage
            # group to avoid possible deadlock situations from concurrent
            # processes
            masking_name = masking_list[0]
            parent_sg_name = self.get_parent_sg_from_child(storagegroup_name)

            # Make sure volume hasn't been recently removed from the sg
            is_vol = self.REST.is_volume_in_storagegroup(
                device_id, storagegroup_name)
            if is_vol:
                num_vol_in_sg = self.REST.get_num_vols_in_sg(
                    storagegroup_name)
                LOG.debug(
                    "There are %(num_vol)d volumes in the storage group "
                    "%(sg_name)s associated with %(mv_name)s. Parent "
                    "storagegroup is %(parent)s.",
                    {'num_vol': num_vol_in_sg, 'sg_name': storagegroup_name,
                     'mv_name': masking_name, 'parent': parent_sg_name})

                if num_vol_in_sg == 1:
                    # Last volume in the storage group - delete sg.
                    self._last_vol_in_sg(
                        device_id, vol_name, storagegroup_name, move,
                        group_conf)
                else:
                    # Not the last volume so remove it from storage group
                    self._multiple_vols_in_sg(
                        device_id, storagegroup_name, vol_name, move,
                        group_conf)
            else:
                LOG.debug("Volume with device_id %(dev)s is no longer a "
                          "member of %(sg)s",
                          {'dev': device_id, 'sg': storagegroup_name})

    def _multiple_vols_in_sg(self, device_id, storagegroup_name,
                             volume_name, move, group_conf):
        """Remove the volume from the SG.

        If the volume is not the last in the storage group,
        remove the volume from the SG and leave the sg on the array.
        :param device_id: volume device id
        :param volume_name: volume name
        :param storagegroup_name: storage group name
        :param move: flag to indicate a move instead of remove
        """
        if move:
            self.add_volume_to_default_storage_group(
                device_id, volume_name, group_conf, src_sg=storagegroup_name)
        else:
            self.REST.remove_vol_from_sg(storagegroup_name, device_id)

        LOG.debug(
            "Volume %(volume_name)s successfully moved/ removed from "
            "storage group %(sg)s.",
            {'volume_name': volume_name, 'sg': storagegroup_name})

        num_vol_in_sg = self.REST.get_num_vols_in_sg(
            storagegroup_name)
        LOG.debug("There are %(num_vol)d volumes remaining in the storage "
                  "group %(sg_name)s.",
                  {'num_vol': num_vol_in_sg,
                   'sg_name': storagegroup_name})

    def _last_vol_in_sg(self, device_id, volume_name, storagegroup_name,
                        move, group_conf):
        """Steps if the volume is the last in a storage group.

        1. Check if the volume is in a masking view.
        2. If it is in a masking view, check if it is the last volume in the
           masking view or just this child storage group.
        3. If it is last in the masking view, delete the masking view,
           delete the initiator group if there are no other masking views
           associated with it, and delete the both the current storage group
           and its parent group.
        4. Otherwise, remove the volume and delete the child storage group.
        5. If it is not in a masking view, delete the storage group.
        :param device_id: volume device id
        :param volume_name: volume name
        :param storagegroup_name: storage group name
        :param move: flag to indicate a move instead of remove
        :returns: status -- bool
        """
        LOG.debug("Only one volume remains in storage group "
                  "%(sgname)s. Driver will attempt cleanup.",
                  {'sgname': storagegroup_name})
        masking_view_list = self.REST.get_masking_views_from_storage_group(
            storagegroup_name)
        if not bool(masking_view_list):
            status = self._last_vol_no_masking_views(
                storagegroup_name, device_id, volume_name, move, group_conf)
        else:
            status = self._last_vol_masking_views(
                storagegroup_name, masking_view_list, device_id, volume_name,
                move, group_conf)
        return status

    def _last_vol_no_masking_views(self, storagegroup_name, device_id,
                                   volume_name, move, group_conf):
        """Remove the last vol from an sg not associated with an mv.

        Helper function for removing the last vol from a storage group
        which is not associated with a masking view.
        :param storagegroup_name: the storage group name
        :param device_id: the device id
        :param volume_name: the volume name
        :param move: flag to indicate a move instead of remove
        :returns: status -- bool
        """
        # Check if storage group is a child sg:
        parent_sg = self.get_parent_sg_from_child(storagegroup_name)
        if parent_sg is None:
            # Move the volume back to the default storage group, if required
            if move:
                self.add_volume_to_default_storage_group(
                    device_id, volume_name, group_conf,
                    src_sg=storagegroup_name)
            # Delete the storage group.
            self.REST.delete_storage_group(storagegroup_name)
            status = True
        else:
            num_vols_parent = self.REST.get_num_vols_in_sg(parent_sg)
            if num_vols_parent == 1:
                self._delete_cascaded_storage_groups(
                    storagegroup_name, parent_sg, device_id, move, group_conf)
            else:
                self._remove_last_vol_and_delete_sg(
                    device_id, volume_name,
                    storagegroup_name, group_conf, parent_sg, move)
            status = True
        return status

    def _remove_last_vol_and_delete_sg(
            self, device_id, volume_name, storagegroup_name, group_conf,
            parent_sg_name=None, move=False):
        """Remove the last volume and delete the storage group.

        If the storage group is a child of another storage group,
        it must be removed from the parent before deletion.
        :param device_id: the volume device id
        :param volume_name: the volume name
        :param storagegroup_name: the sg name
        :param parent_sg_name: the parent sg name
        """
        if move:
            self.add_volume_to_default_storage_group(
                device_id, volume_name, group_conf, src_sg=storagegroup_name)
        else:
            self.REST.remove_vol_from_sg(storagegroup_name, device_id)

        LOG.debug("Remove the last volume %(volumeName)s completed "
                  "successfully.", {'volumeName': volume_name})
        if parent_sg_name:
            self.REST.remove_child_sg_from_parent_sg(
                storagegroup_name, parent_sg_name)

        self.REST.delete_storage_group(storagegroup_name)

    def _delete_cascaded_storage_groups(self, child_sg_name, parent_sg_name,
                                        device_id, move, group_conf):
        """Delete a child and parent storage groups.

        :param child_sg_name: the child storage group name
        :param parent_sg_name: the parent storage group name
        :param device_id: the volume device id
        :param move: flag to indicate if volume should be moved to default sg
        """
        if move:
            self.add_volume_to_default_storage_group(
                device_id, "", group_conf, src_sg=child_sg_name)
        if child_sg_name != parent_sg_name:
            self.REST.delete_sg(parent_sg_name)
            LOG.debug("Storage Group %(storagegroup_name)s "
                      "successfully deleted.",
                      {'storagegroup_name': parent_sg_name})
        self.REST.delete_sg(child_sg_name)

        LOG.debug("Storage Group %(storagegroup_name)s successfully deleted.",
                  {'storagegroup_name': child_sg_name})

    def add_volume_to_default_storage_group(
            self, device_id, volume_name, group_conf, src_sg=None):
        """Return volume to its default storage group.

        :param group_conf:
        :param device_id: the volume device id
        :param volume_name: the volume name
        :param src_sg: the source storage group, if any
        """
        storagegroup_name = self.get_or_create_default_storage_group(
            group_conf.safe_get(SRP), group_conf.safe_get(SLO),
            group_conf.safe_get(WORKLOAD))
        if src_sg is not None:
            self.REST.move_volume_between_storage_groups(
                device_id, src_sg, storagegroup_name, force=True)
        else:
            self._check_adding_volume_to_storage_group(
                device_id, storagegroup_name, volume_name)

    def get_parent_sg_from_child(self, storagegroup_name):
        """Given a storage group name, get its parent storage group, if any.

        :param storagegroup_name: the name of the storage group
        :returns: the parent storage group name, or None
        """
        parent_sg_name = None
        storagegroup = self.REST.get_storage_group(storagegroup_name)
        if storagegroup and storagegroup.get('parent_storage_group'):
            parent_sg_name = storagegroup['parent_storage_group'][0]
        return parent_sg_name

    def _last_vol_masking_views(
            self, storagegroup_name, masking_view_list, device_id, volume_name,
            move, group_conf):
        """Remove the last vol from an sg associated with masking views.

        Helper function for removing the last vol from a storage group
        which is associated with one or more masking views.
        :param storagegroup_name: the storage group name
        :param masking_view_list: the list of masking views
        :param device_id: the device id
        :param volume_name: the volume name
        :param move: flag to indicate a move instead of remove
        :returns: status -- bool
        """
        status = False
        for mv in masking_view_list:
            num_vols_in_mv, parent_sg_name = (
                self._get_num_vols_from_mv(mv))
            # If the volume is the last in the masking view, full cleanup
            if num_vols_in_mv == 1:
                self._delete_mv_ig_and_sg(
                    device_id, mv, storagegroup_name, parent_sg_name, move,
                    group_conf)
            else:
                self._remove_last_vol_and_delete_sg(
                    device_id, volume_name,
                    storagegroup_name, group_conf, parent_sg_name, move)
            status = True
        return status

    def _delete_mv_ig_and_sg(
            self, device_id, masking_view, storagegroup_name,
            parent_sg_name, move, group_conf):
        """Delete the masking view, storage groups and initiator group.

        :param device_id: the device id
        :param masking_view: masking view name
        :param storagegroup_name: storage group name
        :param parent_sg_name: the parent storage group name
        :param move: flag to indicate if the volume should be moved
        """
        host = platform.node()

        initiator_group = self.REST.get_element_from_masking_view(
            masking_view, host=True)
        self._last_volume_delete_masking_view(masking_view)
        self._last_volume_delete_initiator_group(initiator_group, host)
        self._delete_cascaded_storage_groups(
            storagegroup_name, parent_sg_name, device_id, move, group_conf)

    def _last_volume_delete_initiator_group(self, initiator_group_name, host):
        """Delete the initiator group.

        Delete the Initiator group if it has been created by the VMAX driver,
        and if there are no masking views associated with it.
        :param initiator_group_name: initiator group name
        :param host: the short name of the host
        """
        if host is not None:
            protocol = self.get_short_protocol_type(self.protocol)

            default_ig_name = ("DK-%(shortHostName)s-%(protocol)s-IG"
                               % {'shortHostName': host,
                                  'protocol': protocol})

            if initiator_group_name == default_ig_name:
                masking_view_names = (
                    self.REST.get_masking_views_by_host(
                        initiator_group_name))
                if not masking_view_names:
                    # Check initiator group hasn't been recently deleted
                    ig_details = self.REST.get_initiator_group_from_initiator(
                        initiator_group_name)
                    if ig_details:
                        LOG.debug(
                            "Last volume associated with the initiator "
                            "group - deleting the associated initiator "
                            "group %(initiator_group_name)s.",
                            {'initiator_group_name': initiator_group_name})
                        self.REST.delete_host(initiator_group_name)
                else:
                    LOG.warning("Initiator group %(ig_name)s is associated "
                                "with masking views and can't be deleted. "
                                "Number of associated masking view is: "
                                "%(nmv)d.",
                                {'ig_name': initiator_group_name,
                                 'nmv': len(masking_view_names)})
            else:
                LOG.warning("Initiator group %(ig_name)s was "
                            "not created by the VMAX driver so will "
                            "not be deleted by the VMAX driver.",
                            {'ig_name': initiator_group_name})
        else:
            LOG.warning("Cannot get host name from connector object - "
                        "initiator group %(ig_name)s will not be deleted.",
                        {'ig_name': initiator_group_name})

    def _last_volume_delete_masking_view(self, masking_view):
        """Delete the masking view.

        Delete the masking view if the volume is the last one in the
        storage group.
        :param masking_view: masking view name
        """
        LOG.debug("Last volume in the storage group, deleting masking view "
                  "%(masking_view_name)s.",
                  {'masking_view_name': masking_view})
        self.REST.delete_masking_view(self.array, masking_view)
        LOG.debug("Masking view %(masking_view_name)s successfully deleted.",
                  {'masking_view_name': masking_view})

    def _get_num_vols_from_mv(self, masking_view_name):
        """Get the total number of volumes associated with a masking view.

        :param masking_view_name: the name of the masking view
        :returns: num_vols, parent_sg_name
        """
        sg_name = self.REST.get_element_from_masking_view(
            masking_view_name, storagegroup=True)
        parent_sg_name = self.get_parent_sg_from_child(sg_name)
        num_vols = self.REST.get_num_vols_in_sg(parent_sg_name)
        return num_vols, parent_sg_name

    def _populate_masking_dict(self, volume, device_id, group_conf):
        """Get all the names of the maskingview and sub-components.

        :param volume: the volume object
        :returns: dict -- a dictionary with masking view information
        """
        masking_view_dict = {}
        host_name = platform.node()
        unique_name = self.truncate_string(group_conf.safe_get(SRP), 12)

        protocol = self.get_short_protocol_type(self.protocol)
        connector = {}
        if self.protocol.lower() == ISCSI:
            connector['initiator'] = fileutil.get_initiator()
        else:
            connector['wwpns'] = fileutil.get_wwpns()

        short_host_name = self.get_host_short_name(host_name)
        masking_view_dict['replication_enabled'] = False
        slo = group_conf.safe_get(SLO)
        workload = group_conf.safe_get(WORKLOAD)
        port_group = random.choice(group_conf.safe_get('port_groups'))
        short_pg_name = self.get_pg_short_name(port_group)
        masking_view_dict[SLO] = slo
        masking_view_dict[WORKLOAD] = workload
        masking_view_dict[SRP] = unique_name
        masking_view_dict[ARRAY] = self.array
        masking_view_dict[PORTGROUPNAME] = port_group

        if slo:
            slo_wl_combo = self.truncate_string(slo + workload, 10)
            child_sg_name = (
                "DK-%(shortHostName)s-%(srpName)s-%(combo)s-%(pg)s"
                % {'shortHostName': short_host_name,
                   'srpName': unique_name,
                   'combo': slo_wl_combo,
                   'pg': short_pg_name})
        else:
            child_sg_name = (
                "DK-%(shortHostName)s-No_SLO-%(pg)s"
                % {'shortHostName': short_host_name,
                   'pg': short_pg_name})
        mv_prefix = (
            "DK-%(shortHostName)s-%(protocol)s-%(pg)s"
            % {'shortHostName': short_host_name,
               'protocol': protocol, 'pg': short_pg_name})

        masking_view_dict[SG_NAME] = child_sg_name

        masking_view_dict[MV_NAME] = ("%(prefix)s-MV" % {'prefix': mv_prefix})

        masking_view_dict[PARENT_SG_NAME] = ("%(prefix)s-SG"
                                             % {'prefix': mv_prefix})

        masking_view_dict[IG_NAME] = (
            ("DK-%(shortHostName)s-%(protocol)s-IG"
             % {'shortHostName': short_host_name,
                'protocol': protocol}))
        masking_view_dict[CONNECTOR] = connector
        masking_view_dict[DEVICE_ID] = device_id
        masking_view_dict[VOL_NAME] = volume

        return masking_view_dict

    def get_pg_short_name(self, portgroup_name):
        """Create a unique port group name under 12 characters.

        :param portgroup_name: long portgroup_name
        :returns: truncated portgroup_name
        """
        if portgroup_name and len(portgroup_name) > 12:
            portgroup_name = portgroup_name.lower()
            m = hashlib.md5()
            m.update(portgroup_name.encode('utf-8'))
            uuid = m.hexdigest()
            new_name = ("%(pg)s%(uuid)s"
                        % {'pg': portgroup_name[-6:],
                           'uuid': uuid})
            portgroup_name = self.truncate_string(new_name, 12)
        return portgroup_name

    def get_host_short_name(self, host_name):
        """Returns the short name for a given qualified host name.

        Checks the host name to see if it is the fully qualified host name
        and returns part before the dot. If there is no dot in the host name
        the full host name is returned.
        :param host_name: the fully qualified host name
        :returns: string -- the short host_name
        """
        host_array = host_name.split('.')
        if len(host_array) > 1:
            short_host_name = host_array[0]
        else:
            short_host_name = host_name

        return self.generate_unique_trunc_host(short_host_name)

    def generate_unique_trunc_host(self, host_name):
        """Create a unique short host name under 16 characters.

        :param host_name: long host name
        :returns: truncated host name
        """
        if host_name and len(host_name) > 16:
            host_name = host_name.lower()
            m = hashlib.md5()
            m.update(host_name.encode('utf-8'))
            uuid = m.hexdigest()
            new_name = ("%(host)s%(uuid)s"
                        % {'host': host_name[-6:],
                           'uuid': uuid})
            host_name = self.truncate_string(new_name, 16)
        return host_name

    @staticmethod
    def get_short_protocol_type(protocol):
        """Given the protocol type, return I for iscsi and F for fc.

        :param protocol: iscsi or fc
        :returns: string -- 'I' for iscsi or 'F' for fc
        """
        if protocol.lower() == ISCSI.lower():
            return 'I'
        elif protocol.lower() == FC.lower():
            return 'F'
        else:
            return protocol

    @staticmethod
    def truncate_string(str_to_truncate, max_num):
        """Truncate a string by taking first and last characters.

        :param str_to_truncate: the string to be truncated
        :param max_num: the maximum number of characters
        :returns: string -- truncated string or original string
        """
        if len(str_to_truncate) > max_num:
            new_num = len(str_to_truncate) - max_num // 2
            first_chars = str_to_truncate[:max_num // 2]
            last_chars = str_to_truncate[new_num:]
            str_to_truncate = first_chars + last_chars
        return str_to_truncate
