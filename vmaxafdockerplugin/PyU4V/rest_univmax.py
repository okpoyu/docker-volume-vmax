# The MIT License (MIT)
# Copyright (c) 2016 Dell Inc. or its subsidiaries.

# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.



# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import six
import time

from oslo_log import log as logging
from oslo_service import loopingcall
from rest_requests import RestRequests
from vmaxafdockerplugin import exception
# register configuration file
LOG = logging.getLogger(__name__)

SLOPROVISIONING = 'sloprovisioning'
U4V_VERSION = '84'
# HTTP constants
GET = 'GET'
POST = 'POST'
PUT = 'PUT'
DELETE = 'DELETE'
STATUS_200 = 200
STATUS_201 = 201
STATUS_202 = 202
STATUS_204 = 204
# Job constants
INCOMPLETE_LIST = ['created', 'scheduled', 'running',
                   'validating', 'validated']
CREATED = 'created'
SUCCEEDED = 'succeeded'
CREATE_VOL_STRING = "Creating new Volumes"


class RestFunctions:
    def __init__(self, username=None, password=None, server_ip=None,
                 port=None, verify=False, array=None):
        self.array_id = array
        self.end_date = int(round(time.time() * 1000))
        self.start_date = (self.end_date - 3600000)
        base_url = 'https://%s:%s/univmax/restapi' % (server_ip, port)
        self.rest_client = RestRequests(username, password, verify, base_url)

    # masking view

    def get_masking_views(self, masking_view_id=None, filters=None):
        """Get a masking view or list of masking views.

        If masking_view_id, return details of a particular masking view.
        Either masking_view_id or filters can be set
        :param masking_view_id: the name of the masking view
        :param filters: dictionary of filters
        :return: dict, status_code
        """
        target_uri = ("/sloprovisioning/symmetrix/%s/maskingview"
                      % self.array_id)
        if masking_view_id:
            target_uri += "/%s" % masking_view_id
        if masking_view_id and filters:
            LOG.error("masking_view_id and filters are mutually exclusive")
            raise Exception
        return self.rest_client.rest_request(target_uri, GET, params=filters)

    def delete_masking_view(self, masking_view_id):
        """Delete a given masking view.

        :param masking_view_id: the name of the masking view
        :return: None, status code
        """
        self.delete_resource(
            SLOPROVISIONING, 'maskingview', masking_view_id)
        LOG.debug("Masking view successfully deleted.")

    def modify_storagegroup(self, sg_id, edit_sg_data):
        """Edits an existing storage group

        :param sg_id: the name of the storage group
        :param edit_sg_data: the payload of the request
        :return: dict, status_code
        """
        target_uri = ("/sloprovisioning/symmetrix/%s/storagegroup/%s"
                      % (self.array_id, sg_id))
        return self.rest_client.rest_request(
            target_uri, PUT, request_object=edit_sg_data)

    def remove_vol_from_storagegroup(self, sg_id, vol_id):
        """Remove a volume from a given storage group

        :param sg_id: the name of the storage group
        :param vol_id: the device id of the volume
        :return: dict, status_code
        """
        del_vol_data = ({"editStorageGroupActionParam": {
            "removeVolumeParam": {
                "volumeId": [vol_id]}}})
        return self.modify_storagegroup(sg_id, del_vol_data)

    # volume

    def create_volume_from_sg(self, volume_name, storagegroup_name,
                              volume_size):
        """Create a new volume in the given storage group.

        :param volume_name: the volume name (String)
        :param storagegroup_name: the storage group name
        :param volume_size: volume size (String)
        :returns: dict -- volume_dict - the volume dict
        :raises: VolumeBackendAPIException
        """
        payload = (
            {"executionOption": "ASYNCHRONOUS",
             "editStorageGroupActionParam": {
                 "expandStorageGroupParam": {
                     "addVolumeParam": {
                         "num_of_vols": 1,
                         "emulation": "FBA",
                         "volumeIdentifier": {
                             "identifier_name": volume_name,
                             "volumeIdentifierChoice": "identifier_name"},
                         "volumeAttribute": {
                             "volume_size": volume_size,
                             "capacityUnit": "GB"}}}}})
        status_code, job = self.modify_storage_group(
            storagegroup_name, payload)

        LOG.debug("Create Volume: %(volumename)s. Status code: %(sc)lu.",
                  {'volumename': volume_name,
                   'sc': status_code})

        task = self.wait_for_job('Create volume', status_code, job)

        # Find the newly created volume.
        device_id = None
        if task:
            for t in task:
                try:
                    desc = t["description"]
                    if CREATE_VOL_STRING in desc:
                        t_list = desc.split()
                        device_id = t_list[(len(t_list) - 1)]
                        device_id = device_id[1:-1]
                        break
                    if device_id:
                        self.get_volume(device_id)
                except Exception as e:
                    LOG.error("Could not retrieve device id from job. "
                              "Exception received was %(e)s. Attempting "
                              "retrieval by volume_identifier.",
                              {'e': e})

        if not device_id:
            device_id = self.find_volume_device_id(volume_name)

        volume_dict = {'array': self.array_id, 'device_id': device_id}
        return volume_dict

    def get_volume_list(self, params):
        """Get a filtered list of VMAX volumes from array.

        Filter parameters are required as the unfiltered volume list could be
        very large and could affect performance if called often.
        :param params: filter parameters
        :returns: device_ids -- list
        """
        device_ids = []
        volumes = self.get_resource(
            SLOPROVISIONING, 'volume', params=params)
        try:
            volume_dict_list = volumes['resultList']['result']
            for vol_dict in volume_dict_list:
                device_id = vol_dict['volumeId']
                device_ids.append(device_id)
        except (KeyError, TypeError):
            pass
        return device_ids

    def find_volume_device_id(self, volume_name):
        """Given a volume identifier, find the corresponding device_id.

        :param volume_name: the volume name (OS-<UUID>)
        :returns: device_id
        """
        device_id = None
        params = {"volume_identifier": volume_name}

        volume_list = self.get_volume_list(params)
        if not volume_list:
            LOG.debug("Cannot find record for volume %(volumeId)s.",
                      {'volumeId': volume_name})
        else:
            device_id = volume_list[0]
        return device_id

    def get_volume(self, device_id):
        """Get a VMAX volume from array.

        :param device_id: the volume device id
        :returns: volume dict
        :raises: VolumeBackendAPIException
        """
        volume_dict = self.get_resource(
            SLOPROVISIONING, 'volume', resource_name=device_id)
        if not volume_dict:
            exception_message = ("Volume %(deviceID)s not found."
                                 % {'deviceID': device_id})
            raise exception.VMAXPluginException(exception_message)
        return volume_dict

    def get_volumes(self, vol_id=None, filters=None):
        """Gets details of volume(s) from array.

        :param vol_id: the volume's device ID
        :param filters: dictionary of filters
        :return: dict, status_code
        """
        target_uri = "/sloprovisioning/symmetrix/%s/volume" % self.array_id
        if vol_id:
            target_uri += vol_id
        if vol_id and filters:
            LOG.error("volID and filters are mutually exclusive.")
            raise Exception()
        return self.rest_client.rest_request(target_uri, GET,
                                             params=filters)

    def delete_volume(self, device_id):
        """Deallocate or delete a volume.

        :param device_id: volume device id
        """
        # Deallocate volume. Can fail if there are no tracks allocated.
        payload = {"editVolumeActionParam": {
            "freeVolumeParam": {"free_volume": 'true'}}}
        try:
            self._modify_volume(device_id, payload)
        except Exception as e:
            LOG.debug('Deallocate volume failed with %(e)s.'
                      'Attempting delete.', {'e': e})
        # Try to delete the volume if deallocate failed.
        self.delete_resource(SLOPROVISIONING, "volume", device_id)

    def _modify_volume(self, device_id, payload):
        """Modify a volume (PUT operation).

        :param device_id: volume device id
        :param payload: the request payload
        """
        return self.modify_resource(SLOPROVISIONING, 'volume',
                                    payload, resource_name=device_id)

    def get_port(self, port_id):
        """Get director port details.

        :param port_id: the port id
        :returns: port dict, or None
        """
        dir_id = port_id.split(':')[0]
        port_no = port_id.split(':')[1]
        target_uri = (
            '/84/sloprovisioning/symmetrix/%s/director/%s/port/%s' % (
                self.array_id, dir_id, port_no))
        response, sc = self.rest_client.rest_request(target_uri, GET)
        return response, sc

    def get_port_ids(self, port_group):
        """Get a list of port identifiers from a port group.

        :param port_group: the name of the portgroup
        :returns: list of port ids, e.g. ['FA-3D:35', 'FA-4D:32']
        """
        LOG.debug("The portgroup name for iscsiadm is %(pg)s",
                  {'pg': port_group})
        port_list = []
        portgroup_info = self.get_portgroup(port_group)

        if portgroup_info:
            port_key = portgroup_info["symmetrixPortKey"]
            for key in port_key:
                port = key['portId']
                port_list.append(port)
        return port_list

    def get_iscsi_ip_address_and_iqn(self, port_id):
        ip_addresses, iqn = None, None
        port_details, _ = self.get_port(port_id)
        if port_details:
            ip_addresses = port_details['symmetrixPort']['ip_addresses']
            iqn = port_details['symmetrixPort']['identifier']
        return ip_addresses, iqn

    def verify_slo_workload(self, slo, workload):
        """Check if SLO and workload values are valid.

        :param slo: Service Level Object e.g bronze
        :param workload: workload e.g DSS
        :returns: boolean
        """
        is_valid_slo, is_valid_workload = False, False

        if workload and workload.lower() == 'none':
            workload = None

        if not workload:
            is_valid_workload = True

        if slo and slo.lower() == 'none':
            slo = None

        valid_slos = self.get_slo_list()
        valid_workloads = self.get_workload_settings()
        for valid_slo in valid_slos:
            if slo == valid_slo:
                is_valid_slo = True
                break

        for valid_workload in valid_workloads:
            if workload == valid_workload:
                is_valid_workload = True
                break

        if not slo:
            is_valid_slo = True
            if workload:
                is_valid_workload = False

        if not is_valid_slo:
            LOG.error(
                "SLO: %(slo)s is not valid. Valid values are: "
                "%(valid_slos)s.", {'slo': slo, 'valid_slos': valid_slos})

        if not is_valid_workload:
            LOG.error(
                "Workload: %(workload)s is not valid. Valid values are "
                "%(valid_workloads)s. Note you cannot "
                "set a workload without an SLO.",
                {'workload': workload, 'valid_workloads': valid_workloads})

        return is_valid_slo, is_valid_workload

    def get_slo_list(self):
        """Retrieve the list of slo's from the array

        :returns: slo_list -- list of service level names
        """
        slo_list = []
        slo_dict = self.get_resource(SLOPROVISIONING, 'slo')
        if slo_dict and slo_dict.get('sloId'):
            slo_list = slo_dict['sloId']
        return slo_list

    def get_workload_settings(self):
        """Get valid workload options from array.

        :returns: workload_setting -- list of workload names
        """
        workload_setting = []
        wl_details = self.get_resource(SLOPROVISIONING, 'workloadtype')
        if wl_details:
            workload_setting = wl_details['workloadId']
        return workload_setting

    def modify_storage_group(self, storagegroup, payload,
                             version=U4V_VERSION):
        """Modify a storage group (PUT operation).

        :param version: the uv4 version
        :param storagegroup: storage group name
        :param payload: the request payload
        :returns: status_code -- int, message -- string, server response
        """
        return self.modify_resource(
            SLOPROVISIONING, 'storagegroup', payload, version,
            resource_name=storagegroup)

    def get_or_create_default_storage_group(
            self, srp, slo, workload, do_disable_compression=False,
            is_re=False):
        """Get or create a default storage group.

        :param srp: the SRP name
        :param slo: the SLO
        :param workload: the workload
        :param do_disable_compression: flag for compression
        :param is_re: is replication enabled
        :returns: storagegroup_name
        :raises: VolumeBackendAPIException
        """
        storagegroup, storagegroup_name = (
            self.get_vmax_default_storage_group(
                srp, slo, workload, do_disable_compression,
                is_re))
        if storagegroup is None:
            self.create_storage_group(
                storagegroup_name, srp, slo, workload, )
        else:
            # Check that SG is not part of a masking view
            LOG.debug("Using existing default storage group")
            masking_views = self.get_masking_views_from_storage_group(
                storagegroup_name)
            if masking_views:
                exception_message = (
                    ("Default storage group %(sg_name)s is part of masking "
                     "views %(mvs)s. Please remove it from all masking views")
                    % {'sg_name': storagegroup_name,
                       'mvs': masking_views})
                raise exception.VMAXPluginException(exception_message)

        return storagegroup_name

    def get_masking_views_from_storage_group(self, storagegroup):
        """Return any masking views associated with a storage group.

        :param storagegroup: the storage group name
        :returns: masking view list
        """
        maskingviewlist = []
        storagegroup = self.get_storage_group(storagegroup)
        if storagegroup and storagegroup.get('maskingview'):
            maskingviewlist = storagegroup['maskingview']
        return maskingviewlist

    def get_masking_views_by_initiator_group(
            self, initiatorgroup_name):
        """Given initiator group, retrieve the masking view instance name.

        Retrieve the list of masking view instances associated with the
        given initiator group.
        :param initiatorgroup_name: the name of the initiator group
        :returns: list of masking view names
        """
        masking_view_list = []
        ig_details = self.get_initiator_group(initiatorgroup_name)
        if ig_details:
            if ig_details.get('maskingview'):
                masking_view_list = ig_details['maskingview']
        else:
            LOG.error("Error retrieving initiator group %(ig_name)s",
                      {'ig_name': initiatorgroup_name})
        return masking_view_list

    def get_vmax_default_storage_group(
            self, srp, slo, workload,
            do_disable_compression=False, is_re=False):
        """Get the default storage group.

        :param srp: the pool name
        :param slo: the SLO
        :param workload: the workload
        :param do_disable_compression: flag for disabling compression
        :param is_re: flag for replication
        :returns: the storage group dict (or None), the storage group name
        """
        storagegroup_name = self.get_default_storage_group_name(
            srp, slo, workload, do_disable_compression, is_re)
        storagegroup = self.get_storage_group(storagegroup_name)
        return storagegroup, storagegroup_name

    @staticmethod
    def get_default_storage_group_name(
            srp_name, slo, workload, is_compression_disabled=False,
            is_re=False):
        """Determine default storage group from extra_specs.

        :param srp_name: the name of the srp on the array
        :param slo: the service level string e.g Bronze
        :param workload: the workload string e.g DSS
        :param is_compression_disabled:  flag for disabling compression
        :param is_re: flag for replication
        :returns: storage_group_name
        """
        if slo and workload:
            prefix = ("OS-%(srpName)s-%(slo)s-%(workload)s"
                      % {'srpName': srp_name, 'slo': slo,
                         'workload': workload})

            if is_compression_disabled:
                prefix += "-CD"

        else:
            prefix = "OS-no_SLO"
        if is_re:
            prefix += "-RE"

        storage_group_name = ("%(prefix)s-SG" % {'prefix': prefix})
        return storage_group_name

    def get_num_vols_in_sg(self, storage_group_name):
        """Get the number of volumes in a storage group.

        :param storage_group_name: the storage group name
        :returns: num_vols -- int
        """
        num_vols = 0
        storagegroup = self.get_storage_group(storage_group_name)
        try:
            num_vols = int(storagegroup['num_of_vols'])
        except (KeyError, TypeError):
            pass
        return num_vols

    def create_storage_group(self, storagegroup_name, srp, slo, workload):
        """Create the volume in the specified storage group.

        :param storagegroup_name: the group name (String)
        :param srp: the SRP (String)
        :param slo: the SLO (String)
        :param workload: the workload (String)
        :returns: storagegroup_name - string
        """
        srp_id = srp if slo else "None"
        payload = ({"srpId": srp_id,
                    "storageGroupId": storagegroup_name,
                    "emulation": "FBA"})

        if slo:
            slo_param = {"num_of_vols": 0,
                         "sloId": slo,
                         "workloadSelection": workload,
                         "volumeAttribute": {
                             "volume_size": "0",
                             "capacityUnit": "GB"}}
            slo_param.update({"noCompression": "true"})

            payload.update({"sloBasedStorageGroupParam": [slo_param]})

        status_code, job = self.create_resource(
            SLOPROVISIONING, 'storagegroup', payload)
        self.wait_for_job('Create storage group', status_code, job)
        return storagegroup_name

    def get_storage_group(self, storage_group_name):
        """Given a name, return storage group details.

        :param storage_group_name: the name of the storage group
        :returns: storage group dict or None
        """
        return self.get_resource(
            SLOPROVISIONING, 'storagegroup',
            resource_name=storage_group_name)

    def delete_storage_group(self, storagegroup_name):
        """Delete a storage group.

        :param storagegroup_name: storage group name
        """
        self.delete_resource(
            SLOPROVISIONING, 'storagegroup', storagegroup_name)
        LOG.debug("Storage Group successfully deleted.")

    def get_portgroup(self, portgroup):
        """Get a portgroup from the array.

        :param portgroup: the portgroup name
        :returns: portgroup dict or None
        """
        return self.get_resource(
            SLOPROVISIONING, 'portgroup', resource_name=portgroup)

    def get_masking_view(self, masking_view_name):
        """Get details of a masking view.

        :param masking_view_name: the masking view name
        :returns: masking view dict
        """
        return self.get_resource(
            SLOPROVISIONING, 'maskingview', masking_view_name)

    def remove_vol_from_sg(self, storagegroup_name, device_id):
        """Remove a volume from a storage group.

        :param storagegroup_name: storage group name
        :param device_id: the device id
        """
        if not isinstance(device_id, list):
            device_id = [device_id]
        payload = ({"executionOption": "ASYNCHRONOUS",
                    "editStorageGroupActionParam": {
                        "removeVolumeParam": {
                            "volumeId": device_id}}})
        status_code, job = self.modify_storage_group(
            storagegroup_name, payload)

        self.wait_for_job('Remove vol from sg', status_code, job)

    def remove_child_sg_from_parent_sg(self, child_sg, parent_sg):
        """Remove a storage group from its parent storage group.

        This method removes a child storage group from its parent group.
        :param child_sg: the name of the child sg
        :param parent_sg: the name of the parent sg
        """
        payload = {"editStorageGroupActionParam": {
            "removeStorageGroupParam": {
                "storageGroupId": [child_sg], "force": 'true'}}}
        status_code, job = self.modify_storage_group(parent_sg, payload)
        self.wait_for_job(
            'Remove child sg from parent sg', status_code, job)

    def add_child_sg_to_parent_sg(
            self, child_sg, parent_sg):
        """Add a storage group to a parent storage group.

        This method adds an existing storage group to another storage
        group, i.e. cascaded storage groups.
        :param child_sg: the name of the child sg
        :param parent_sg: the name of the parent sg
        """
        payload = {"editStorageGroupActionParam": {
            "expandStorageGroupParam": {
                "addExistingStorageGroupParam": {
                    "storageGroupId": [child_sg]}}}}
        sc, job = self.modify_storage_group(parent_sg, payload)
        self.wait_for_job('Add child sg to parent sg', sc, job)

    def is_child_sg_in_parent_sg(self, child_name, parent_name):
        """Check if a child storage group is a member of a parent group.

        :param child_name: the child sg name
        :param parent_name: the parent sg name
        :returns: bool
        """
        parent_sg = self.get_storage_group(parent_name)
        if parent_sg and parent_sg.get('child_storage_group'):
            child_sg_list = parent_sg['child_storage_group']
            if child_name in child_sg_list:
                return True
        return False

    def get_element_from_masking_view(
            self, maskingview_name, portgroup=False, host=False,
            storagegroup=False):
        """Return the name of the specified element from a masking view.

        :param maskingview_name: the masking view name
        :param portgroup: the port group name - optional
        :param host: the host name - optional
        :param storagegroup: the storage group name - optional
        :returns: name of the specified element -- string
        :raises: VolumeBackendAPIException
        """
        element = None
        masking_view_details = self.get_masking_view(maskingview_name)
        if masking_view_details:
            if portgroup:
                element = masking_view_details['portGroupId']
            elif host:
                element = masking_view_details['hostId']
            elif storagegroup:
                element = masking_view_details['storageGroupId']
        else:
            exception_message = "Error retrieving masking group."
            raise exception.VMAXPluginException(exception_message)
        return element

    def create_masking_view(self, maskingview_name, storagegroup_name,
                            port_group_name, init_group_name):
        """Create a new masking view.

        :param maskingview_name: the masking view name
        :param storagegroup_name: the storage group name
        :param port_group_name: the port group
        :param init_group_name: the initiator group
        """
        payload = ({"executionOption": "ASYNCHRONOUS",
                    "portGroupSelection": {
                        "useExistingPortGroupParam": {
                            "portGroupId": port_group_name}},
                    "maskingViewId": maskingview_name,
                    "hostOrHostGroupSelection": {
                        "useExistingHostParam": {
                            "hostId": init_group_name}},
                    "storageGroupSelection": {
                        "useExistingStorageGroupParam": {
                            "storageGroupId": storagegroup_name}}})

        status_code, job = self.create_resource(
            SLOPROVISIONING, 'maskingview', payload)

        self.wait_for_job('Create masking view', status_code, job)

    def add_volume_to_storage_group(self, storagegroup_name, device_id):
        """Add a volume to a storage group.

        :param storagegroup_name: storage group name
        :param device_id: the device id
        """
        if not isinstance(device_id, list):
            device_id = [device_id]
        payload = ({"executionOption": "ASYNCHRONOUS",
                    "editStorageGroupActionParam": {
                        "expandStorageGroupParam": {
                            "addSpecificVolumeParam": {
                                "volumeId": device_id}}}})
        status_code, job = self.modify_storage_group(
            storagegroup_name, payload)

        self.wait_for_job('Add volume to sg', status_code, job)

    def move_volume_between_storage_groups(
            self, device_id, source_storagegroup_name,
            target_storagegroup_name, force=False):
        """Move a volume to a different storage group.

        :param source_storagegroup_name: the originating storage group name
        :param target_storagegroup_name: the destination storage group name
        :param device_id: the device id
        :param force: force flag (necessary on a detach)
        """
        force_flag = "true" if force else "false"
        payload = ({"executionOption": "ASYNCHRONOUS",
                    "editStorageGroupActionParam": {
                        "moveVolumeToStorageGroupParam": {
                            "volumeId": [device_id],
                            "storageGroupId": target_storagegroup_name,
                            "force": force_flag}}})
        status_code, job = self.modify_storage_group(
            source_storagegroup_name, payload)
        self.wait_for_job('move volume between storage groups', status_code,
                          job)

    def is_volume_in_storagegroup(self, device_id, storagegroup):
        """See if a volume is a member of the given storage group.

        :param device_id: the device id
        :param storagegroup: the storage group name
        :returns: bool
        """
        is_vol_in_sg = False
        sg_list = self.get_storage_groups_from_volume(device_id)
        if storagegroup in sg_list:
            is_vol_in_sg = True
        return is_vol_in_sg

    def get_storage_groups_from_volume(self, device_id):
        """Returns all the storage groups for a particular volume.

        :param device_id: the volume device id
        :returns: storagegroup_list
        """
        sg_list = []
        vol = self.get_volume(device_id)
        if vol and vol.get('storageGroupId'):
            sg_list = vol['storageGroupId']
        num_storage_groups = len(sg_list)
        LOG.debug("There are %(num)d storage groups associated "
                  "with volume %(deviceId)s.",
                  {'num': num_storage_groups, 'deviceId': device_id})
        return sg_list

    # Initiators

    def create_initiator_group(self, init_group_name, init_list):
        """Create a new initiator group containing the given initiators.

        :param init_group_name: the initiator group name
        :param init_list: the list of initiators
        """
        new_ig_data = ({"executionOption": "ASYNCHRONOUS",
                        "hostId": init_group_name, "initiatorId": init_list})
        sc, job = self.create_resource(SLOPROVISIONING, 'host', new_ig_data)
        self.wait_for_job('create initiator group', sc, job)

    def get_initiator_group(self, initiator_group=None, params=None):
        """Retrieve initiator group details from the array.

        :param initiator_group: the initaitor group name
        :param params: optional filter parameters
        :returns: initiator group dict, or None
        """
        return self.get_resource(
            SLOPROVISIONING, 'host', resource_name=initiator_group,
            params=params)

    def delete_initiator_group(self, initiatorgroup_name):
        """Delete an initiator group.

        :param initiatorgroup_name: initiator group name
        """
        self.delete_resource(
            SLOPROVISIONING, 'host', initiatorgroup_name)
        LOG.debug("Initiator Group successfully deleted.")

    def get_in_use_initiator_list_from_array(self):
        """Get the list of initiators which are in-use from the array.

        Gets the list of initiators from the array which are in
        hosts/ initiator groups.
        :returns: init_list
        """
        params = {'in_a_host': 'true'}
        return self.get_initiator_list(params)

    def get_initiator_group_from_initiator(self, initiator):
        """Given an initiator, get its corresponding initiator group, if any.

        :param initiator: the initiator id
        :returns: found_init_group_name -- string
        """
        found_init_group_name = None
        init_details = self.get_initiator(initiator)
        if init_details:
            found_init_group_name = init_details.get('host')
        else:
            LOG.error("Unable to retrieve initiator details for "
                      "%(init)s.", {'init': initiator})
        return found_init_group_name

    def get_initiator(self, initiator_id):
        """Retrieve initaitor details from the array.

        :param initiator_id: the initiator id
        :returns: initiator dict, or None
        """
        return self.get_resource(SLOPROVISIONING, 'initiator',
                                 resource_name=initiator_id)

    def get_initiator_list(self, params=None):
        """Retrieve initaitor list from the array.

        :param params: dict of optional params
        :returns: list of initiators
        """
        init_dict = self.get_resource(
            SLOPROVISIONING, 'initiator', params=params)
        try:
            init_list = init_dict['initiatorId']
        except KeyError:
            init_list = []
        return init_list

    def close_session(self):
        """Close the current rest session
        """
        self.rest_client.close_session()

    # Utils
    # Manipulate resources

    def get_resource(self, category, resource_type,
                     resource_name=None, params=None, private=''):
        """Get resource details from array.

        :param category: the resource category e.g. sloprovisioning
        :param resource_type: the resource type e.g. maskingview
        :param resource_name: the name of a specific resource
        :param params: query parameters
        :param private: empty string or '/private' if private url
        :returns: resource object -- dict or None
        """
        resource_object = None
        target_uri = self._build_uri(self.array_id, category, resource_type,
                                     resource_name, private)
        message, sc = self.rest_client.rest_request(target_uri, GET,
                                                    params=params)
        operation = 'get %(res)s' % {'res': resource_type}
        try:
            self.check_status_code_success(operation, sc, message)
        except Exception as e:
            LOG.debug("Get resource failed with %(e)s",
                      {'e': e})
        if sc == STATUS_200:
            resource_object = message
        return resource_object

    def create_resource(self, category, resource_type, payload,
                        private=''):
        """Create a provisioning resource.

        :param category: the category
        :param resource_type: the resource type
        :param payload: the payload
        :param private: empty string or '/private' if private url
        :returns: status_code -- int, message -- string, server response
        """
        target_uri = self._build_uri(self.array_id, category, resource_type,
                                     None, private)
        message, status_code = self.rest_client.rest_request(
            target_uri, POST, request_object=payload)
        operation = 'Create %(res)s resource' % {'res': resource_type}
        self.check_status_code_success(
            operation, status_code, message)
        return status_code, message

    def modify_resource(self, category, resource_type, payload,
                        version=U4V_VERSION, resource_name=None, private=''):
        """Modify a resource.

        :param version: the uv4 version
        :param category: the category
        :param resource_type: the resource type
        :param payload: the payload
        :param resource_name: the resource name
        :param private: empty string or '/private' if private url
        :returns: status_code -- int, message -- string (server response)
        """
        target_uri = self._build_uri(self.array_id, category, resource_type,
                                     resource_name, private, version)
        message, status_code = self.rest_client.rest_request(
            target_uri, PUT, request_object=payload)
        operation = 'modify %(res)s resource' % {'res': resource_type}
        self.check_status_code_success(operation, status_code, message)
        return status_code, message

    def delete_resource(
            self, category, resource_type, resource_name,
            payload=None, private='', params=None):
        """Delete a provisioning resource.

        :param category: the resource category e.g. sloprovisioning
        :param resource_type: the type of resource to be deleted
        :param resource_name: the name of the resource to be deleted
        :param payload: the payload, optional
        :param private: empty string or '/private' if private url
        :param params: dict of optional query params
        """
        target_uri = self._build_uri(self.array_id, category, resource_type,
                                     resource_name, private)
        message, status_code = self.rest_client.rest_request(
            target_uri, DELETE, request_object=payload, params=params)
        operation = 'delete %(res)s resource' % {'res': resource_type}
        self.check_status_code_success(operation, status_code, message)

    @staticmethod
    def _build_uri(array, category, resource_type,
                   resource_name=None, private='', version=U4V_VERSION):
        """Build the target url.

        :param array: the array serial number
        :param category: the resource category e.g. sloprovisioning
        :param resource_type: the resource type e.g. maskingview
        :param resource_name: the name of a specific resource
        :param private: empty string or '/private' if private url
        :returns: target url, string
        """
        target_uri = ('%(private)s/%(version)s/%(category)s/symmetrix/'
                      '%(array)s/%(resource_type)s'
                      % {'private': private, 'version': version,
                         'category': category, 'array': array,
                         'resource_type': resource_type})
        if resource_name:
            target_uri += '/%(resource_name)s' % {
                'resource_name': resource_name}
        return target_uri

    @staticmethod
    def check_status_code_success(operation, status_code, message):
        """Check if a status code indicates success.

        :param operation: the operation
        :param status_code: the status code
        :param message: the server response
        :raises: VolumeBackendAPIException
        """
        if status_code not in [STATUS_200, STATUS_201, STATUS_202, STATUS_204]:
            exception_message = (
                ('Error %(operation)s. The status code received '
                 'is %(sc)s and the message is %(message)s.')
                % {'operation': operation,
                   'sc': status_code, 'message': message})
            raise exception.VMAXPluginException(exception_message)

    def wait_for_job(self, operation, status_code, job):
        """Check if call is async, wait for it to complete.

        :param operation: the operation being performed
        :param status_code: the status code
        :param job: the job
        :returns: task -- list of dicts detailing tasks in the job
        :raises: VolumeBackendAPIException
        """
        task = None
        if status_code == STATUS_202:
            rc, result, status, task = self.wait_for_job_complete(job)
            if rc != 0:
                exception_message = (
                    ("Error %(operation)s. Status code: %(sc)lu. "
                     "Error: %(error)s. Status: %(status)s.")
                    % {'operation': operation, 'sc': rc,
                       'error': six.text_type(result),
                       'status': status})
                LOG.error(exception_message)
                raise exception.VMAXPluginException(exception_message)
        return task

    def wait_for_job_complete(self, job):
        """Given the job wait for it to complete.

        :param job: the job dict
        :returns: rc -- int, result -- string, status -- string,
                  task -- list of dicts detailing tasks in the job
        :raises: VolumeBackendAPIException
        """
        res, tasks = None, None
        if job['status'].lower == CREATED:
            try:
                res, tasks = job['result'], job['task']
            except KeyError:
                pass
            return 0, res, job['status'], tasks

        def _wait_for_job_complete():
            result = None
            # Called at an interval until the job is finished.
            retries = kwargs['retries']
            try:
                kwargs['retries'] = retries + 1
                if not kwargs['wait_for_job_called']:
                    is_complete, result, rc, status, task = (
                        self._is_job_finished(job_id))
                    if is_complete is True:
                        kwargs['wait_for_job_called'] = True
                        kwargs['rc'], kwargs['status'] = rc, status
                        kwargs['result'], kwargs['task'] = result, task
            except Exception:
                exception_message = "Issue encountered waiting for job."
                LOG.exception(exception_message)
                raise exception.VMAXPluginException(exception_message)

            if retries > 200:
                LOG.error("_wait_for_job_complete failed after "
                          "%(retries)d tries.", {'retries': retries})
                kwargs['rc'], kwargs['result'] = -1, result

                raise loopingcall.LoopingCallDone()
            if kwargs['wait_for_job_called']:
                raise loopingcall.LoopingCallDone()

        job_id = job['jobId']
        kwargs = {'retries': 0, 'wait_for_job_called': False,
                  'rc': 0, 'result': None}

        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_job_complete)
        timer.start(interval=3).wait()
        LOG.debug("Return code is: %(rc)lu. Result is %(res)s.",
                  {'rc': kwargs['rc'], 'res': kwargs['result']})
        return (kwargs['rc'], kwargs['result'],
                kwargs['status'], kwargs['task'])

    def _is_job_finished(self, job_id):
        """Check if the job is finished.

        :param job_id: the id of the job
        :returns: complete -- bool, result -- string,
                  rc -- int, status -- string, task -- list of dicts
        """
        complete, rc, status, result, task = False, 0, None, None, None
        job_url = "/%s/system/job/%s" % (U4V_VERSION, job_id)
        job = self._get_request(job_url, 'job')
        if job:
            status = job['status']
            try:
                result, task = job['result'], job['task']
            except KeyError:
                pass
            if status.lower() == SUCCEEDED:
                complete = True
            elif status.lower() in INCOMPLETE_LIST:
                complete = False
            else:
                rc, complete = -1, True
        return complete, result, rc, status, task

    def _get_request(self, target_uri, resource_type, params=None):
        """Send a GET request to the array.

        :param target_uri: the target uri
        :param resource_type: the resource type, e.g. maskingview
        :param params: optional dict of filter params
        :returns: resource_object -- dict or None
        """
        resource_object = None
        message, sc = self.rest_client.rest_request(target_uri, GET,
                                                    params=params)
        operation = 'get %(res)s' % {'res': resource_type}
        try:
            self.check_status_code_success(operation, sc, message)
        except Exception as e:
            LOG.debug("Get resource failed with %(e)s",
                      {'e': e})
        if sc == STATUS_200:
            resource_object = message
        return resource_object
