import json
from random import randint

import requests
from requests.auth import HTTPBasicAuth

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class RestRequest(object):

    def __init__(self, ip_address, port='8443'):
        self.ip = ip_address
        ip_port = ("%(ip)s:%(port)s" % {'ip': ip_address, 'port': port})
        self.session = self._establish_rest_session()
        self.base_url = ("http://%(ip_port)s"
                         % {'ip_port': ip_port})

    @staticmethod
    def _establish_rest_session():
        """Establish the rest session.

        :returns: requests.session() -- session, the rest session
        :raises: VolumeBackendAPIException
        """
        session = requests.session()
        session.headers = {'content-type': 'application/json',
                           'accept': 'application/json'}
        session.auth = HTTPBasicAuth('smc', 'smc')
        session.verify = False

        return session

    def rest_request(self, target_url, method, params=None, request_object=None
                     ):
        """Sends a request (GET, POST, PUT, DELETE) to the target api.

        :param target_url: target url (string)
        :param method: The method (GET, POST, PUT, or DELETE)
        :param params: Additional URL parameters
        :param request_object: request payload (dict)
        :return: server response object (dict)
        """
        message = None
        status_code = None
        if not self.session:
            self._establish_rest_session()
        url = ("%(self.base_url)s%(target_url)s" %
               {'self.base_url': self.base_url,
                'target_url': target_url})
        try:
            if request_object:
                response = self.session.request(
                    method=method, url=url,
                    data=json.dumps(request_object, sort_keys=True,
                                    indent=4))
            elif params:
                response = self.session.request(method=method, url=url,
                                                params=params)
            else:
                response = self.session.request(method=method, url=url)
            status_code = response.status_code
            try:
                message = response.json()
            except ValueError:
                print("No response received from API. Status code "
                      "received is: %(status_code)s" %
                      {'status_code': status_code})
                message = None
            #print("\n")
            print("==========================================================")
            print("%(method)s request to \n%(url)s \nhas returned with a "
                  "status code of: %(status_code)s. " %
                  {'method': method, 'url': target_url,
                   'status_code': status_code})
            if response:
                print("Response is")
                print(json.dumps(message, indent=4, sort_keys=True))
                print("\n")

        except requests.Timeout:
            print("The %(method)s request to URL \n%(url)s\n"
                  "timed-out, but may have been successful."
                  "Please check the array. " % {'method': method, 'url': target_url})
        except Exception as e:
            print("The %(method)s request to URL %(url)s\n"
                  "failed with exception %(e)s"
                  % {'method': method, 'url': target_url, 'e': e})

        return status_code, message


rr = RestRequest(ip_address='127.0.0.1', port='8000')


def test_activate():
    url = '/Plugin.Activate'
    return rr.rest_request(url, 'POST')


def test_create():
    url = '/VolumeDriver.Create'
    volume_name = 'test_volume' + str(randint(1000, 100000))
    req_object = {
        "Name": volume_name,
        "Opts": {
            "size": 3
        }
    }
    return rr.rest_request(url, 'POST', request_object=req_object)


def test_remove(vol_name):
    url = '/VolumeDriver.Remove'
    req_object = {
        "Name": vol_name
    }
    return rr.rest_request(url, 'POST', request_object=req_object)

def test_get(vol_name):
    url = '/VolumeDriver.Get'
    req_object = {
        "Name": vol_name
    }
    return rr.rest_request(url, 'POST', request_object=req_object)

#ret = test_create()
#ret = test_remove("test_volume61442")
ret = test_get('test_volume54473')
print(ret)