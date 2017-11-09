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
from oslo_log import log as logging
import json
import requests
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

LOG = logging.getLogger(__name__)


class RestRequests:

    def __init__(self, username, password, verify, base_url):
        self.username = username
        self.password = password
        self.verifySSL = verify
        self.base_url = base_url
        self.headers = {'content-type': 'application/json',
                        'accept': 'application/json'}
        self.session = self.establish_rest_session()

    def establish_rest_session(self):
        session = requests.session()
        session.headers = self.headers
        session.auth = HTTPBasicAuth(self.username, self.password)
        session.verify = self.verifySSL
        return session

    def rest_request(self, target_url, method, params=None,
                     request_object=None):
        """Sends a request (GET, POST, PUT, DELETE) to the target api.

        :param target_url: target url (string)
        :param method: The method (GET, POST, PUT, or DELETE)
        :param params: Additional URL parameters
        :param request_object: request payload (dict)
        :return: server response object (dict), status code
        """
        response, status_code = None, None
        if not self.session:
            self.establish_rest_session()
        url = ("%(self.base_url)s%(target_url)s" %
               {'self.base_url': self.base_url,
                'target_url': target_url})
        try:
            if request_object:
                response = self.session.request(
                    method=method, url=url, timeout=120,
                    data=json.dumps(request_object, sort_keys=True,
                                    indent=4))
            elif params:
                response = self.session.request(method=method, url=url,
                                                params=params, timeout=60)
            else:
                response = self.session.request(method=method, url=url)
            status_code = response.status_code
            try:
                response = response.json()
            except ValueError:
                LOG.debug("No response received from API. Status code "
                          "received is: %(status_code)s" %
                          {'status_code': status_code})
                response = None
            LOG.debug("%(method)s request to %(url)s has returned with "
                      "a status code of: %(status_code)s"
                      % {'method': method, 'url': url,
                         'status_code': status_code})

        except (requests.Timeout, requests.ConnectionError) as e:
            LOG.error(("The %(method)s request to URL %(url)s "
                       "timed-out, but may have been successful."
                       "Please check the array. Exception received:"
                       "%(e)s.")
                      % {'method': method, 'url': url, 'e': e})
        except Exception as e:
                LOG.error(("The %(method)s request to URL %(url)s "
                           "failed with exception %(e)s")
                          % {'method': method, 'url': url, 'e': e})
                raise
        return response, status_code
            
    def close_session(self):
        """
        Close the current rest session
        """
        return self.session.close()
