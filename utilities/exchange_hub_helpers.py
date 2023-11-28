#
# Copyright 2020, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json

import requests
import logging


# from http.client import HTTPConnection
# HTTPConnection.debuglevel = 0
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# rootlogger = logging.getLogger('root')
# errlogger = logging.getLogger('root.err')

# from python_settings import settings
import settings
import logging
logging.getLogger("requests").setLevel(logging.WARNING)

def post(url, data):
    result =  requests.post(url, data=data)
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return result

def create_exchange():
    url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges?dataExchangeId=nci_idc_bigquery_data_exchange'
    data = {
        "displayName": "nci-idc-bigquery-data-exchange",
        "description": "Exchange for publication of NCI IDC BQ datasets",
        "primaryContact": "bcliffor@systemsbiology.org"
    }

    headers = {
        "Authorization": "Bearer ya29.c.c0AY_VpZisbyPKWvyeQ_QNZzxGFgDacflLr-EbnrTsKpBSi_zu7gf48tpXFKEAxTkKWTQLimYEBvjNtgXVr6yfN89neU3K4OCMYKPyUkv5jceS8_LCbRewn3QSEEld8eTXp9HbnWySdeBrHDgO34bMRh5CcTfIK7CYFW-Ugj4sRLGcGZdvtd6mpeNkqwvIRXCbudZcjTglKSFx__M6f62bdbl58O0t7OTQ5emrX6Bsr_oWcyopgLhX9HkyR4wC7FsNo2lBS_DnGy9omDy7Et0ClCTWwnp6h81TkJnwxw6toUGJqOvHhU1tU6M8FifjTvsdAVNoAh7APT6C5sSYDxRUWoGMFfWHWKgLeByZkdXSxy2vSxv7OB2K4S10xCj8RcrtWp5MAQE399CBgkW00nJOs_j-qwW8mV99oeBZltzinXcWnpOhYQn97tVsufUJft8Z7wpZprrUo0aSu00ygIf8FoQ9BS--X8h1meaxwsVXtfydQOhccrfz-r2dnkxu_pryfngvX8l8ca4Qhvtg-8vVZ6fV4gOiBOi4wIt8dS8bb2sWnvQq4nZ3d_rnhiOo6JzjFSYaXW4Xxx_moV4555Shpd9nF6_4MaIhbpSeJUqa3MYhYm_ZknSQ2SfBd2cp1dhvgr-fBM1rZ1ZdSvco70Qj6kw3boeh2k_UrViml8mMQ1ikuWg0S5e3z1OFcm7ZnIqv5sjiI64FuOoJMeo56loqW74IQ4an-nywjnpF898iJ12gylV3Z0ynQb6wWicIkWUp2est529ptv3yb5ewF23B90B1JXt4Zn3xfh9pFrIa4Vu-qhyyR_Rk1VqehpnYzQ5Qhsd1u6q9eRl5-iwQj9BokSb5-4pQvx9vwzWmfr96wMS8_4B_niOXJ5mYe7y0I6Wq-V15UI5kxVqhX29aukI2RtV0JothZIyXY5t_QOcaYIQjMjp4cVk3_t9bhd3RVhWmveZUt8xBSMWrIdoB0ngijMZdsbBgfRVQunMWtn7kR9myrQu7wu19p08"
    }
    result =  requests.post(url, data=json.dumps(data), headers=headers)
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return (response)

if __name__ == "__main__":
    r = create_exchange()


