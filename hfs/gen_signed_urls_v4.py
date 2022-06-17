#
# Copyright 2015-2021, Institute for Systems Biology
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

import datetime
import time
import json
from google.cloud import storage
import argparse
import binascii
import collections
import datetime
import hashlib
import sys
from multiprocessing import Process, Queue
from google.oauth2 import service_account
import six
from six.moves.urllib.parse import quote


def generate_signed_url(service_account_file, bucket_name, object_name,
                        subresource=None, expiration=604800, http_method='GET',
                        query_parameters=None, headers=None):

    if expiration > 604800:
        print('Expiration Time can\'t be longer than 604800 seconds (7 days).')
        sys.exit(1)

    escaped_object_name = quote(six.ensure_binary(object_name), safe=b'/~')
    canonical_uri = '/{}'.format(escaped_object_name)

    datetime_now = datetime.datetime.now(tz=datetime.timezone.utc)
    request_timestamp = datetime_now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = datetime_now.strftime('%Y%m%d')

    google_credentials = service_account.Credentials.from_service_account_file(
        service_account_file)
    client_email = google_credentials.service_account_email
    credential_scope = '{}/auto/storage/goog4_request'.format(datestamp)
    credential = '{}/{}'.format(client_email, credential_scope)

    if headers is None:
        headers = dict()
    host = '{}.storage.googleapis.com'.format(bucket_name)
    headers['host'] = host

    canonical_headers = ''
    ordered_headers = collections.OrderedDict(sorted(headers.items()))
    for k, v in ordered_headers.items():
        lower_k = str(k).lower()
        strip_v = str(v).lower()
        canonical_headers += '{}:{}\n'.format(lower_k, strip_v)

    signed_headers = ''
    for k, _ in ordered_headers.items():
        lower_k = str(k).lower()
        signed_headers += '{};'.format(lower_k)
    signed_headers = signed_headers[:-1]  # remove trailing ';'

    if query_parameters is None:
        query_parameters = dict()
    query_parameters['X-Goog-Algorithm'] = 'GOOG4-RSA-SHA256'
    query_parameters['X-Goog-Credential'] = credential
    query_parameters['X-Goog-Date'] = request_timestamp
    query_parameters['X-Goog-Expires'] = expiration
    query_parameters['X-Goog-SignedHeaders'] = signed_headers
    if subresource:
        query_parameters[subresource] = ''

    canonical_query_string = ''
    ordered_query_parameters = collections.OrderedDict(
        sorted(query_parameters.items()))
    for k, v in ordered_query_parameters.items():
        encoded_k = quote(str(k), safe='')
        encoded_v = quote(str(v), safe='')
        canonical_query_string += '{}={}&'.format(encoded_k, encoded_v)
    canonical_query_string = canonical_query_string[:-1]  # remove trailing '&'

    canonical_request = '\n'.join([http_method,
                                   canonical_uri,
                                   canonical_query_string,
                                   canonical_headers,
                                   signed_headers,
                                   'UNSIGNED-PAYLOAD'])

    canonical_request_hash = hashlib.sha256(
        canonical_request.encode()).hexdigest()

    string_to_sign = '\n'.join(['GOOG4-RSA-SHA256',
                                request_timestamp,
                                credential_scope,
                                canonical_request_hash])

    # signer.sign() signs using RSA-SHA256 with PKCS1v15 padding
    signature = binascii.hexlify(
        google_credentials.signer.sign(string_to_sign)
    ).decode()

    scheme_and_host = '{}://{}'.format('https', host)
    signed_url = '{}{}?{}&x-goog-signature={}'.format(
        scheme_and_host, canonical_uri, canonical_query_string, signature)

    return signed_url

def generate_download_signed_url_v4(storage_client, bucket, blob):
    """Generates a v4 signed URL for downloading a blob.

    Note that this method requires a service account key file. You can not use
    this if you are using Application Default Credentials from Google Compute
    Engine or from the Google Cloud SDK.
    """
    # bucket_name = 'your-bucket-name'
    # blob_name = 'your-object-name'

    url = blob.generate_signed_url(
        version="v4",
        # This URL is valid for 15 minutes
        expiration=datetime.timedelta(minutes=15),
        # Allow GET requests using this URL.
        method="GET",
    )

    return url


def worker(input, args):
    client = storage.Client(credentials=service_account.Credentials.from_service_account_file(args.creds))
    bucket = client.bucket(args.bucket_name)

    for blob_names in iter(input.get, 'STOP'):
        for blob_name in blob_names:
            blob = bucket.blob(blob_name)
            # url = generate_signed_url(args.creds, bucket, blob)
            # url = generate_signed_url(host, google_credentials, canonical_query_string, canonical_request, blob.name)
            url = blob.generate_signed_url(
                version="v4",
                # This URL is valid for 15 minutes
                expiration=datetime.timedelta(minutes=15),
                # Allow GET requests using this URL.
                method="GET",
            )


def get_blob_names(client, args, object_name):
    jsn = json.loads(client.bucket(args.bucket_name).blob(object_name).download_as_text())
    instance_uids = jsn['instances']['SOPInstanceUIDs']
    uuid = jsn['uuid']
    blob_names = [f'{uuid}/{instance_uid}.dcm' for instance_uid in instance_uids]
    return blob_names


def generate_signed_urls_mp(args):
    storage_client = storage.Client(credentials=None)
    processes = []
    # Create queues
    task_queue = Queue()

    num_processes = args.num_processes
    if num_processes == 0:
        client = storage.Client(credentials=service_account.Credentials.from_service_account_file(args.creds))
        bucket = client.bucket(args.bucket_name)
        n=0
        start = time.time()
        for i in range(args.repeat):
            if True:
                for object in args.object_names:
                    blob_names = get_blob_names(storage_client, args, object)
                    offset = 0
                    while offset < len(blob_names):
                        page = blob_names[offset:offset + args.batch]
                        offset += args.batch
                        for blob_name in page:
                            blob = bucket.blob(blob_name)
                            # url = generate_signed_url(args.creds, bucket, blob)
                            # url = generate_signed_url(host, google_credentials, canonical_query_string, canonical_request, blob.name)
                            url = blob.generate_signed_url(
                                version="v4",
                                # This URL is valid for 15 minutes
                                expiration=datetime.timedelta(minutes=15),
                                # Allow GET requests using this URL.
                                method="GET",
                            )
                    n += len(blob_names)
            else:
                pages = storage_client.list_blobs(args.bucket_name, prefix=args.folder, delimiter='/', page_size=args.batch)
                for page in pages.pages:
                    blob_names = [blob.name for blob in page if blob.name.endswith('dcm')]
                    task_queue.put(blob_names)
                n += len(blob_names)
        print(f'Duration: {time.time() - start}, n: {n}')


    else:
        # Start worker processes
        for process in range(num_processes):
            args.pid = process + 1
            processes.append(
                Process(target=worker, args=(task_queue, args)))
            processes[-1].start()

        start = time.time()
        n=0
        for i in range(args.repeat):
            if True:
                for object in args.object_names:
                    blob_names = get_blob_names(storage_client, args, object)
                    offset = 0
                    while offset < len(blob_names):
                        page = blob_names[offset:offset + args.batch]
                        offset += args.batch
                        task_queue.put(page)
                    n += len(blob_names)
            else:
                pages = storage_client.list_blobs(args.bucket_name, prefix=args.folder, delimiter='/', page_size=args.batch)
                for page in pages.pages:
                    blob_names = [blob.name for blob in page if blob.name.endswith('dcm')]
                    task_queue.put(blob_names)
                n += len(blob_names)

        # Tell child processes to stop
        for process in processes:
            task_queue.put('STOP')

        # Wait for them to stop
        for process in processes:
            process.join()

        print(f'Duration: {time.time() - start}, n: {n}, rate: {n/(time.time() - start):.2f}')


if __name__ == '__main__':
    client = storage.Client()
    parser = argparse.ArgumentParser()
    parser.add_argument('--creds', default='/home/bcliffor/cred.json')
    # parser.add_argument('--creds', default='/Users/BillClifford/Downloads/idc-dev-etl-0a66ec01ec76.json')
    parser.add_argument('--bucket_name', default='whc_dev')
    parser.add_argument('--object_names', default=[
        '75472328-7a9a-4261-a611-04332f929f14.idc',
        '35d333a1-2ac1-4dd5-94c3-cf9f139bb3ff.idc',
        '3af21e94-db21-4644-93d3-29275bace391.idc',
        '0a0e1704-c688-41ec-b58c-64da1aacac12.idc',
        'c645edae-8552-4a53-a652-2d72facccc3b.idc',
        'b381675b-cf36-4fa6-a757-83f2d4359d2e.idc',
        '7450f285-be83-401d-862d-5dc9a3332aa8.idc',
        '6c0f8c79-4443-4e80-81a4-493c35d7650b.idc',
        '71be0b45-395a-4574-8fd2-c8f340552790.idc'
    ])
    parser.add_argument('--folder', default='75472328-7a9a-4261-a611-04332f929f14/')
    parser.add_argument('--batch', default=100)
    parser.add_argument('--num_processes', default=0)
    parser.add_argument('--repeat', default=8)
    args = parser.parse_args()

    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    generate_signed_urls_mp(args)



