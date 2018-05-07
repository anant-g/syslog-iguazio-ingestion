#!/usr/bin/python
import socket
import sys
import grequests
import json
import collections
import logging
import yaml

logger = None

with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
    IGZ_USER = cfg['iguazio']['user']
    IGZ_PASS = cfg['iguazio']['pwd']
    SYSLOG_KV = cfg['iguazio']['syslog_kv']
    NGINX_HOST = cfg['iguazio']['nginx_host']
    NGINX_PORT = cfg['iguazio']['nginx_port']
    CONTAINER_ID = cfg['iguazio']['container_id']
    UDP_IP = cfg['syslog']['host']
    UDP_PORT = cfg['syslog']['port']




def ingest_syslog():
    init_logger()  # initialize logger
    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    try:
        while True:
            data, addr = sock.recvfrom(4096)  # buffer size is 4096 bytes
            data_json = json.loads(data)
            kv_rec = json.dumps(get_structured_message_syslog(data_json))
            send_records_raw(kv_rec)

    finally:
        print >>sys.stderr, 'closing socket'
        sock.close()


def send_records_raw(record):
    async_tasks = []
    payload = record
    headers = get_function_headers('PutItem')
    url = get_kv_url(SYSLOG_KV)
    task = grequests.post(url, auth=(IGZ_USER, IGZ_PASS), data=payload, headers=headers, timeout=1, callback=handle_response)
    async_tasks.append(task)
    send_to_kv(async_tasks)


def get_function_headers(v3io_function):
    return {'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'X-v3io-function': v3io_function
            }


def get_kv_url(kv_name):
    return 'http://{0}:{1}/{2}/{3}/'.format(NGINX_HOST, NGINX_PORT, CONTAINER_ID, kv_name)


def handle_exception(request, exception):
    # print request, "::failed with exception::", exception
    logger.error("failed with exception %s", str(exception))


def handle_response(response, **kwargs):
    try:
        if response.status_code != 204:
            logger.error("ingestion failed for record %s with status code %s", response.request.body, response.status_code)
        # print 'status: {0}'.format(response.status_code)
    except Exception, e:
        # print "ERROR: {0}".format(str(e))
        logger.error(str(e))


def send_to_kv(async_tasks):
    print "sending requests"
    grequests.map(async_tasks, size=100, exception_handler=handle_exception)
    print "requests sent"


def get_items_ordered_collection(msg):
    coll = collections.OrderedDict()
    coll['version'] = {'S': msg['@version']}
    coll['message'] = {'S': msg['message']}
    coll['sysloghost'] = {'S': msg['sysloghost']}
    coll['severity'] = {'S': msg['severity']}
    coll['facility'] = {'S': msg['facility']}
    coll['programname'] = {'S': msg['programname']}
    coll['procid'] = {'S': msg['procid']}

    return coll


def get_structured_message_syslog(rec):
    item_dict = get_items_ordered_collection(rec)
    json_dict = collections.OrderedDict([
        ('Key', collections.OrderedDict([('id', {'S': rec['@timestamp']})])),
        ('Item', item_dict)
    ])
    return json_dict


def init_logger():
    global logger
    if logger is None:
        logger = logging.getLogger('ingest syslog')
    handler = logging.FileHandler('logs/requests.log')

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)


ingest_syslog()
