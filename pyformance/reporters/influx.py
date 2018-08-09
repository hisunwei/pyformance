# -*- coding: utf-8 -*-

import base64
import logging
try:
    from urllib2 import quote, urlopen, Request, URLError
except ImportError:
    from urllib.error import URLError
    from urllib.parse import quote
    from urllib.request import urlopen, Request

from .reporter import Reporter

LOG = logging.getLogger(__name__)

DEFAULT_INFLUX_SERVER = '127.0.0.1'
DEFAULT_INFLUX_PORT = 8086
DEFAULT_INFLUX_DATABASE = "metrics"
DEFAULT_INFLUX_USERNAME = None
DEFAULT_INFLUX_PASSWORD = None
DEFAULT_INFLUX_PROTOCOL = "http"

class InfluxReporter(Reporter):

    """
    InfluxDB reporter using native http api
    (based on https://influxdb.com/docs/v1.1/guides/writing_data.html)
    metrics_tag_keys for some customize tag for some metric, like url, http status code or some dynamic error code
    that is not convinent to batch query in inlfuxdb if you put url/code like metric into table name.
    instead we use influxdb tag feature to support this
    while name a matric use a prefix '^' to mark the tags, use '_' to split the  metric name and tag values, ex: "^_httpRequest_url1_200"
    a metrics tag in metrics_tag_keys is need, ex:  metrics_tag_keys = {"httpRequest": ["url", "status"]},
    """
    def __init__(self, registry=None, reporting_interval=5, prefix="",tags={},
                 metrics_tag_keys={}, metrics_tag_keys_prefix="^", metrics_tag_split="_",
                 database=DEFAULT_INFLUX_DATABASE, server=DEFAULT_INFLUX_SERVER,
                 username=DEFAULT_INFLUX_USERNAME,
                 password=DEFAULT_INFLUX_PASSWORD,
                 port=DEFAULT_INFLUX_PORT, protocol=DEFAULT_INFLUX_PROTOCOL,
                 autocreate_database=False, clock=None):
        self.tags_str = ",".join(["%s=%s" % (k, tags[k]) for k in tags.keys()])
        super(InfluxReporter, self).__init__(
            registry, reporting_interval, clock)
        self.prefix = prefix
        self.metrics_tag_keys = metrics_tag_keys
        self.metrics_tag_keys_prefix = metrics_tag_keys_prefix
        self.metrics_tag_split = metrics_tag_split
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.protocol = protocol
        self.server = server
        self.autocreate_database = autocreate_database
        self._did_create_database = False

    def _create_database(self):
        url = "%s://%s:%s/query" % (self.protocol, self.server, self.port)
        q = quote("CREATE DATABASE %s" % self.database)
        request = Request(url + "?q=" + q)
        if self.username:
            auth = _encode_username(self.username, self.password)
            request.add_header("Authorization", "Basic %s" % auth.decode('utf-8'))
        try:
            response = urlopen(request)
            _result = response.read()
            # Only set if we actually were able to get a successful response
            self._did_create_database = True
        except URLError as err:
            LOG.warning("Cannot create database %s to %s: %s",
                        self.database, self.server, err.reason)

    def report_now(self, registry=None, timestamp=None):
        if self.autocreate_database and not self._did_create_database:
            self._create_database()
        timestamp = timestamp or int(round(self.clock.time()))
        metrics = (registry or self.registry).dump_metrics()
        post_data = []
        for key, metric_values in metrics.items():
            if not self.prefix:
                table = key
            else:
                table = "%s.%s" % (self.prefix, key)

            if str(key).startswith(self.metrics_tag_prefix + self.metrics_tag_split):
                slices = str(key).split(self.metrics_tag_split)
                #0 is ^ prefix,  1 is metric name, left is tag values
                if len(slices) >= 0:
                    metric_name = slices[1]
                    tag_keys = self.metrics_tag_keys[metric_name]
                    if len(slices) == len(tag_keys) +1 :
                        metric_tags_str = ",".join(["%s=%s" % (slices[i+1], tag_keys[i]) for i in range(0, len(tag_keys))])
                        table = "%s,%s" % (table, metric_tags_str)
                    else:
                        LOG.warn("skip metric tags parse error, metric key:" + str(key))
                        continue
                else:
                    LOG.warn("skip metric tags parse error, metric key:" + str(key))
                    continue

            values = ",".join(["%s=%s" % (k, v if type(v) is not str \
                                               else '"{}"'.format(v))
                              for (k, v) in metric_values.items()])
            if self.tags_str.__len__()>0:
                table = "%s,%s" % (table, self.tags_str)

            line = "%s %s %s" % (table, values, timestamp)
            post_data.append(line)
        post_data = "\n".join(post_data)
        path = "/write?db=%s&precision=s" % self.database
        url = "%s://%s:%s%s" % (self.protocol, self.server, self.port, path)
        request = Request(url, post_data.encode("utf-8"))
        if self.username:
            auth = _encode_username(self.username, self.password)
            request.add_header("Authorization", "Basic %s" % auth.decode('utf-8'))
        try:
            response = urlopen(request)
            response.read()
        except URLError as err:
            LOG.warning("Cannot write to %s: %s",
                        self.server, err.reason)


def _encode_username(username, password):
    auth_string = ('%s:%s' % (username, password)).encode()
    return base64.b64encode(auth_string)
