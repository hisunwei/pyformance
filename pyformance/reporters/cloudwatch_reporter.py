# -*- coding: utf-8 -*-

import logging
from .reporter import Reporter
import boto3

LOG = logging.getLogger(__name__)

class CloudWatchReporter(Reporter):

    """
    InfluxDB reporter using native http api
    (based on https://influxdb.com/docs/v1.1/guides/writing_data.html)
    """
    def __init__(self, application_name="", tags={},
                 registry=None, reporting_interval=5, clock=None,
                 aws_access_key_id="123", aws_secret_access_key="123", region="local"):
        super(CloudWatchReporter, self).__init__(
            registry, reporting_interval, clock)

        # Do not hard code credentials
        self.cloudwatch = boto3.client(
            'cloudwatch',
            # Hard coded strings as credentials, not recommended.
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name = region,
        )
        self.application_name = application_name

        self.dimensions = []
        for key in tags.keys():
            self.dimensions.append({"Name":key, "Value":tags[key]})

    def report_now(self, registry=None, timestamp=None):
        timestamp = timestamp or int(round(self.clock.time()))
        metric_data = self._collect_metrics(self.registry, timestamp)

        try:
            for i in range(0, len(metric_data), 20):
                self.cloudwatch.put_metric_data(MetricData=metric_data[i: i+20], Namespace=self.application_name)
        except IOError as (errno, strerror):
            LOG.warning("I/O error({0}): {1}, Cannot write to cloudwatch.".format(errno, strerror))
        except ValueError:
            LOG.warning("Could not convert data to an integer. Cannot write to cloudwatch.")
        except :
            LOG.warning("Cannot write to cloudwatch")

    def _collect_metrics(self, registry, timestamp=None):
        # timestamp = timestamp or int(round(self.clock.time())) # only ts in last 2weeks is allowed, timezone is important
        metrics = registry.dump_metrics()
        metrics_data = []
        for metric_key in metrics.keys():
            for metric_type in metrics[metric_key].keys():
                metrics_data.append({
                    'MetricName': "{0}.{1}".format(metric_key, metric_type),
                    # 'Timestamp': timestamp,
                    'Dimensions': self.dimensions,
                    'Unit': 'None',
                    'Value': metrics[metric_key][metric_type],
                })
        return metrics_data



