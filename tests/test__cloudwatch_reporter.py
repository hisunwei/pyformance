import mock

from pyformance.reporters.cloudwatch_reporter import CloudWatchReporter
from pyformance import MetricsRegistry
from tests import TimedTestCase

AWS_ACCESS_KEY_ID='my_abc_id'
AWS_SECRET_ACCESS_KEY='my_abc_key'
REGION="my_region"

class TestCloudWatchReporter(TimedTestCase):
    def setUp(self):
        super(TestCloudWatchReporter, self).setUp()
        self.registry = MetricsRegistry(clock=self.clock)
        self.maxDiff = None

    def tearDown(self):
        super(TestCloudWatchReporter, self).tearDown()

    def test_report_now(self):
        r = CloudWatchReporter(application_name="app", tags={"host":"localhost"},
                               registry=self.registry, reporting_interval=1, clock=self.clock,
                               aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region=REGION)

        h1 = self.registry.histogram("hist")
        for i in range(10):
            h1.add(2 ** i)
        t1 = self.registry.timer("t1")
        m1 = self.registry.meter("m1")
        m1.mark()
        with t1.time():
            c1 = self.registry.counter("c1")
            c2 = self.registry.counter("counter-2")
            c1.inc()
            c2.dec()
            c2.dec()
            self.clock.add(1)
        output = r._collect_metrics(registry=self.registry)
        self.assertEqual(len(output), 31)
        for data in output:
            assert data['metric'].startswith("prefix.")

    def test_send_request(self):
        r = CloudWatchReporter(application_name="app", tags={"host":"localhost"},
                               registry=self.registry, reporting_interval=1, clock=self.clock,
                               aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region="ap-south-1")
        h1 = self.registry.histogram("hist")
        for i in range(10):
            h1.add(2 ** i)
        t1 = self.registry.timer("t1")
        m1 = self.registry.meter("m1")
        m1.mark()
        with t1.time():
            c1 = self.registry.counter("c1")
            c2 = self.registry.counter("counter-2")
            c1.inc()
            c2.dec()
            c2.dec()
            self.clock.add(1)
        r.report_now()
        # with mock.patch("pyformance.reporters.cloudwatch_reporter.botocore.client.CloudWatch.put_metric_data") as patch:
        with mock.patch("pyformance.reporters.cloudwatch_report.CloudWatchReporter._collect_metrics") as patch:
            r.report_now()
            patch.assert_called()
