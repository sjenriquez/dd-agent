from nose.plugins.attrib import attr

from checks import AgentCheck
from tests.common import AgentCheckTest


@attr(requires='fluentd')
class TestFluentd(AgentCheckTest):
    CHECK_NAME = 'fluentd'
    CHECK_GAUGES = ['retry_count', 'buffer_total_queued_size', 'buffer_queue_length']

    def __init__(self, *args, **kwargs):
        AgentCheckTest.__init__(self, *args, **kwargs)
        self.config = {
            "instances": [
                {
                    "monitor_agent_url": "http://localhost:24220/api/plugins.json",
                    "plugin_ids": ["plg1"],
                }
            ]
        }

        self.agentConfig = {
            'version': '0.1',
            'api_key': 'toto'
        }

    def test_fluentd(self):
        self.run_check(self.config, agent_config=self.agentConfig)
        self.assertServiceCheckOK(self.check.SERVICE_CHECK_NAME,
                                  tags=['fluentd_host:localhost', 'fluentd_port:24220'])
        for m in self.metrics:
            if m[0] in self.CHECK_GAUGES:
                self.assertEquals(m[2], 0)
            self.assertEquals(m[3]['type'], 'gauge')
            self.assertEquals(m[3]['tags'], ['plugin_id:plg1'])

        self.assertEquals(len(self.metrics), 3)

        service_checks_count = len(self.service_checks)
        self.assertTrue(isinstance(self.service_checks, list))
        self.assertTrue(service_checks_count > 0)

        is_ok = [sc for sc in self.service_checks if sc['check'] == self.check.SERVICE_CHECK_NAME]
        self.assertEquals(len(is_ok), 1)
        self.assertEquals(
            set(is_ok[0]['tags']), set(['fluentd_host:localhost', 'fluentd_port:24220']))

    def test_fluentd_exception(self):
        self.assertRaises(Exception, lambda: self.run_check({"instances": [{
            "monitor_agent_url": "http://localhost:24222/api/plugins.json",
            "plugin_ids": ["plg2"]}]}))

        self.assertServiceCheckCritical(self.check.SERVICE_CHECK_NAME,
                                        tags=['fluentd_host:localhost', 'fluentd_port:24222'])
