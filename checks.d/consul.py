# 3p
import requests

# project
from checks import AgentCheck

"""
Number of nodes in the cluster - /v1/catalog/nodes
Number of services in the cluster - /v1/catalog/services
Number of nodes in a given service - /v1/catalog/service/<service>
Number of services running per node - /v1/catalog/node/<node>
"""

class ConsulCheck(AgentCheck):
    CONSUL_CHECK = 'consul.up'
    HEALTH_CHECK = 'consul.check'

    def consul_request(self, instance, endpoint):
        url = "{}{}".format(instance.get('url'), endpoint)
        resp = requests.get(url)

        resp.raise_for_status()
        return resp.json()

    def should_check(self, instance):
        try:
            local_config = self.consul_request(instance, '/v1/agent/self')
            agent_addr = local_config.get('Config', {}).get('AdvertiseAddr')
            agent_port = local_config.get('Config', {}).get('Ports', {}).get('Server')
            agent_url = "{}:{}".format(agent_addr, agent_port)

            leader = self.consul_request(instance, '/v1/status/leader')
            return agent_url == leader

        except Exception as e:
            return False

    def get_services_in_cluster(self, instance):
        services = self.consul_request(instance, '/v1/catalog/services')
        return services

    def check(self, instance):
        if not self.should_check(instance):
            self.log.debug("Skipping check for this instance")
            return

        service_check_tags = ['consul_url:{}'.format(instance.get('url'))]

        try:

            health_state = self.consul_request(instance, '/v1/health/state/any')

            STATUS_SC = {
                'passing': AgentCheck.OK,
                'warning': AgentCheck.WARNING,
                'critical': AgentCheck.CRITICAL,
            }

            for check in health_state:
                status = STATUS_SC.get(check['Status'])
                if status is None:
                    continue

                tags = ["check:{}".format(check["CheckID"])]
                if check["ServiceName"]:
                    tags.append("service:{}".format(check["ServiceName"]))
                if check["ServiceID"]:
                    tags.append("service-id:{}".format(check["ServiceID"]))

            services = self.get_services_in_cluster(instance)
            self.service_check(self.HEALTH_CHECK, status, tags=tags)

        except Exception as e:
            self.service_check(self.CONSUL_CHECK, AgentCheck.CRITICAL,
                               tags=service_check_tags)
        else:
            self.service_check(self.CONSUL_CHECK, AgentCheck.OK,
                               tags=service_check_tags)
