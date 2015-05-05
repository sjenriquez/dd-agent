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

    CONSUL_CATALOG_CHECK = 'consul.catalog'
    CONSUL_NODE_CHECK = 'consul.node'

    def consul_request(self, instance, endpoint):
        url = "{}{}".format(instance.get('url'), endpoint)
        try:
            resp = requests.get(url)
        except requests.exceptions.Timeout:
            self.log.exception('Consul request to {0} timed out'.format(url))
            raise

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

    # Problem these are all blocking HTTP requests and will, they should not be
    def get_services_in_cluster(self, instance):
        return self.consul_request(instance, '/v1/catalog/services')

    def get_nodes_in_cluster(self, instance):
        return self.consul_request(instance, '/v1/catalog/nodes')

    def get_nodes_with_service(self, instance, service):
        return self.consul_request(instance, '/v1/catalog/service/{0}'.format(service))

    def get_services_on_node(self, instance, node):
        return self.consul_request(instance, '/v1/catalog/node/{0}'.format(node))

    def check(self, instance):
        if not self.should_check(instance):
            self.log.debug("Skipping check for this instance")
            return

        service_check_tags = ['consul_url:{}'.format(instance.get('url'))]
        perform_catalog_checks = instance.get('perform_catalog_checks',
                                              self.init_config.get('perform_catalog_checks'))


        global_catalog_check = perform_catalog_checks.get('global')
        services_to_check = perform_catalog_checks.get('services')
        nodes_to_check = perform_catalog_checks.get('nodes')

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

        if perform_catalog_checks:
            if global_catalog_check:
                services = self.get_services_in_cluster(instance)
                self.gauge('{0}.services_up'.format(self.CONSUL_CATALOG_CHECK), len(services))

                nodes = self.get_nodes_in_cluster(instance)
                self.gauge('{0}.nodes_up'.format(self.CONSUL_CATALOG_CHECK), len(nodes))

            if services_to_check:
                for s in services_to_check:
                    nodes_providing_s = self.get_nodes_with_service(instance, s)
                    metric_key = '{0}.{1}.nodes_up'.format(self.CONSUL_CATALOG_CHECK, s)
                    self.gauge(metric_key, len(nodes_providing_s))

            if nodes_to_check:
                for n in nodes_to_check:
                    services_provided_on_n = self.get_services_on_node(instance, n)
                    metric_key = '{0}.services_up'.format(self.CONSUL_NODE_CHECK)
                    self.gauge(metric_key, len(services_provided_on_n))
