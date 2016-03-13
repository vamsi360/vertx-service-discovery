/*
 * Copyright (c) 2016 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.dropwizard.discovery.bundle;

import com.flipkart.ranger.ServiceProviderBuilders;
import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.flipkart.ranger.serviceprovider.ServiceProvider;
import com.google.common.collect.Lists;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.discovery.client.io.dropwizard.ranger.ServiceDiscoveryClient;
import io.dropwizard.discovery.common.ShardInfo;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.net.InetAddress;
import java.util.List;

/**
 * A dropwizard bundle for service discovery.
 */
@Slf4j
public abstract class ServiceDiscoveryBundle<T extends Configuration> implements ConfiguredBundle<T> {

    private ServiceDiscoveryConfiguration serviceDiscoveryConfiguration;
    private List<Healthcheck> healthchecks = Lists.newArrayList();
    private ServiceProvider<ShardInfo> serviceProvider;

    @Getter
    private ServiceDiscoveryClient serviceDiscoveryClient;

    protected ServiceDiscoveryBundle() {

    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {

    }

    @Override
    public void run(T configuration, Environment environment) throws Exception {
        serviceDiscoveryConfiguration = getRangerConfiguration(configuration);
        val objectMapper = environment.getObjectMapper();
        CuratorFramework curator = CuratorFrameworkFactory.newClient(
                                            serviceDiscoveryConfiguration.getZookeeper(),
                                            new RetryForever(serviceDiscoveryConfiguration.getConnectionRetryInterval()));
        final String namespace = serviceDiscoveryConfiguration.getNamespace();
        final String serviceName = getServiceName(configuration);
        final String hostname = getHost();
        final int port = getPort(configuration);

        serviceProvider = ServiceProviderBuilders.<ShardInfo>shardedServiceProviderBuilder()
                .withCuratorFramework(curator)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withSerializer(data -> {
                    try {
                        return objectMapper.writeValueAsBytes(data);
                    } catch (Exception e) {
                        log.warn("Could not parse node data", e);
                    }
                    return null;
                })
                .withHostname(hostname)
                .withPort(port)
                .withNodeData(ShardInfo.builder()
                                .environment(serviceDiscoveryConfiguration.getEnvironment())
                                .build())
                .withHealthcheck(() -> {
                    for(Healthcheck healthcheck : healthchecks) {
                        if(HealthcheckStatus.unhealthy == healthcheck.check()) {
                            return HealthcheckStatus.unhealthy;
                        }
                    }
                    return HealthcheckStatus.healthy;
                })
                .buildServiceDiscovery();

        serviceDiscoveryClient = ServiceDiscoveryClient.fromCurator()
                                    .curator(curator)
                                    .namespace(namespace)
                                    .serviceName(serviceName)
                                    .environment(serviceDiscoveryConfiguration.getEnvironment())
                                    .objectMapper(environment.getObjectMapper())
                                    .build();

        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
                curator.start();
                serviceProvider.start();
                serviceDiscoveryClient.start();
            }

            @Override
            public void stop() throws Exception {
                serviceDiscoveryClient.stop();
                serviceProvider.stop();
                curator.close();
            }
        });

        environment.jersey().register(new InfoResource(serviceDiscoveryClient));
    }

    protected abstract ServiceDiscoveryConfiguration getRangerConfiguration(T configuration);

    protected abstract String getServiceName(T configuration);

    protected abstract int getPort(T configuration);

    protected String getHost() throws Exception {
        if(serviceDiscoveryConfiguration.getPublishedHost().equals(Constants.LOCALHOST)) {
            return InetAddress.getLocalHost().getCanonicalHostName();
        }
        return serviceDiscoveryConfiguration.getPublishedHost();
    }

    public void registerHealthcheck(Healthcheck healthcheck) {
        this.healthchecks.add(healthcheck);
    }

    public void registerHealthchecks(List<Healthcheck> healthchecks) {
        this.healthchecks.addAll(healthchecks);
    }

}
