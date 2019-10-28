/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
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

package io.appform.dropwizard.discovery.bundle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceProviderBuilders;
import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.flipkart.ranger.healthservice.TimeEntity;
import com.flipkart.ranger.healthservice.monitor.IsolatedHealthMonitor;
import com.flipkart.ranger.serviceprovider.ServiceProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.appform.dropwizard.discovery.bundle.healthchecks.InitialDelayChecker;
import io.appform.dropwizard.discovery.bundle.healthchecks.InternalHealthChecker;
import io.appform.dropwizard.discovery.bundle.healthchecks.RotationCheck;
import io.appform.dropwizard.discovery.bundle.id.IdGenerator;
import io.appform.dropwizard.discovery.bundle.id.NodeIdManager;
import io.appform.dropwizard.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.dropwizard.discovery.bundle.monitors.DropwizardHealthMonitor;
import io.appform.dropwizard.discovery.bundle.monitors.DropwizardServerStartupCheck;
import io.appform.dropwizard.discovery.bundle.rotationstatus.DropwizardServerStatus;
import io.appform.dropwizard.discovery.bundle.rotationstatus.RotationStatus;
import io.appform.dropwizard.discovery.client.ServiceDiscoveryClient;
import io.appform.dropwizard.discovery.common.ShardInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.Router;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

/**
 * A dropwizard bundle for service discovery.
 */
@Slf4j
public abstract class ServiceDiscoveryBundle {

  private ServiceDiscoveryConfiguration serviceDiscoveryConfiguration;
  private List<Healthcheck> healthchecks = Lists.newArrayList();
  private ServiceProvider<ShardInfo> serviceProvider;
  private final List<IdValidationConstraint> globalIdConstraints;

  @Getter
  private CuratorFramework curator;

  @Getter
  private ServiceDiscoveryClient serviceDiscoveryClient;

  @Getter
  @VisibleForTesting
  private RotationStatus rotationStatus;

  @Getter
  @VisibleForTesting
  private DropwizardServerStatus serverStatus;
  private Vertx vertx;

  protected ServiceDiscoveryBundle() {
    globalIdConstraints = Collections.emptyList();
  }

  protected ServiceDiscoveryBundle(List<IdValidationConstraint> globalIdConstraints) {
    this.globalIdConstraints = globalIdConstraints != null
        ? globalIdConstraints
        : Collections.emptyList();
  }

  public void initialize(Vertx vertx) {
    this.vertx = vertx;
  }

  public void run(ObjectMapper objectMapper, Router router) throws Exception {
    serviceDiscoveryConfiguration = getRangerConfiguration();
    final String namespace = serviceDiscoveryConfiguration.getNamespace();
    final String serviceName = getServiceName();
    final String hostname = getHost();
    final int port = getPort();
    rotationStatus = new RotationStatus(serviceDiscoveryConfiguration.isInitialRotationStatus());
    serverStatus = new DropwizardServerStatus(false);

    curator = CuratorFrameworkFactory.builder()
        .connectString(serviceDiscoveryConfiguration.getZookeeper())
        .namespace(namespace)
        .retryPolicy(new RetryForever(serviceDiscoveryConfiguration.getConnectionRetryIntervalMillis()))
        .build();
    serviceProvider = buildServiceProvider(
        objectMapper,
        namespace,
        serviceName,
        hostname,
        port
    );
    serviceDiscoveryClient = buildDiscoveryClient(
        objectMapper,
        namespace,
        serviceName);

    vertx.deployVerticle(new ServiceDiscoveryManager(serviceName));
    router.get("/instances").handler(routingContext -> {
      byte[] bytes = new byte[0];
      try {
        bytes = objectMapper.writeValueAsBytes(serviceDiscoveryClient.getAllNodes());
      } catch (JsonProcessingException e) {
        log.error("Exception in writing nodes", e);
        routingContext.fail(500, e);
      }
      routingContext.response().setStatusCode(200).end(Buffer.buffer(bytes));
    });

//    environment.lifecycle()
//        .manage(new ServiceDiscoveryManager(serviceName));
//    environment.jersey()
//        .register(new InfoResource(serviceDiscoveryClient));
//    environment.admin()
//        .addTask(new OORTask(rotationStatus));
//    environment.admin()
//        .addTask(new BIRTask(rotationStatus));
  }

  protected abstract ServiceDiscoveryConfiguration getRangerConfiguration();

  protected abstract String getServiceName();

  protected List<IsolatedHealthMonitor> getHealthMonitors() {
    return Lists.newArrayList();
  }

  protected int getPort() {
    Preconditions.checkArgument(
        Constants.DEFAULT_PORT != serviceDiscoveryConfiguration.getPublishedPort()
            && 0 != serviceDiscoveryConfiguration.getPublishedPort(),
        "Looks like publishedPost has not been set and getPort() has not been overridden. This is wrong. \n" +
            "Either set publishedPort in config or override getPort() to return the port on which the service is running");
    return serviceDiscoveryConfiguration.getPublishedPort();
  }

  protected String getHost() throws UnknownHostException {
    final String host = serviceDiscoveryConfiguration.getPublishedHost();

    if (Strings.isNullOrEmpty(host) || host.equals(Constants.DEFAULT_HOST)) {
      return InetAddress.getLocalHost()
          .getCanonicalHostName();
    }
    return host;
  }

  public void registerHealthcheck(Healthcheck healthcheck) {
    this.healthchecks.add(healthcheck);
  }

  public void registerHealthchecks(List<Healthcheck> healthchecks) {
    this.healthchecks.addAll(healthchecks);
  }

  private ServiceDiscoveryClient buildDiscoveryClient(
      ObjectMapper objectMapper,
      String namespace,
      String serviceName) {
    return ServiceDiscoveryClient.fromCurator()
        .curator(curator)
        .namespace(namespace)
        .serviceName(serviceName)
        .environment(serviceDiscoveryConfiguration.getEnvironment())
        .objectMapper(objectMapper)
        .refreshTimeMs(serviceDiscoveryConfiguration.getRefreshTimeMs())
        .disableWatchers(serviceDiscoveryConfiguration.isDisableWatchers())
        .build();
  }

  private ServiceProvider<ShardInfo> buildServiceProvider(
      ObjectMapper objectMapper,
      String namespace,
      String serviceName,
      String hostname,
      int port) {
    val nodeInfo = ShardInfo.builder()
        .environment(serviceDiscoveryConfiguration.getEnvironment())
        .build();
    val initialDelayForMonitor = serviceDiscoveryConfiguration.getInitialDelaySeconds() > 1
        ? serviceDiscoveryConfiguration.getInitialDelaySeconds() - 1
        : 0;
    val dwMonitoringInterval = serviceDiscoveryConfiguration.getDropwizardCheckInterval() == 0
        ? Constants.DEFAULT_DW_CHECK_INTERVAl
        : serviceDiscoveryConfiguration.getDropwizardCheckInterval();
    val dwMonitoringStaleness = serviceDiscoveryConfiguration.getDropwizardCheckStaleness() < dwMonitoringInterval + 1
        ? dwMonitoringInterval + 1
        : serviceDiscoveryConfiguration.getDropwizardCheckStaleness();
    val serviceProviderBuilder = ServiceProviderBuilders.<ShardInfo>shardedServiceProviderBuilder()
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
        .withNodeData(nodeInfo)
        .withHealthcheck(new InternalHealthChecker(healthchecks))
        .withHealthcheck(new RotationCheck(rotationStatus))
        .withHealthcheck(new InitialDelayChecker(serviceDiscoveryConfiguration.getInitialDelaySeconds()))
        .withHealthcheck(new DropwizardServerStartupCheck(environment, serverStatus))
        .withIsolatedHealthMonitor(new IsolatedHealthMonitor() {
          @Override
          public HealthcheckStatus monitor() {
            return null;
          }
        })
        .withIsolatedHealthMonitor(
            new DropwizardHealthMonitor(
                new TimeEntity(initialDelayForMonitor, dwMonitoringInterval, TimeUnit.SECONDS),
                dwMonitoringStaleness * 1_000, environment))
        .withHealthUpdateIntervalMs(serviceDiscoveryConfiguration.getRefreshTimeMs())
        .withStaleUpdateThresholdMs(10000);

    val healthMonitors = getHealthMonitors();
    if (healthMonitors != null && !healthMonitors.isEmpty()) {
      healthMonitors.forEach(serviceProviderBuilder::withIsolatedHealthMonitor);
    }
    return serviceProviderBuilder.buildServiceDiscovery();
  }

  private class ServiceDiscoveryManager extends AbstractVerticle {

    private final String serviceName;

    public ServiceDiscoveryManager(String serviceName) {
      this.serviceName = serviceName;
    }

    @Override
    public void start() throws Exception {
      curator.start();
      serviceProvider.start();
      serviceDiscoveryClient.start();
      NodeIdManager nodeIdManager = new NodeIdManager(curator, serviceName);
      IdGenerator.initialize(nodeIdManager.fixNodeId(), globalIdConstraints, Collections.emptyMap());
    }

    @Override
    public void stop() throws Exception {
      serviceDiscoveryClient.stop();
      serviceProvider.stop();
      curator.close();
      IdGenerator.cleanUp();
    }
  }
}
