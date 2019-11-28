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

package io.appform.vertx.discovery.bundle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceProviderBuilders;
import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthservice.TimeEntity;
import com.flipkart.ranger.healthservice.monitor.IsolatedHealthMonitor;
import com.flipkart.ranger.serviceprovider.ServiceProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.appform.vertx.discovery.bundle.healthchecks.InitialDelayChecker;
import io.appform.vertx.discovery.bundle.healthchecks.InternalHealthChecker;
import io.appform.vertx.discovery.bundle.healthchecks.RotationCheck;
import io.appform.vertx.discovery.bundle.id.IdGenerator;
import io.appform.vertx.discovery.bundle.id.NodeIdManager;
import io.appform.vertx.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.vertx.discovery.bundle.monitors.VertxHealthMonitor;
import io.appform.vertx.discovery.bundle.monitors.VertxServerStartupCheck;
import io.appform.vertx.discovery.bundle.rotationstatus.RotationStatus;
import io.appform.vertx.discovery.bundle.rotationstatus.ServerStatus;
import io.appform.vertx.discovery.client.ServiceDiscoveryClient;
import io.appform.vertx.discovery.common.ShardInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.web.Router;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

/**
 * A vertx bundle for service discovery.
 */
@Slf4j
public class ServiceDiscoveryBundle implements Closeable {

  private final ObjectMapper objectMapper;
  private final Vertx vertx;
  private final HealthChecks vertxHealthChecks;
  private final Router router;

  private final String serviceDiscoveryName;
  private final ServiceDiscoveryConfiguration serviceDiscoveryConfiguration;

  private List<Healthcheck> rangerHealthChecks = Lists.newArrayList();
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
  private ServerStatus serverStatus;

  public ServiceDiscoveryBundle(ObjectMapper objectMapper,
      Vertx vertx,
      HealthChecks vertxHealthChecks,
      Router router,
      String serviceDiscoveryName,
      ServiceDiscoveryConfiguration serviceDiscoveryConfiguration) {
    this.objectMapper = objectMapper;
    this.vertx = vertx;
    this.vertxHealthChecks = vertxHealthChecks;
    this.router = router;
    this.serviceDiscoveryName = serviceDiscoveryName;
    this.serviceDiscoveryConfiguration = serviceDiscoveryConfiguration;
    globalIdConstraints = Collections.emptyList();
  }

  public void start() throws UnknownHostException, InterruptedException {
    String namespace = serviceDiscoveryConfiguration.getNamespace();
    String hostname = host();
    int port = port();
    rotationStatus = new RotationStatus(serviceDiscoveryConfiguration.isInitialRotationStatus());
    serverStatus = new ServerStatus(false);

    curator = CuratorFrameworkFactory.builder()
        .connectString(serviceDiscoveryConfiguration.getZookeeper())
        .namespace(namespace)
        .retryPolicy(new RetryForever(serviceDiscoveryConfiguration.getConnectionRetryIntervalMillis()))
        .build();
    serviceProvider = buildServiceProvider(
        objectMapper,
        vertxHealthChecks,
        namespace,
        serviceDiscoveryName,
        hostname,
        port
    );
    serviceDiscoveryClient = buildDiscoveryClient(
        objectMapper,
        namespace,
        serviceDiscoveryName);

    CountDownLatch startupLatch = new CountDownLatch(2);

    vertx.deployVerticle(new ServiceDiscoveryVerticle(serviceDiscoveryName, startupLatch));
    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start() {
        log.info("Marking app as started..");
        serverStatus.markStarted();
        startupLatch.countDown();
      }

      @Override
      public void stop() {
        log.info("Marking app as stopped..");
        serverStatus.markStopped();
      }
    });

    router.get("/instances").handler(routingContext -> {
      try {
        byte[] bytes = objectMapper.writeValueAsBytes(serviceDiscoveryClient.getAllNodes());
        routingContext.response().setStatusCode(200).end(Buffer.buffer(bytes));
      } catch (JsonProcessingException e) {
        log.error("Exception in writing nodes", e);
        routingContext.fail(500, e);
      }
    });

    router.post("/tasks/ranger-oor").handler(routingContext -> {
      log.info("Taking App OOR on Ranger");
      rotationStatus.oor();
      routingContext.response().setStatusCode(200).end("Took OOR..");
    });

    router.post("/tasks/ranger-bir").handler(routingContext -> {
      log.info("Taking App BIR on Ranger");
      rotationStatus.bir();
      routingContext.response().setStatusCode(200).end("Took BIR..");
    });

    log.info("Awaiting on startupLatch..");
    startupLatch.await();
  }

  @Override
  public void close() {
  }

  public List<IsolatedHealthMonitor> getHealthMonitors() {
    return Lists.newArrayList();
  }

  private int port() {
    Preconditions.checkArgument(
        Constants.DEFAULT_PORT != serviceDiscoveryConfiguration.getPublishedPort()
            && 0 != serviceDiscoveryConfiguration.getPublishedPort(),
        "Looks like publishedPost has not been set and getPort() has not been overridden. This is wrong. \n" +
            "Either set publishedPort in config or override getPort() to return the port on which the service is running");
    return serviceDiscoveryConfiguration.getPublishedPort();
  }

  private String host() throws UnknownHostException {
    final String host = serviceDiscoveryConfiguration.getPublishedHost();

    if (Strings.isNullOrEmpty(host) || host.equals(Constants.DEFAULT_HOST)) {
      return InetAddress.getLocalHost()
          .getCanonicalHostName();
    }
    return host;
  }

  public void registerHealthcheck(Healthcheck healthcheck) {
    this.rangerHealthChecks.add(healthcheck);
  }

  public void registerHealthchecks(List<Healthcheck> healthchecks) {
    this.rangerHealthChecks.addAll(healthchecks);
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
      HealthChecks healthChecks,
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
    val dwMonitoringInterval = serviceDiscoveryConfiguration.getCheckInterval() == 0
        ? Constants.DEFAULT_CHECK_INTERVAl
        : serviceDiscoveryConfiguration.getCheckInterval();
    val dwMonitoringStaleness = Math
        .max(serviceDiscoveryConfiguration.getCheckStaleness(), dwMonitoringInterval + 1);
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
        .withHealthcheck(new InternalHealthChecker(this.rangerHealthChecks))
        .withHealthcheck(new RotationCheck(rotationStatus))
        .withHealthcheck(new InitialDelayChecker(serviceDiscoveryConfiguration.getInitialDelaySeconds()))
        .withHealthcheck(new VertxServerStartupCheck(serverStatus))
        .withIsolatedHealthMonitor(
            new VertxHealthMonitor(
                new TimeEntity(initialDelayForMonitor, dwMonitoringInterval, TimeUnit.SECONDS),
                dwMonitoringStaleness * 1_000, healthChecks))
        .withHealthUpdateIntervalMs(serviceDiscoveryConfiguration.getRefreshTimeMs())
        .withStaleUpdateThresholdMs(10000);

    val healthMonitors = getHealthMonitors();
    if (healthMonitors != null && !healthMonitors.isEmpty()) {
      healthMonitors.forEach(serviceProviderBuilder::withIsolatedHealthMonitor);
    }
    return serviceProviderBuilder.buildServiceDiscovery();
  }

  private class ServiceDiscoveryVerticle extends AbstractVerticle {

    private final String serviceName;
    private final CountDownLatch startupLatch;

    public ServiceDiscoveryVerticle(String serviceName, CountDownLatch startupLatch) {
      this.serviceName = serviceName;
      this.startupLatch = startupLatch;
    }

    @Override
    public void start() throws Exception {
      curator.start();
      serviceProvider.start();
      serviceDiscoveryClient.start();
      NodeIdManager nodeIdManager = new NodeIdManager(curator, serviceName);
      IdGenerator.initialize(nodeIdManager.fixNodeId(), globalIdConstraints, Collections.emptyMap());

      startupLatch.countDown();
    }

    @Override
    public void stop() throws Exception {
      serviceDiscoveryClient.stop();

      log.info("== Stopping ServiceProvider ==");
      serviceProvider.stop();

      log.info("== Stopping Curator ==");
      curator.close();
      IdGenerator.cleanUp();
    }
  }
}
