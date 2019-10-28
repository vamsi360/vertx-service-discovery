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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.flipkart.ranger.model.ServiceNode;
import io.appform.vertx.discovery.client.ServiceDiscoveryClient;
import io.appform.vertx.discovery.common.ShardInfo;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class ServiceDiscoveryBundleTest {

  private final ServiceDiscoveryBundle bundle = new ServiceDiscoveryBundle() {
    @Override
    protected ServiceDiscoveryConfiguration getRangerConfiguration() {
      return serviceDiscoveryConfiguration;
    }

    @Override
    protected String getServiceName() {
      return "TestService";
    }
  };

  private ServiceDiscoveryConfiguration serviceDiscoveryConfiguration;
  private final TestingCluster testingCluster = new TestingCluster(1);
  private HealthcheckStatus status = HealthcheckStatus.healthy;

  private Vertx vertx;
  private int vertxPort = 9471;
  private HttpServer httpServer;
  private String discoveryEnv = "testing";
  private String publishedHost = "TestHost";

  @Before
  public void setup() throws Exception {
    testingCluster.start();

    serviceDiscoveryConfiguration = ServiceDiscoveryConfiguration.builder()
        .zookeeper(testingCluster.getConnectString())
        .namespace("test")
        .environment(discoveryEnv)
        .connectionRetryIntervalMillis(5000)
        .publishedHost(publishedHost)
        .publishedPort(8021)
        .initialRotationStatus(true)
        .checkInterval(2)
        //.checkStaleness(6)
        .build();
    bundle.initialize();
  }

  private void run(Healthcheck healthcheck) throws UnknownHostException, InterruptedException {
    vertx = Vertx.vertx();
    HealthChecks healthChecks = HealthChecks.create(vertx);
    Router router = Router.router(vertx);
    bundle.registerHealthcheck(healthcheck);
    bundle.run(new ObjectMapper(), vertx, healthChecks, router);
    httpServer = vertx.createHttpServer();
    httpServer.requestHandler(router)
        .listen(vertxPort, "0.0.0.0", event -> {
          log.info("Listening on port: {}", vertxPort);
        });
  }

  @After
  public void tearDown() throws IOException {
    httpServer.close(event -> log.info("Closing HttpServer.."));
    vertx.close(event -> log.info("Tearing down Vertx.."));
    testingCluster.stop();
  }

  @Test
  public void testDiscovery() throws InterruptedException, UnknownHostException {
    run(() -> status);

    ServiceDiscoveryClient serviceDiscoveryClient = bundle.getServiceDiscoveryClient();
    Optional<ServiceNode<ShardInfo>> info = serviceDiscoveryClient.getNode();
    assertTrue(info.isPresent());
    assertEquals(discoveryEnv, info.get().getNodeData().getEnvironment());
    assertEquals(publishedHost, info.get().getHost());
    assertEquals(8021, info.get().getPort());
    status = HealthcheckStatus.unhealthy;

    Thread.sleep(10000);
    info = serviceDiscoveryClient.getNode();
    assertFalse(info.isPresent());
  }

  @Test
  public void testDiscoveryMonitor() throws InterruptedException, UnknownHostException {
    run(new Healthcheck() {
      private AtomicInteger counter = new AtomicInteger(2);

      @Override
      public HealthcheckStatus check() {
        return (counter.decrementAndGet() < 0) ? HealthcheckStatus.unhealthy : HealthcheckStatus.healthy;
      }
    });

    Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
    Thread.sleep(1000);
    assertTrue(info.isPresent());
    assertEquals(discoveryEnv, info.get().getNodeData().getEnvironment());
    assertEquals(publishedHost, info.get().getHost());
    assertEquals(8021, info.get().getPort());

    /* after 2 turns, the healthcheck will return unhealthy, and since checkInterval
       is 2 seconds, within 2*2=4 seconds, nodes should be absent */
    Thread.sleep(11000);
    info = bundle.getServiceDiscoveryClient().getNode();
    assertFalse(info.isPresent());
  }

  @Test
  public void testDiscoveryStaleness() throws InterruptedException, UnknownHostException {
    run(new Healthcheck() {
      private AtomicInteger counter = new AtomicInteger(1);

      @Override
      public HealthcheckStatus check() {
        if (counter.decrementAndGet() < 0) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        log.info("Registering healthy now..");
        return HealthcheckStatus.healthy;
      }
    });

    Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
    assertTrue(info.isPresent());
    assertEquals(discoveryEnv, info.get().getNodeData().getEnvironment());
    assertEquals(publishedHost, info.get().getHost());
    assertEquals(8021, info.get().getPort());

        /* since healtcheck is sleeping for 5secs, the staleness allowed is 2+1=3 seconds, node should vanish after
           3 seconds */
    Thread.sleep(6000);
    assertTrue(bundle.getServiceDiscoveryClient().getNode().isPresent());
    Thread.sleep(6000);
    info = bundle.getServiceDiscoveryClient().getNode();
    assertFalse(info.isPresent());
  }

  @Test
  public void testDiscoveryBundleOORAndBIR() throws InterruptedException, UnknownHostException {
    run(() -> status);

    Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
    Thread.sleep(1000);
    assertTrue(info.isPresent());
    assertEquals(discoveryEnv, info.get().getNodeData().getEnvironment());
    assertEquals(publishedHost, info.get().getHost());
    assertEquals(8021, info.get().getPort());

    CountDownLatch latch = new CountDownLatch(2);
    AtomicInteger errors = new AtomicInteger();

    WebClient webClient = WebClient.create(vertx);
    webClient.post(vertxPort, "localhost", "/tasks/ranger-oor").send(result -> {
      log.info("Triggered App OOR");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        errors.incrementAndGet();
      }
      Optional<ServiceNode<ShardInfo>> info2 = bundle.getServiceDiscoveryClient().getNode();
      try {
        assertFalse(info2.isPresent());
        log.info("No service nodes as app is successfully OOR");
      } catch (AssertionError e) {
        log.error("Node is not supposed to be present but it's there.", e);
        errors.incrementAndGet();
      } finally {
        latch.countDown();
      }

      webClient.post(vertxPort, "localhost", "/tasks/ranger-bir").send(birResult -> {
        log.info("Triggered App BIR");
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          errors.incrementAndGet();
        }

        Optional<ServiceNode<ShardInfo>> info3 = bundle.getServiceDiscoveryClient().getNode();
        try {
          assertTrue(info3.isPresent());
          log.info("ServiceNodes after BIR: {}", info3.get());
        } catch (AssertionError e) {
          log.error("Not is supposed to be there but it's not", e);
          errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    });

    assertTrue(latch.await(45, TimeUnit.SECONDS));
  }
}