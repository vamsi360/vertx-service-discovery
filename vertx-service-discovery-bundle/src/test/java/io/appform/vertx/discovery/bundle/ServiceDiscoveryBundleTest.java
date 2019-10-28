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
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.flipkart.ranger.model.ServiceNode;
import io.appform.vertx.discovery.client.ServiceDiscoveryClient;
import io.appform.vertx.discovery.common.ShardInfo;
import io.vertx.core.Vertx;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.web.Router;
import java.util.Optional;
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
  private HealthChecks healthChecks;
  private Router router;

  @Before
  public void setup() throws Exception {
    testingCluster.start();

    vertx = Vertx.vertx();
    healthChecks = HealthChecks.create(vertx);
    router = Router.router(vertx);

    serviceDiscoveryConfiguration = ServiceDiscoveryConfiguration.builder()
        .zookeeper(testingCluster.getConnectString())
        .namespace("test")
        .environment("testing")
        .connectionRetryIntervalMillis(5000)
        .publishedHost("TestHost")
        .publishedPort(8021)
        .initialRotationStatus(true)
        .build();
    bundle.initialize();

    bundle.run(new ObjectMapper(), vertx, healthChecks, router);
    //bundle.getServerStatus().markStarted();
    bundle.registerHealthcheck(() -> status);
  }

  @After
  public void tearDown() throws Exception {
    vertx.close(event -> log.info("Tearing down Vertx.."));
    testingCluster.stop();
  }

  @Test
  public void testDiscovery() throws Exception {
    ServiceDiscoveryClient serviceDiscoveryClient = bundle.getServiceDiscoveryClient();
    Optional<ServiceNode<ShardInfo>> info = serviceDiscoveryClient.getNode();
    assertTrue(info.isPresent());
    assertEquals("testing", info.get().getNodeData().getEnvironment());
    assertEquals("TestHost", info.get().getHost());
    assertEquals(8021, info.get().getPort());
    status = HealthcheckStatus.unhealthy;

    Thread.sleep(10000);
    info = serviceDiscoveryClient.getNode();
    assertFalse(info.isPresent());
  }

  @Test
  public void testDiscoveryCustomHostPort() throws Exception {
    Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
    assertTrue(info.isPresent());
    assertEquals("testing", info.get().getNodeData().getEnvironment());
    assertEquals("CustomHost", info.get().getHost());
    assertEquals(21000, info.get().getPort());
    status = HealthcheckStatus.unhealthy;

    Thread.sleep(10000);
    info = bundle.getServiceDiscoveryClient().getNode();
    assertFalse(info.isPresent());
  }

  @Test
  public void testDiscoveryMonitor() throws Exception {
    Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
    Thread.sleep(1000);
    assertTrue(info.isPresent());
    assertEquals("testing", info.get().getNodeData().getEnvironment());
    assertEquals("CustomHost", info.get().getHost());
    assertEquals(21000, info.get().getPort());

        /* after 2 turns, the healthcheck will return unhealthy, and since dropwizardCheckInterval
           is 2 seconds, within 2*2=4 seconds, nodes should be absent */
    Thread.sleep(11000);
    info = bundle.getServiceDiscoveryClient().getNode();
    assertFalse(info.isPresent());
  }

  @Test
  public void testDiscoveryStaleness() throws Exception {
    Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
    assertTrue(info.isPresent());
    assertEquals("testing", info.get().getNodeData().getEnvironment());
    assertEquals("CustomHost", info.get().getHost());
    assertEquals(21000, info.get().getPort());

        /* since healtcheck is sleeping for 5secs, the staleness allowed is 2+1=3 seconds, node should vanish after
           3 seconds */
    Thread.sleep(6000);
    assertTrue(bundle.getServiceDiscoveryClient().getNode().isPresent());
    Thread.sleep(6000);
    info = bundle.getServiceDiscoveryClient().getNode();
    assertFalse(info.isPresent());
  }

  @Test
  public void testDiscoveryBundleOOR() throws Exception {
    Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
    Thread.sleep(1000);
    assertTrue(info.isPresent());
    assertEquals("testing", info.get().getNodeData().getEnvironment());
    assertEquals("TestHost", info.get().getHost());
    assertEquals(8021, info.get().getPort());

//    OORTask oorTask = new OORTask(rotationStatus);
//    oorTask.execute(null, null);

    Thread.sleep(10000);
    info = bundle.getServiceDiscoveryClient().getNode();
    assertFalse(info.isPresent());

//    BIRTask birTask = new BIRTask(rotationStatus);
//    birTask.execute(null, null);
    Thread.sleep(10000);

    info = bundle.getServiceDiscoveryClient().getNode();
    assertTrue(info.isPresent());
  }
}