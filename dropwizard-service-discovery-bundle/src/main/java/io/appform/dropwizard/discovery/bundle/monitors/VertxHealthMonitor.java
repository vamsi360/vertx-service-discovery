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

package io.appform.dropwizard.discovery.bundle.monitors;

import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.flipkart.ranger.healthservice.TimeEntity;
import com.flipkart.ranger.healthservice.monitor.IsolatedHealthMonitor;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.healthchecks.HealthChecks;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VertxHealthMonitor extends IsolatedHealthMonitor<HealthcheckStatus> {

  private final HealthChecks healthChecks;

  public VertxHealthMonitor(
      TimeEntity runInterval,
      long stalenessAllowedInMillis,
      HealthChecks healthChecks) {
    super("vertx-health-monitor", runInterval, stalenessAllowedInMillis);
    this.healthChecks = healthChecks;
  }

  @Override
  public HealthcheckStatus monitor() {
    final AtomicReference<HealthcheckStatus> healthCheckStatusRef = new AtomicReference<>(HealthcheckStatus.unhealthy);
    healthChecks.invoke(entries -> {
      JsonArray checks = entries.getJsonArray("checks");
      if ("UP".equalsIgnoreCase(entries.getString("outcome"))) {
        healthCheckStatusRef.set(HealthcheckStatus.healthy);
      } else {
        healthCheckStatusRef.set(HealthcheckStatus.unhealthy);
      }
      log.info("HealthCheckStatus: {}; NoOfChecks: {}", healthCheckStatusRef.get(), checks.size());
    });
    return healthCheckStatusRef.get();
  }
}
