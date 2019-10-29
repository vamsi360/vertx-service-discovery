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

package io.appform.vertx.discovery.bundle.monitors;

import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import io.appform.vertx.discovery.bundle.rotationstatus.ServerStatus;
import lombok.extern.slf4j.Slf4j;

/**
 * This healthcheck listens to server started event to mark service healthy on ranger.
 */
@Slf4j
public class VertxServerStartupCheck implements Healthcheck {

  private final ServerStatus serverStatus;

  public VertxServerStartupCheck(ServerStatus serverStatus) {
    this.serverStatus = serverStatus;
  }

  @Override
  public HealthcheckStatus check() {
    return serverStatus.started() ? HealthcheckStatus.healthy : HealthcheckStatus.unhealthy;
  }
}
