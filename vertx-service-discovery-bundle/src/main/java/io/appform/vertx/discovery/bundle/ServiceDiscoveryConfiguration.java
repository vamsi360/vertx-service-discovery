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

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Ranger configuration.
 */
@Data
@EqualsAndHashCode
@ToString
@NoArgsConstructor
public class ServiceDiscoveryConfiguration {

  private String namespace = Constants.DEFAULT_NAMESPACE;
  private String environment;
  private String zookeeper;
  private int connectionRetryIntervalMillis = Constants.DEFAULT_RETRY_CONN_INTERVAL;
  private String publishedHost = Constants.DEFAULT_HOST;
  private int publishedPort = Constants.DEFAULT_PORT;
  private int refreshTimeMs;
  private boolean disableWatchers;
  private long initialDelaySeconds;
  private boolean initialRotationStatus = true;
  private int checkInterval = Constants.DEFAULT_CHECK_INTERVAl;
  private int checkStaleness;

  @Builder
  public ServiceDiscoveryConfiguration(
      String namespace,
      String environment,
      String zookeeper,
      int connectionRetryIntervalMillis,
      String publishedHost,
      int publishedPort,
      int refreshTimeMs,
      boolean disableWatchers,
      long initialDelaySeconds,
      boolean initialRotationStatus,
      int checkInterval,
      int checkStaleness) {
    this.namespace = Strings.isNullOrEmpty(namespace)
        ? Constants.DEFAULT_NAMESPACE
        : namespace;
    this.environment = environment;
    this.zookeeper = zookeeper;
    this.connectionRetryIntervalMillis = connectionRetryIntervalMillis == 0
        ? Constants.DEFAULT_RETRY_CONN_INTERVAL
        : connectionRetryIntervalMillis;
    this.publishedHost = Strings.isNullOrEmpty(publishedHost)
        ? Constants.DEFAULT_HOST
        : publishedHost;
    this.publishedPort = publishedPort == 0
        ? Constants.DEFAULT_PORT
        : publishedPort;
    this.refreshTimeMs = refreshTimeMs;
    this.disableWatchers = disableWatchers;
    this.initialDelaySeconds = initialDelaySeconds;
    this.initialRotationStatus = initialRotationStatus;
    this.checkInterval = checkInterval == 0
        ? Constants.DEFAULT_CHECK_INTERVAl
        : checkInterval;
    this.checkStaleness = checkStaleness;
  }
}
