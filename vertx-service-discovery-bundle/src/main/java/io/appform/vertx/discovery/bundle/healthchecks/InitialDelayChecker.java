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

package io.appform.vertx.discovery.bundle.healthchecks;

import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;

/**
 * The following will return healthy only after stipulated time
 * This will give other bundles etc to startup properly
 * By the time the node joins the cluster
 */
public class InitialDelayChecker implements Healthcheck {
    private final long validRegistrationTime;


    public InitialDelayChecker(long initialDelaySeconds) {
        validRegistrationTime = System.currentTimeMillis() + initialDelaySeconds * 1000;
    }

    @Override
    public HealthcheckStatus check() {
        return System.currentTimeMillis() > validRegistrationTime
               ? HealthcheckStatus.healthy
               : HealthcheckStatus.unhealthy;
    }
}
