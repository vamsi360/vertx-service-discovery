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

package io.appform.vertx.discovery.bundle.rotationstatus;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Current server startup started
 */
public class ServerStatus {

  private AtomicBoolean serverStarted;

  public ServerStatus(boolean initialStatus) {
    serverStarted = new AtomicBoolean(initialStatus);
  }

  public void markStarted() {
    serverStarted.set(true);
  }

  public void markStopped() {
    serverStarted.set(false);
  }

  public boolean started() {
    return serverStarted.get();
  }
}
