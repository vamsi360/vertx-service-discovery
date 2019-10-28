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

package io.appform.dropwizard.discovery.bundle.id;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test on {@link CollisionChecker}
 */
public class CollisionCheckerTest {

    @Test
    public void testCheck() throws Exception {
        CollisionChecker collisionChecker = new CollisionChecker();
        Assert.assertTrue(collisionChecker.check(100, 1));
        Assert.assertFalse(collisionChecker.check(100, 1));
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(collisionChecker.check(101, i));
            Assert.assertFalse(collisionChecker.check(101, i));
        }

    }
}