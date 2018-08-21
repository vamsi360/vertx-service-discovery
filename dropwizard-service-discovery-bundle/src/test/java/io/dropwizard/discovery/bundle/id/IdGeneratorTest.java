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

package io.dropwizard.discovery.bundle.id;

import com.google.common.collect.ImmutableList;
import io.dropwizard.discovery.bundle.id.constraints.IdValidationConstraint;
import io.dropwizard.discovery.bundle.id.constraints.impl.JavaHashCodeBasedKeyPartitioner;
import io.dropwizard.discovery.bundle.id.constraints.impl.PartitionValidator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test for {@link IdGenerator}
 */
@Slf4j
public class IdGeneratorTest {

    @Getter
    private static final class Runner implements Callable<Long> {
        private boolean stop = false;
        private long count = 0L;

        @Override
        public Long call() throws Exception {
            while (!stop) {
                Id id = IdGenerator.generate("X");
                count++;
            };
            return count;
        }
    }

    @Getter
    private static final class ConstraintRunner implements Callable<Long> {
        private final IdValidationConstraint constraint;
        private boolean stop = false;
        private long count = 0L;

        private ConstraintRunner(IdValidationConstraint constraint) {
            this.constraint = constraint;
        }

        @Override
        public Long call() throws Exception {
            while (!stop) {
                Id id = IdGenerator.generateWithConstraints("X", Collections.singletonList(constraint));
                count++;
            };
            return count;
        }
    }

    @Test
    public void testGenerate() throws Exception {
        IdGenerator.initialize(23);
        int numRunners = 20;

        ImmutableList.Builder<Runner> listBuilder = ImmutableList.builder();
        for (int i = 0; i < numRunners; i++) {
            listBuilder.add(new Runner());
        }

        List<Runner> runners = listBuilder.build();
        ExecutorService executorService = Executors.newFixedThreadPool(numRunners);
        for(Runner runner : runners) {
            executorService.submit(runner);
        }
        Thread.sleep(10000);
        executorService.shutdownNow();

        long totalCount = runners.stream().mapToLong(Runner::getCount).sum();

        log.debug("Generated ID count: {}", totalCount);
        log.debug("Generated ID rate: {}/sec", totalCount/10);
        Assert.assertTrue(totalCount > 0);

    }


    @Test
    public void testGenerateWithConstraintsNoConstraint() throws Exception {
        IdGenerator.initialize(23);
        int numRunners = 20;

        ImmutableList.Builder<ConstraintRunner> listBuilder = ImmutableList.builder();
        for (int i = 0; i < numRunners; i++) {
            listBuilder.add(new ConstraintRunner(new PartitionValidator(4, new JavaHashCodeBasedKeyPartitioner(16))));
        }

        List<ConstraintRunner> runners = listBuilder.build();
        ExecutorService executorService = Executors.newFixedThreadPool(numRunners);
        for(ConstraintRunner runner : runners) {
            executorService.submit(runner);
        }
        Thread.sleep(10000);
        executorService.shutdownNow();

        long totalCount = runners.stream().mapToLong(ConstraintRunner::getCount).sum();

        log.debug("Generated ID count: {}", totalCount);
        log.debug("Generated ID rate: {}/sec", totalCount/10);
        Assert.assertTrue(totalCount > 0);

    }
}