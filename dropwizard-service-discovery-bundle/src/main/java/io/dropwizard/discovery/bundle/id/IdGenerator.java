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
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.security.SecureRandom;
import java.util.List;

/**
 * Id generation
 */
public class IdGenerator {

    private static final class IdInfo {
        int exponent;
        long time;

        public IdInfo(int exponent, long time) {
            this.exponent = exponent;
            this.time = time;
        }
    }

    private static SecureRandom random = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    private static int nodeId;
    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyMMddHHmmssSSS");
    private static final CollisionChecker collisionChecker = new CollisionChecker();

    public static void initialize(int node) {
        nodeId = node;
    }

    /**
     * Generate id with given prefix
     * @param prefix String prefix with will be used to blindly merge
     * @return
     */
    public static Id generate(String prefix) {
        final IdInfo idInfo = random();
        DateTime dateTime = new DateTime(idInfo.time);
        final String id = String.format("%s%s%04d%03d", prefix, formatter.print(dateTime), nodeId, idInfo.exponent);
        return Id.builder()
                .id(id)
                .exponent(idInfo.exponent)
                .generatedDate(dateTime.toDate())
                .node(nodeId)
                .build();
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     * @param prefix String prefix
     * @param inConstraints Constraints that need to be validate.
     * @return
     */
    public static Id generateWithConstraints(String prefix, IdValidationConstraint... inConstraints) {
        return generateWithConstraints(prefix, ImmutableList.copyOf(inConstraints));
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     * @param prefix String prefix
     * @param inConstraints Constraints that need to be validate.
     * @return
     */
    public static Id generateWithConstraints(String prefix, final List<IdValidationConstraint> inConstraints) {
        Id id;
        do {
            id = generate(prefix);
        } while (validateId(inConstraints, id));
        return id;
    }

    private synchronized static IdInfo random() {
        int randomGen;
        long time;
        do {
            time = System.currentTimeMillis();
            randomGen = random.nextInt(Constants.MAX_ID_PER_MS);
        } while (!collisionChecker.check(time, randomGen));
        return new IdInfo(randomGen, time);
    }

    private static boolean validateId(List<IdValidationConstraint> inConstraints, Id id) {
        return null != inConstraints
                && !inConstraints.stream()
                .allMatch(constraint -> constraint.isValid(id));
    }
}
