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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.dropwizard.discovery.bundle.id.constraints.IdValidationConstraint;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.security.SecureRandom;
import java.util.*;

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
    private static List<IdValidationConstraint> globalConstraints = Collections.emptyList();
    private static Map<String, List<IdValidationConstraint>> domainSpecificConstraints = new HashMap<>();

    public static void initialize(int node) {
        nodeId = node;
    }

    public static void initialize(
            int node, List<IdValidationConstraint> globalConstraints, Map<String, List<IdValidationConstraint>> domainSpecificConstraints) {
        nodeId = node;
        IdGenerator.globalConstraints = globalConstraints != null
                ? globalConstraints
                : Collections.emptyList();
        IdGenerator.domainSpecificConstraints.putAll(domainSpecificConstraints);
    }

    public static synchronized void registerGlobalConstraints(IdValidationConstraint... constraints) {
        registerGlobalConstraints(ImmutableList.copyOf(constraints));
    }

    public static synchronized void registerGlobalConstraints(List<IdValidationConstraint> constraints) {
        Preconditions.checkArgument(null != constraints && !constraints.isEmpty());
        if(null == globalConstraints) {
            globalConstraints = new ArrayList<>();
        }
        globalConstraints.addAll(constraints);
    }

    public static synchronized void registerDomainSpecificConstraints(String domain, IdValidationConstraint... validationConstraints) {
        registerDomainSpecificConstraints(domain, ImmutableList.copyOf(validationConstraints));
    }

    public static synchronized void registerDomainSpecificConstraints(String domain, List<IdValidationConstraint> validationConstraints) {
        Preconditions.checkArgument(null != validationConstraints && !validationConstraints.isEmpty());
        if(!domainSpecificConstraints.containsKey(domain)) {
            domainSpecificConstraints.put(domain, new ArrayList<>());
        }
        domainSpecificConstraints.get(domain).addAll(validationConstraints);
    }

    /**
     * Generate id with given prefix
     *
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
     *
     * @param prefix        String prefix
     * @param domain Domain for constraint selection
     * @return
     */
    public static Id generateWithConstraints(String prefix, String domain) {
        return generateWithConstraints(prefix, domainSpecificConstraints.getOrDefault(domain, Collections.emptyList()), true);
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param domain Domain for constraint selection
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return
     */
    public static Id generateWithConstraints(String prefix, String domain, boolean skipGlobal) {
        return generateWithConstraints(prefix, domainSpecificConstraints.getOrDefault(domain, Collections.emptyList()), skipGlobal);
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param inConstraints Constraints that need to be validate.
     * @return
     */
    public static Id generateWithConstraints(String prefix, final List<IdValidationConstraint> inConstraints) {
        return generateWithConstraints(prefix, inConstraints, false);
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param inConstraints Constraints that need to be validate.
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return
     */
    public static Id generateWithConstraints(String prefix, final List<IdValidationConstraint> inConstraints, boolean skipGlobal) {
        Id id;
        do {
            id = generate(prefix);
        } while (!isValidId(inConstraints, id, skipGlobal));
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

    private static boolean isValidId(List<IdValidationConstraint> inConstraints, Id id, boolean skipGlobal) {
        return (skipGlobal
                || globalConstraints.stream()
                .allMatch(constraint -> constraint.isValid(id)))
                && (null == inConstraints
                || inConstraints.stream()
                .allMatch(constraint -> constraint.isValid(id)));
    }
}
