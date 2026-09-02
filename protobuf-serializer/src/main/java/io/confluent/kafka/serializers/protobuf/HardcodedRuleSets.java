/*
 * Copyright 2026 Confluent Inc.
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
 */

package io.confluent.kafka.serializers.protobuf;

import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import java.util.Collections;

/**
 * Rule sets compiled into this jar rather than read from a schema's registration.
 *
 * <p>Domain rules normally travel with the schema: a subject is registered with a
 * {@code ruleset} and the deserializer reads it off the schema it just fetched. A deployment
 * that cannot put rules on its subjects — no write access to the registry, or a registry that
 * predates rule support — has no way to express them, so {@link #READ_RULE_SET} states them in
 * code instead. It is the exact equivalent of registering
 *
 * <pre>
 * ruleset:
 *   domainRules:
 *     - name: mask-pii
 *       kind: TRANSFORM
 *       type: REDACT
 *       mode: READ
 *       tags: [ "PII" ]
 * </pre>
 *
 * <p>Applied only when {@code hardcoded.rule.set.enable} is set, and only on the read side —
 * see {@link AbstractKafkaProtobufDeserializer}. This artifact is shared by every Protobuf
 * consumer in a deployment, so the rules live here but the decision to run them does not.
 *
 * <h2>Why REDACT and not CEL_FIELD</h2>
 *
 * <p>A {@code CEL_FIELD} rule replacing values with a literal is the more obvious spelling, and
 * it works — for strings. It cannot be made to cover a mixed set of tagged fields, for two
 * reasons that compound. A single expression cannot branch on type, because CEL rejects a
 * conditional whose arms differ ({@code typeName == 'STRING' ? '****' : value} fails to compile
 * against a bytes field). Nor can several type-guarded rules be combined, because
 * {@code FieldRuleExecutor} skips a transform when another rule carries the same tags, kind,
 * mode and type — so of three rules all tagged {@code PII}, exactly one ever runs. Covering
 * several types with CEL therefore means splitting the tag itself
 * ({@code PII_STR} / {@code PII_BYTES} / {@code PII_NUM}), which pushes the problem into every
 * schema. And a literal that does not match the field's type does not degrade gracefully: it
 * fails the deserialize with {@code "Wrong object type used with protocol message reflection"},
 * so a single mistagged {@code int64} stops the consumer.
 *
 * <p>{@code REDACT} sidesteps all of it. One rule and one tag redact strings and bytes, and
 * every other type is returned unchanged instead of throwing — see
 * {@code FieldRedactionExecutor}. Fields that cannot be redacted in-band are simply not: there
 * is no honest redaction of a {@code bool}, where {@code false} is indistinguishable from a real
 * value. Confluent's own field-level encryption draws the line in the same place, refusing
 * anything but string and bytes.
 *
 * <h2>What the consumer must supply</h2>
 *
 * <p>{@code FieldRedactionExecutor} ships in {@code kafka-schema-registry-client}, which this
 * module already depends on, so no extra artifact is needed. It is not registered through the
 * {@code ServiceLoader}, though, so the consumer has to name it:
 *
 * <pre>
 * hardcoded.rule.set.enable = true
 * rule.executors            = redact
 * rule.executors.redact.class = io.confluent.kafka.schemaregistry.rules.FieldRedactionExecutor
 * </pre>
 *
 * <p>Without that registration the rule does not silently no-op — it fails the deserialize with
 * {@code "Could not find rule executor of type REDACT"}. Fail-closed is the right default for a
 * masking rule, but it means the three properties travel together.
 *
 * <p>The other half is the tags themselves. With no registry metadata to carry them, the only
 * live source is the proto: {@code [(confluent.field_meta).tags = "PII"]} on each field to
 * redact. A schema with no such option matches no field and the rule is a no-op.
 */
public final class HardcodedRuleSets {

  public static final String MASK_PII_RULE_NAME = "mask-pii";

  public static final String PII_TAG = "PII";

  /** Type of {@code FieldRedactionExecutor}, which lives in the schema registry client. */
  public static final String REDACT_TYPE = "REDACT";

  /**
   * Domain rules run on the read path when the hardcoded rule set is enabled and the schema
   * carries none of its own.
   */
  public static final RuleSet READ_RULE_SET = new RuleSet(
      Collections.emptyList(),
      Collections.singletonList(new Rule(
          MASK_PII_RULE_NAME,
          "Redacts every string and bytes field tagged PII on read",
          RuleKind.TRANSFORM,
          RuleMode.READ,
          REDACT_TYPE,
          Collections.singleton(PII_TAG),
          null,
          null,
          null,
          null,
          false
      ))
  );

  private HardcodedRuleSets() {
  }
}
