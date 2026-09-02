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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.FieldRedactionExecutor;
import io.confluent.kafka.schemaregistry.rules.RulePhase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * The deserializer's compiled-in rule set, exercised against a schema that carries inline
 * {@code PII} tags and no ruleset of its own — the shape of the one deployment that cannot
 * register rules on its subjects.
 *
 * <p>These run against the real {@code FieldRedactionExecutor}, not a stand-in: it ships in
 * {@code kafka-schema-registry-client}, which this module already depends on. (A
 * {@code CEL_FIELD} rule could not be tested this way — its executor lives in
 * {@code kafka-schema-rules}, which cannot be a dependency here because that module
 * test-depends on this one, and Maven rejects the reactor cycle.)
 */
public class KafkaProtobufDeserializerHardcodedRuleSetTest {

  private static final String TOPIC = "test";

  /**
   * Two tagged fields the executor can redact, two it cannot, one untagged. The int64 and bool
   * are the interesting ones: they must come back untouched rather than failing the record.
   */
  private static final String SCHEMA_STRING = "syntax = \"proto3\";\n"
      + "import \"confluent/meta.proto\";\n"
      + "package io.confluent.kafka.serializers.protobuf.test;\n"
      + "message Customer {\n"
      + "  string email = 1 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  bytes  scan = 2 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  int64  account_id = 3 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  bool   is_resident = 4 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  string country = 5;\n"
      + "}\n";

  private static final String EMAIL = "someone@example.com";
  private static final ByteString SCAN = ByteString.copyFromUtf8("passport-scan");
  private static final long ACCOUNT_ID = 1234567890123L;
  private static final String COUNTRY = "CH";
  private static final String REDACTED = "<REDACTED>";

  private SchemaRegistryClient schemaRegistry;
  private byte[] payload;

  @Before
  public void setUp() throws Exception {
    schemaRegistry = new MockSchemaRegistryClient();
    ProtobufSchema schema = new ProtobufSchema(SCHEMA_STRING);
    schemaRegistry.register(TOPIC + "-value", schema);

    Descriptor descriptor = schema.toDescriptor();
    DynamicMessage message = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("email"), EMAIL)
        .setField(descriptor.findFieldByName("scan"), SCAN)
        .setField(descriptor.findFieldByName("account_id"), ACCOUNT_ID)
        .setField(descriptor.findFieldByName("is_resident"), true)
        .setField(descriptor.findFieldByName("country"), COUNTRY)
        .build();

    Map<String, Object> serializerConfig = new HashMap<>();
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    // Send under the registered schema rather than one derived from the message, so the
    // deserializer reads back the schema that carries the inline tags.
    serializerConfig.put(KafkaProtobufSerializerConfig.USE_LATEST_VERSION, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.LATEST_COMPATIBILITY_STRICT, false);
    KafkaProtobufSerializer<DynamicMessage> serializer =
        new KafkaProtobufSerializer<>(schemaRegistry, serializerConfig);
    payload = serializer.serialize(TOPIC, message);
  }

  @Test
  public void testRuleSetMatchesTheRegisteredForm() {
    assertTrue(HardcodedRuleSets.READ_RULE_SET.hasRules(RulePhase.DOMAIN, RuleMode.READ));
    assertEquals(1, HardcodedRuleSets.READ_RULE_SET.getDomainRules().size());
    Rule rule = HardcodedRuleSets.READ_RULE_SET.getDomainRules().get(0);
    assertEquals("mask-pii", rule.getName());
    assertEquals(RuleKind.TRANSFORM, rule.getKind());
    assertEquals(RuleMode.READ, rule.getMode());
    assertEquals("REDACT", rule.getType());
    assertEquals(Collections.singleton("PII"), rule.getTags());
    assertFalse(rule.isDisabled());
  }

  @Test
  public void testTaggedStringAndBytesAreRedactedWhenEnabled() {
    Message value = deserialize(true);
    assertEquals(REDACTED, field(value, "email"));
    assertEquals(ByteString.copyFromUtf8(REDACTED), field(value, "scan"));
    assertEquals(COUNTRY, field(value, "country"));
  }

  /**
   * The property that makes a single-tag rule safe to hardcode: a tagged field the executor
   * cannot redact is returned as-is, so one mistagged numeric field cannot stop the consumer.
   */
  @Test
  public void testTaggedFieldsThatCannotBeRedactedPassThrough() {
    Message value = deserialize(true);
    assertEquals(ACCOUNT_ID, field(value, "account_id"));
    assertEquals(true, field(value, "is_resident"));
  }

  @Test
  public void testNothingIsRedactedByDefault() {
    Message value = deserialize(false);
    assertEquals(EMAIL, field(value, "email"));
    assertEquals(SCAN, field(value, "scan"));
    assertEquals(ACCOUNT_ID, field(value, "account_id"));
    assertEquals(COUNTRY, field(value, "country"));
  }

  private Message deserialize(boolean hardcodedRuleSetEnabled) {
    Map<String, Object> config = new HashMap<>();
    config.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    if (hardcodedRuleSetEnabled) {
      config.put(KafkaProtobufDeserializerConfig.HARDCODED_RULE_SET_ENABLE, true);
    }
    // REDACT is not registered through the ServiceLoader, so name it explicitly -- the same
    // two properties a consumer of this jar has to set.
    config.put("rule.executors", "redact");
    config.put("rule.executors.redact.class", FieldRedactionExecutor.class.getName());
    KafkaProtobufDeserializer<Message> deserializer =
        new KafkaProtobufDeserializer<>(schemaRegistry, config);
    return deserializer.deserialize(TOPIC, payload);
  }

  private static Object field(Message message, String name) {
    return message.getField(message.getDescriptorForType().findFieldByName(name));
  }
}
