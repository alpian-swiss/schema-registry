/*
 * Copyright 2026 Alpian SA
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.protobuf;

import static io.confluent.connect.protobuf.ProtobufData.PII_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

/**
 * Alpian addition: a field tagged {@code PII} in its Confluent field meta must reach the Connect
 * schema as a {@code PII=true} parameter, so a connector downstream can act on the
 * classification without parsing the Protobuf descriptor itself.
 */
public class ProtobufDataPiiTagTest {

  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "\n"
      + "import \"confluent/meta.proto\";\n"
      + "\n"
      + "package alpian;\n"
      + "\n"
      + "message Customer {\n"
      + "  string plain = 1;\n"
      + "  string ssn = 2 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  bytes fingerprint = 3 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  int64 account_number = 4 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  string other_tag_only = 5 [(.confluent.field_meta).tags = \"INTERNAL\"];\n"
      + "  string second_of_two_tags = 6 [(.confluent.field_meta) = "
      + "{ tags: [\"INTERNAL\", \"PII\"] }];\n"
      + "  repeated string aliases = 7 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "  Address address = 8;\n"
      + "}\n"
      + "message Address {\n"
      + "  string line1 = 1 [(.confluent.field_meta).tags = \"PII\"];\n"
      + "}";

  private Schema connectSchema() {
    Map<String, Object> configs = new HashMap<>();
    ProtobufData protobufData = new ProtobufData(new ProtobufDataConfig(configs));
    return protobufData.toConnectSchema(new ProtobufSchema(SCHEMA));
  }

  private Map<String, String> params(Schema schema, String field) {
    Map<String, String> params = schema.field(field).schema().parameters();
    return params != null ? params : Collections.emptyMap();
  }

  @Test
  public void testTaggedFieldGetsParameter() {
    assertEquals("true", params(connectSchema(), "ssn").get(PII_TAG));
  }

  @Test
  public void testUntaggedFieldHasNoParameter() {
    assertFalse(params(connectSchema(), "plain").containsKey(PII_TAG));
  }

  @Test
  public void testUnrelatedTagDoesNotSetParameter() {
    assertFalse(params(connectSchema(), "other_tag_only").containsKey(PII_TAG));
  }

  /**
   * PII need not be the first tag. Reading only {@code getTagsList().get(0)} would miss this.
   */
  @Test
  public void testPiiTagIsFoundBehindAnotherTag() {
    assertEquals("true", params(connectSchema(), "second_of_two_tags").get(PII_TAG));
  }

  /**
   * A tag is metadata, not a redaction instruction, so it propagates for every field type --
   * including the ones {@code FieldRedactionExecutor} cannot mask.
   */
  @Test
  public void testParameterIsSetRegardlessOfFieldType() {
    Schema schema = connectSchema();
    assertEquals("true", params(schema, "fingerprint").get(PII_TAG));
    assertEquals("true", params(schema, "account_number").get(PII_TAG));
  }

  /**
   * For a repeated field the parameter lands on the array schema, the same place the field's
   * {@code io.confluent.protobuf.Tag} parameter goes -- not on the element schema.
   */
  @Test
  public void testRepeatedFieldTagsTheArraySchema() {
    Schema aliases = connectSchema().field("aliases").schema();
    assertEquals(Schema.Type.ARRAY, aliases.type());
    assertEquals("true", aliases.parameters().get(PII_TAG));
    Map<String, String> elementParams = aliases.valueSchema().parameters();
    assertTrue(elementParams == null || !elementParams.containsKey(PII_TAG));
  }

  /**
   * The parameter is a one-way projection of the proto option onto the Connect side:
   * {@code fromConnectSchema} reads only the parameters upstream defines, so converting back
   * does not re-emit the field meta option. ALPIAN.md says so; this is what checks it.
   */
  @Test
  public void testParameterDoesNotRoundTripBackIntoProtobuf() {
    ProtobufData protobufData = new ProtobufData(new ProtobufDataConfig(new HashMap<>()));
    Schema connectSchema = protobufData.toConnectSchema(new ProtobufSchema(SCHEMA));
    ProtobufSchema roundTripped = protobufData.fromConnectSchema(connectSchema);
    assertNotNull(roundTripped);
    assertFalse(roundTripped.canonicalString().contains(PII_TAG));
  }

  @Test
  public void testNestedMessageFieldsAreTagged() {
    Schema schema = connectSchema();
    assertNull(params(schema, "address").get(PII_TAG));
    assertEquals("true",
        schema.field("address").schema().field("line1").schema().parameters().get(PII_TAG));
  }
}
