/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceSparkReadOptionsTest {

  @Test
  public void testBuilderWithNamespaceConfig() {
    Map<String, String> namespaceConfig = new HashMap<>();
    namespaceConfig.put("impl", "rest");
    namespaceConfig.put("uri", "http://localhost:8080");

    List<String> tableId = Arrays.asList("namespace1", "table1");

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path/to/dataset")
            .tableId(tableId)
            .namespaceImpl("rest")
            .namespaceConfig(namespaceConfig)
            .build();

    assertEquals("rest", options.getNamespaceImpl());
    assertNotNull(options.getNamespaceConfig());
    assertEquals("rest", options.getNamespaceConfig().get("impl"));
    assertEquals("http://localhost:8080", options.getNamespaceConfig().get("uri"));
    assertEquals(tableId, options.getTableId());
  }

  @Test
  public void testHasNamespaceWithConfig() {
    Map<String, String> namespaceConfig = new HashMap<>();
    namespaceConfig.put("impl", "rest");

    List<String> tableId = Arrays.asList("namespace1", "table1");

    // With namespace config but no namespace object
    LanceSparkReadOptions optionsWithConfig =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path/to/dataset")
            .tableId(tableId)
            .namespaceImpl("rest")
            .namespaceConfig(namespaceConfig)
            .build();

    // hasNamespace() should return true because config is available for reconstruction
    assertTrue(optionsWithConfig.hasNamespace());
    assertTrue(optionsWithConfig.hasNamespaceConfig());
    // But the actual namespace object is null (not set directly)
    assertNull(optionsWithConfig.getNamespace());
  }

  @Test
  public void testHasNamespaceWithoutConfig() {
    // Without namespace config
    LanceSparkReadOptions optionsWithoutConfig =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path/to/dataset").build();

    assertFalse(optionsWithoutConfig.hasNamespace());
    assertFalse(optionsWithoutConfig.hasNamespaceConfig());
    assertNull(optionsWithoutConfig.getNamespaceImpl());
    assertNull(optionsWithoutConfig.getNamespaceConfig());
  }

  @Test
  public void testHasNamespaceRequiresTableId() {
    Map<String, String> namespaceConfig = new HashMap<>();
    namespaceConfig.put("impl", "rest");

    // With namespace config but no tableId
    LanceSparkReadOptions optionsNoTableId =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path/to/dataset")
            .namespaceImpl("rest")
            .namespaceConfig(namespaceConfig)
            .build();

    // hasNamespace() should return false because tableId is required
    assertFalse(optionsNoTableId.hasNamespace());
    // But hasNamespaceConfig() should still be true
    assertTrue(optionsNoTableId.hasNamespaceConfig());
  }

  @Test
  public void testSerializationPreservesNamespaceConfig()
      throws IOException, ClassNotFoundException {
    Map<String, String> namespaceConfig = new HashMap<>();
    namespaceConfig.put("impl", "rest");
    namespaceConfig.put("uri", "http://localhost:8080");
    namespaceConfig.put("token", "secret-token");

    List<String> tableId = Arrays.asList("namespace1", "table1");

    LanceSparkReadOptions original =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path/to/dataset")
            .tableId(tableId)
            .namespaceImpl("rest")
            .namespaceConfig(namespaceConfig)
            .batchSize(1024)
            .pushDownFilters(false)
            .build();

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
    }

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    LanceSparkReadOptions deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (LanceSparkReadOptions) ois.readObject();
    }

    // Verify namespace config is preserved
    assertEquals(original.getNamespaceImpl(), deserialized.getNamespaceImpl());
    assertEquals(original.getNamespaceConfig(), deserialized.getNamespaceConfig());
    assertEquals(original.getTableId(), deserialized.getTableId());

    // Verify other fields are preserved
    assertEquals(original.getDatasetUri(), deserialized.getDatasetUri());
    assertEquals(original.getBatchSize(), deserialized.getBatchSize());
    assertEquals(original.isPushDownFilters(), deserialized.isPushDownFilters());

    // The transient namespace field should be null after deserialization
    // (actual reconstruction happens in LanceFragmentScanner)
    assertNull(deserialized.getNamespace());

    // But hasNamespaceConfig should still be true
    assertTrue(deserialized.hasNamespaceConfig());
  }

  @Test
  public void testSerializationWithoutNamespaceConfig() throws IOException, ClassNotFoundException {
    LanceSparkReadOptions original =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path/to/dataset")
            .batchSize(512)
            .build();

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
    }

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    LanceSparkReadOptions deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (LanceSparkReadOptions) ois.readObject();
    }

    // Verify fields are preserved
    assertEquals(original.getDatasetUri(), deserialized.getDatasetUri());
    assertEquals(original.getBatchSize(), deserialized.getBatchSize());

    // No namespace config
    assertNull(deserialized.getNamespaceImpl());
    assertNull(deserialized.getNamespaceConfig());
    assertFalse(deserialized.hasNamespaceConfig());
  }
}
