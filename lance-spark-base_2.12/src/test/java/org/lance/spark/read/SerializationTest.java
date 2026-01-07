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
package org.lance.spark.read;

import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for serialization of Lance Spark connector classes.
 *
 * <p>These tests verify that the reader classes can be properly serialized, which is required for
 * Spark operations like persist() with replication (e.g., MEMORY_AND_DISK_2).
 *
 * <p>Note: Full integration testing with persist(MEMORY_AND_DISK_2) is covered in
 * BaseSparkConnectorReadTest.testPersistWithReplicatedStorageLevel()
 */
public class SerializationTest {

  @Test
  public void testLanceInputPartitionSerializable() throws Exception {
    LanceInputPartition partition = TestUtils.TestTable1Config.inputPartition;

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(partition);
    oos.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceInputPartition deserialized = (LanceInputPartition) ois.readObject();
    ois.close();

    // Verify
    assertNotNull(deserialized);
    assertEquals(partition.getPartitionId(), deserialized.getPartitionId());
    assertEquals(partition.getScanId(), deserialized.getScanId());
    assertEquals(partition.getSchema(), deserialized.getSchema());
    assertEquals(
        partition.getLanceSplit().getFragments(), deserialized.getLanceSplit().getFragments());
  }

  @Test
  public void testLanceInputPartitionWithAllFieldsSerializable() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("col1", DataTypes.StringType, true),
              DataTypes.createStructField("col2", DataTypes.IntegerType, false),
            });

    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.from(TestUtils.TestTable1Config.datasetUri);
    LanceSplit split = new LanceSplit(Arrays.asList(0, 1, 2));

    LanceInputPartition partition =
        new LanceInputPartition(
            schema,
            5,
            split,
            readOptions,
            Optional.of("col1 = 'test'"),
            Optional.of(100),
            Optional.of(10),
            Optional.empty(),
            Optional.empty(),
            "scan-123");

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(partition);
    oos.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceInputPartition deserialized = (LanceInputPartition) ois.readObject();
    ois.close();

    // Verify all fields
    assertNotNull(deserialized);
    assertEquals(schema, deserialized.getSchema());
    assertEquals(5, deserialized.getPartitionId());
    assertEquals(Arrays.asList(0, 1, 2), deserialized.getLanceSplit().getFragments());
    assertEquals("col1 = 'test'", deserialized.getWhereCondition().get());
    assertEquals(Integer.valueOf(100), deserialized.getLimit().get());
    assertEquals(Integer.valueOf(10), deserialized.getOffset().get());
    assertEquals("scan-123", deserialized.getScanId());
  }

  @Test
  public void testLanceSplitSerializable() throws Exception {
    LanceSplit split = new LanceSplit(Arrays.asList(0, 1, 2, 3, 4));

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(split);
    oos.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSplit deserialized = (LanceSplit) ois.readObject();
    ois.close();

    assertNotNull(deserialized);
    assertEquals(Arrays.asList(0, 1, 2, 3, 4), deserialized.getFragments());
  }

  @Test
  public void testLanceSparkReadOptionsSerializable() throws Exception {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.from(TestUtils.TestTable1Config.datasetUri);

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(readOptions);
    oos.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserialized = (LanceSparkReadOptions) ois.readObject();
    ois.close();

    assertNotNull(deserialized);
    assertEquals(readOptions.getDatasetUri(), deserialized.getDatasetUri());
  }
}
