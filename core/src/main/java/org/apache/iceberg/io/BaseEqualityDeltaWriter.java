/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.io;

import java.io.IOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

public class BaseEqualityDeltaWriter<T> implements EqualityDeltaWriter<T> {

  private final PartitioningWriter<T, DataWriteResult> insertWriter;

  private final PartitioningWriter<T, DeleteWriteResult> deleteWriter;
  private boolean closed;

  public BaseEqualityDeltaWriter(
      PartitioningWriter<T, DataWriteResult> insertWriter,
      PartitioningWriter<T, DeleteWriteResult> deleteWriter) {
    this.insertWriter = insertWriter;
    this.deleteWriter = deleteWriter;
  }

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) {
    insertWriter.write(row, spec, partition);
  }

  @Override
  public void delete(T row, PartitionSpec spec, StructLike partition) {
    deleteWriter.write(row, spec, partition);
  }

  @Override
  public void deleteKey(T key, PartitionSpec spec, StructLike partition) {
    deleteWriter.write(key, spec, partition);
  }

  @Override
  public WriteResult result() {
    //    Preconditions.checkState(closed, "Cannot get result from unclosed writer");

    DeleteWriteResult deleteWriteResult = deleteWriter.result();

    return WriteResult.builder()
        .addDataFiles(dataFiles())
        .addDeleteFiles(deleteWriteResult.deleteFiles())
        .addReferencedDataFiles(deleteWriteResult.referencedDataFiles())
        .build();
  }

  private Iterable<DataFile> dataFiles() {
    return insertWriter.result().dataFiles();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      insertWriter.close();
      deleteWriter.close();

      this.closed = true;
    }
  }
}
