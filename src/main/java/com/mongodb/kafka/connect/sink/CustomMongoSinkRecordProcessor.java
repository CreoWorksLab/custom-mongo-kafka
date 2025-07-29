/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_BATCH_SIZE_CONFIG;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.kafka.connect.sink.dlq.ErrorReporter;

final class CustomMongoSinkRecordProcessor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(CustomMongoSinkRecordProcessor.class);

  static List<List<CustomMongoProcessedSinkRecordData>> orderedGroupByTopicAndNamespace(
      final Collection<SinkRecord> records,
      final MongoSinkConfig sinkConfig,
      final ErrorReporter errorReporter) {
    LOGGER.debug("Number of sink records to process: {}", records.size());

    List<List<CustomMongoProcessedSinkRecordData>> orderedProcessedSinkRecordData =
        new ArrayList<>();
    List<CustomMongoProcessedSinkRecordData> currentGroup = new ArrayList<>();
    CustomMongoProcessedSinkRecordData previous = null;

    for (SinkRecord record : records) {
      CustomMongoProcessedSinkRecordData processedData =
          new CustomMongoProcessedSinkRecordData(record, sinkConfig);

      if (processedData.getException() != null) {
        errorReporter.report(processedData.getSinkRecord(), processedData.getException());
        continue;
      } else if (processedData.getNamespace() == null || processedData.getWriteModel() == null) {
        // Some CDC events can be Noops (eg tombstone events)
        continue;
      }

      if (previous == null) {
        previous = processedData;
      }

      int maxBatchSize = processedData.getConfig().getInt(MAX_BATCH_SIZE_CONFIG);
      if (maxBatchSize > 0 && currentGroup.size() == maxBatchSize
          || !previous.getSinkRecord().topic().equals(processedData.getSinkRecord().topic())
          || !previous.getNamespace().equals(processedData.getNamespace())) {

        orderedProcessedSinkRecordData.add(currentGroup);
        currentGroup = new ArrayList<>();
      }
      previous = processedData;
      currentGroup.add(processedData);
    }

    if (!currentGroup.isEmpty()) {
      orderedProcessedSinkRecordData.add(currentGroup);
    }
    return orderedProcessedSinkRecordData;
  }

  private CustomMongoSinkRecordProcessor() {}
}
