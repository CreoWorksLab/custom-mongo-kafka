package com.mongodb.kafka.connect.sink.writemodel.strategy;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class CustomInsertManyWriteModelStrategy implements CustomWriteModelStrategy {
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  @Override
  public List<WriteModel<BsonDocument>> createWriteModel(SinkDocument sinkDocument) {
    BsonDocument valueDoc =
        sinkDocument
            .getValueDoc()
            .orElseThrow(() -> new DataException("Sink record value missing"));

    String companyId = valueDoc.getString("companyId").getValue();
    String engineId = valueDoc.getString("engineId").getValue();
    String dateTimeStr = valueDoc.getString("dateTime").getValue();

    BsonDocument data = valueDoc.getDocument("data");
    String deviceId = data.getString("deviceId").getValue();

    List<WriteModel<BsonDocument>> writes = new ArrayList<>();
    for (BsonValue channelValue : data.getArray("channel")) {
      BsonDocument chan = channelValue.asDocument();
      String channelId = chan.getString("channelId").getValue();
      String valuesStr = chan.getString("value").getValue();

      double sum = Arrays.stream(valuesStr.split(",")).mapToDouble(Double::parseDouble).sum();

      // target 문서 생성
      BsonDocument target = new BsonDocument();
      LocalDateTime ldt = LocalDateTime.parse(dateTimeStr, FORMATTER);
      long epochMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
      target.put("timestamp", new BsonDateTime(epochMillis));

      // metadata 하위문서
      BsonDocument md =
          new BsonDocument()
              .append("companyId", new BsonString(companyId))
              .append("deviceId", new BsonString(deviceId))
              .append("channelId", new BsonString(channelId))
              .append("engineId", new BsonString(engineId));
      target.put("metadata", md);

      // value 필드
      target.put("value", new BsonString(String.valueOf(sum)));

      writes.add(new InsertOneModel<>(target));
    }

    return writes;
  }
}
