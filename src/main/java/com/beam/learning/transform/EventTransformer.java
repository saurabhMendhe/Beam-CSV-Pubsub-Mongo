package com.beam.learning.transform;

import com.beam.learning.transform.function.CSVParseFunction;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.Serializable;

import static com.beam.learning.common.constants.Constants.csvRecordTuple;
import static com.beam.learning.common.constants.Constants.errorTupleTag;

public class EventTransformer implements Serializable {
  private static final long serialVersionUID = -6040608819872251826L;

  public PCollectionTuple toEventPayload(PCollection<PubsubMessage> pubsubMessagePCollection) {
    return pubsubMessagePCollection.apply(
        "PubSub Message to CSV Record",
        ParDo.of(new CSVParseFunction())
            .withOutputTags(csvRecordTuple, TupleTagList.of(errorTupleTag)));
  }
}
