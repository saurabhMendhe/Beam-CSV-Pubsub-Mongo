package com.beam.learning.transform;

import com.beam.learning.transform.function.EventPayloadFunction;
import com.beam.learning.common.utils.csv.Record;
import com.beam.learning.model.datamodel.CustomerInformation;
import com.beam.learning.transform.function.DBWriteFunction;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.Serializable;

import static com.beam.learning.common.constants.Constants.errorTupleTag;
import static com.beam.learning.common.constants.Constants.eventModelTupleTag;
import static com.beam.learning.common.constants.Constants.writeTupleTag;

public class StorageTransformer implements Serializable {
  private static final long serialVersionUID = 6461080937370093552L;

  public PCollectionTuple toEventModel(PCollection<Record> listPCollection) {
    return listPCollection.apply(
        "CSV Record to Event Model",
        ParDo.of(new EventPayloadFunction())
            .withOutputTags(eventModelTupleTag, TupleTagList.of(errorTupleTag)));
  }

  public PCollectionTuple writeInDb(PCollection<CustomerInformation> dataModelPCollection) {
    return dataModelPCollection.apply(
        "Write in No SQL",
        ParDo.of(new DBWriteFunction())
            .withOutputTags(writeTupleTag, TupleTagList.of(errorTupleTag)));
  }
}
