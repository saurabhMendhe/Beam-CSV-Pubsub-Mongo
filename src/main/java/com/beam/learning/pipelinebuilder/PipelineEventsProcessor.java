package com.beam.learning.pipelinebuilder;

import com.beam.learning.transform.EventTransformer;
import com.beam.learning.common.coders.CustomCoderRegistry;
import com.beam.learning.common.utils.csv.Record;
import com.beam.learning.model.datamodel.CustomerInformation;
import com.beam.learning.options.StreamProcessorPipelineOptions;
import com.beam.learning.transform.StorageTransformer;
import com.beam.learning.transform.io.PubSubDataAccessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import java.io.Serializable;

import static com.beam.learning.common.constants.Constants.csvRecordTuple;
import static com.beam.learning.common.constants.Constants.errorTupleTag;
import static com.beam.learning.common.constants.Constants.eventModelTupleTag;
import static com.beam.learning.common.constants.Constants.writeTupleTag;

@Slf4j
public class PipelineEventsProcessor implements Serializable {
  private static final long serialVersionUID = -7051487427580716869L;
  private final transient Pipeline pipeline;
  private final transient PubSubDataAccessor pubSubDataAccessor;
  private final transient EventTransformer eventTransformer;
  private final transient StorageTransformer storageTransformer;
  private final transient StreamProcessorPipelineOptions options;

  public PipelineEventsProcessor(Pipeline pipeline, StreamProcessorPipelineOptions options) {
    this.pipeline = pipeline;
    this.options = options;
    this.pubSubDataAccessor = new PubSubDataAccessor();
    this.eventTransformer = new EventTransformer();
    this.storageTransformer = new StorageTransformer();
  }

  public void processPipelineStream() throws CannotProvideCoderException {
    /* Step 1 Read Events from pubsub subscription */
    final ValueProvider<String> subscription = options.getTopicSubscription();

    final PCollection<PubsubMessage> events =
        pubSubDataAccessor.readPubSubMessages(pipeline, subscription);

    /* Step 2 PubSubMessage to EventPayload */
    final PCollectionTuple eventPayloadTuple = eventTransformer.toEventPayload(events);

    /* Step 2.1 valid EventPayload from csv events */
    final PCollection<Record> eventPayloadPCollection =
        eventPayloadTuple
            .get(csvRecordTuple)
            .setCoder(CustomCoderRegistry.CODER_REGISTRY.getCoder(Record.class));

    /* Step 2.2 filter error and invalid csv events and post error
     * message to pubsub topic */
    handleInvalidInput(eventPayloadTuple);

    /* Step 3 EventPayload to DataModel */
    final PCollectionTuple dataModelTuple =
        storageTransformer.toEventModel(eventPayloadPCollection);

    /* Step 3.1 valid DataModel  object */
    final PCollection<CustomerInformation> dataModelPCollection =
        dataModelTuple.get(eventModelTupleTag);

    /* Step 3.2 filter error and invalid csv events and post error
     * message to pubsub topic */
    handleInvalidInput(dataModelTuple);

    /* Step 4 EventPayload to DataModel */
    final PCollectionTuple dbWriteTuple = storageTransformer.writeInDb(dataModelPCollection);

    /* Step 4.1 valid Write status  object */
    final PCollection<Boolean> dbWriteTuplePCollection = dbWriteTuple.get(writeTupleTag);

    /* Step 4.2 filter error and incase write operation fails and post error
     * message to pubsub topic */
    handleInvalidInput(dbWriteTuple);
  }

  private void handleInvalidInput(PCollectionTuple collectionTuple) {
    try {
      log.info("Publishing error message on the topic ...");
      PCollection<String> leadTimeErrorCollection = collectionTuple.get(errorTupleTag);
      // Create Pubsub Payload and publish the messgae on the topic.
    } catch (Exception exception) {
      log.error(
          "Error while creating/writing exception message...Exception : {}",
          exception.getMessage());
    }
  }
}
