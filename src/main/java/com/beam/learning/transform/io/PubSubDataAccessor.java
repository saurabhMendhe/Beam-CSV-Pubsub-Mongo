package com.beam.learning.transform.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;

@Slf4j
public class PubSubDataAccessor {
  public PCollection<PubsubMessage> readPubSubMessages(
      Pipeline pipeline, ValueProvider<String> subscriptionPath) {
    try {
      return pipeline.apply(
          "Read Pubsub Events",
          PubsubIO.readMessagesWithAttributes().fromSubscription(subscriptionPath));
    } catch (Exception exception) {
      log.error(
          "Error while reading events from pubsub subscription. {}, {}",
          subscriptionPath,
          exception);
    }
    return null;
  }

  /* write error messages to pubsub topic
   * */
  public void writeErrorMessagesToPubsub(
      PCollection<PubsubMessage> errorPCollection, ValueProvider<String> topicName) {
    errorPCollection.apply(
        "Write Error Messages to Pubsusb", PubsubIO.writeMessages().to(topicName));
  }
}
