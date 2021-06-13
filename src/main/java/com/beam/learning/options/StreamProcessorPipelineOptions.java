package com.beam.learning.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface StreamProcessorPipelineOptions extends DataflowPipelineOptions {

  @Description("PubSub Subscription for csv events")
  @Validation.Required
  ValueProvider<String> getTopicSubscription();

  void setTopicSubscription(ValueProvider<String> topicSubscription);
}
