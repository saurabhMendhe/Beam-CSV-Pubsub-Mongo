package com.beam.learning;

import com.beam.learning.common.enums.ErrorCodes;
import com.beam.learning.common.exceptions.BaseException;
import com.beam.learning.options.StreamProcessorPipelineOptions;
import com.beam.learning.pipelinebuilder.PipelineEventsProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@Slf4j
public class StreamProcessorApplication {
  public static void main(String[] args) {
    // main class for streaming application
    StreamProcessorPipelineOptions options;
    Pipeline pipeline;
    try {
      PipelineOptionsFactory.register(StreamProcessorPipelineOptions.class);
      options =
          PipelineOptionsFactory.fromArgs(args)
              .withValidation()
              .create()
              .as(StreamProcessorPipelineOptions.class);
      log.info("\nPipeline Options : {}", options);
      if (!options.getTopicSubscription().isAccessible()) {
        throw new BaseException(
            String.valueOf(ErrorCodes.PIPELINE_START_ERROR), options.toString());
      }

      log.info("----------Pipeline Created from Options.----------------");
      pipeline = Pipeline.create(options);
      PipelineEventsProcessor fireStoreStreamProcessor =
          new PipelineEventsProcessor(pipeline, options);
      fireStoreStreamProcessor.processPipelineStream();
      pipeline.run(options).waitUntilFinish();
    } catch (BaseException exception) {
      log.error(
          "----------- Cancelling Pipleine -------- : Error while creation stream pipeline options",
          exception);
    } catch (UnsupportedOperationException unsupportedOperationException) {
      log.error("Disregard This error for Pubsub unbounded source...");
    } catch (CannotProvideCoderException e) {
      e.printStackTrace();
    }
  }
}
