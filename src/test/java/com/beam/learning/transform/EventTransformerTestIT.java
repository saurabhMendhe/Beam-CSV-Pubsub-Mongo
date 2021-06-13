package com.beam.learning.transform;

import com.beam.learning.common.TestCommonUtils;
import com.beam.learning.common.utils.CommonUtils;
import com.beam.learning.model.datamodel.CustomerInformation;
import com.beam.learning.common.coders.CustomCoderRegistry;
import com.beam.learning.common.exceptions.BaseException;
import com.beam.learning.common.utils.csv.Record;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.beam.learning.common.constants.Constants.csvRecordTuple;
import static com.beam.learning.common.constants.Constants.eventModelTupleTag;
import static com.beam.learning.common.constants.Constants.writeTupleTag;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CommonUtils.class)
@PowerMockIgnore("javax.management.*")
public class EventTransformerTestIT {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void init() {}

  /** Scenario to check if the raw CSV input is able to convert to customer information */
  @Test
  @Category({NeedsRunner.class})
  public void convertCSVToCustomerPayload() {
    PowerMockito.mockStatic(CommonUtils.class);
    EventTransformer eventTransformer = new EventTransformer();
    Mockito.when(CommonUtils.currentDate()).thenReturn("2021-04-11");
    StorageTransformer storageTransformer = new StorageTransformer();
    Map<String, String> messageProperties = new HashMap<>();
    messageProperties.put("timestamp", String.valueOf(System.currentTimeMillis()));
    try {
      String payloadStr = TestCommonUtils.readFile("/input1.csv");
      PubsubMessage pubsubMessage =
          new PubsubMessage(payloadStr.getBytes(StandardCharsets.UTF_8), messageProperties);
      final PCollection<PubsubMessage> pubsubMessagePCollection =
          pipeline.apply(Create.of(pubsubMessage));
      final PCollectionTuple eventPayloadTuple =
          eventTransformer.toEventPayload(pubsubMessagePCollection);
      final PCollection<Record> eventPayloadPCollection =
          eventPayloadTuple
              .get(csvRecordTuple)
              .setCoder(CustomCoderRegistry.CODER_REGISTRY.getCoder(Record.class));
      final PCollectionTuple dataModelTuple =
          storageTransformer.toEventModel(eventPayloadPCollection);
      final PCollection<CustomerInformation> dataModelPCollection =
          dataModelTuple.get(eventModelTupleTag);

      CustomerInformation expectedCustomerInformation = new CustomerInformation();
      expectedCustomerInformation.setNumberOfChildren("1");
      expectedCustomerInformation.setCustomerId("1000001");
      expectedCustomerInformation.setSex("F");
      expectedCustomerInformation.setAge("0-17");
      expectedCustomerInformation.setNumberOfVisit(1);
      expectedCustomerInformation.setLastVisitTimeStamp("2021-04-11");
      PAssert.that(dataModelPCollection).containsInAnyOrder(expectedCustomerInformation);
      pipeline.run().waitUntilFinish();
    } catch (BaseException | CannotProvideCoderException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Scenario When Customer do not have children who can play in play ground. Hence cannot be store
   * in DB
   */
  @Test
  public void customerInfoWriteDataIntoDBNegative() {
    EventTransformer eventTransformer = new EventTransformer();
    StorageTransformer storageTransformer = new StorageTransformer();
    Map<String, String> messageProperties = new HashMap<>();
    messageProperties.put("timestamp", String.valueOf(System.currentTimeMillis()));
    try {
      String payloadStr = TestCommonUtils.readFile("/input1.csv");
      PubsubMessage pubsubMessage =
          new PubsubMessage(payloadStr.getBytes(StandardCharsets.UTF_8), messageProperties);
      final PCollection<PubsubMessage> pubsubMessagePCollection =
          pipeline.apply(Create.of(pubsubMessage));
      final PCollectionTuple eventPayloadTuple =
          eventTransformer.toEventPayload(pubsubMessagePCollection);
      final PCollection<Record> eventPayloadPCollection =
          eventPayloadTuple
              .get(csvRecordTuple)
              .setCoder(CustomCoderRegistry.CODER_REGISTRY.getCoder(Record.class));
      final PCollectionTuple dataModelTuple =
          storageTransformer.toEventModel(eventPayloadPCollection);
      final PCollection<CustomerInformation> dataModelPCollection =
          dataModelTuple.get(eventModelTupleTag);
      final PCollectionTuple dbWriteTuple = storageTransformer.writeInDb(dataModelPCollection);
      final PCollection<Boolean> dbWriteTuplePCollection = dbWriteTuple.get(writeTupleTag);
      PAssert.that(dbWriteTuplePCollection).empty();
      pipeline.run().waitUntilFinish();
    } catch (BaseException | CannotProvideCoderException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  /** Scenario When Customer have children who can play in play ground . Hence can be store in DB */
  @Test
  public void customerInfoWriteDataIntoDBPositive() {
    EventTransformer eventTransformer = new EventTransformer();
    StorageTransformer storageTransformer = new StorageTransformer();
    Map<String, String> messageProperties = new HashMap<>();
    messageProperties.put("timestamp", String.valueOf(System.currentTimeMillis()));
    try {
      String payloadStr = TestCommonUtils.readFile("/input4.csv");
      PubsubMessage pubsubMessage =
          new PubsubMessage(payloadStr.getBytes(StandardCharsets.UTF_8), messageProperties);
      final PCollection<PubsubMessage> pubsubMessagePCollection =
          pipeline.apply(Create.of(pubsubMessage));
      final PCollectionTuple eventPayloadTuple =
          eventTransformer.toEventPayload(pubsubMessagePCollection);
      final PCollection<Record> eventPayloadPCollection =
          eventPayloadTuple
              .get(csvRecordTuple)
              .setCoder(CustomCoderRegistry.CODER_REGISTRY.getCoder(Record.class));
      final PCollectionTuple dataModelTuple =
          storageTransformer.toEventModel(eventPayloadPCollection);
      final PCollection<CustomerInformation> dataModelPCollection =
          dataModelTuple.get(eventModelTupleTag);
      final PCollectionTuple dbWriteTuple = storageTransformer.writeInDb(dataModelPCollection);
      final PCollection<Boolean> dbWriteTuplePCollection = dbWriteTuple.get(writeTupleTag);
      PAssert.that(dbWriteTuplePCollection).containsInAnyOrder(true);
      pipeline.run().waitUntilFinish();
    } catch (BaseException | CannotProvideCoderException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
