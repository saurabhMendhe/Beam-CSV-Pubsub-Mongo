package com.beam.learning.transform.function;

import com.google.gson.Gson;
import com.beam.learning.common.constants.Constants;
import com.beam.learning.common.utils.CommonUtils;
import com.beam.learning.model.datamodel.CustomerInformation;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.bson.Document;

import java.io.Serializable;

import static com.mongodb.client.model.Filters.eq;

@Slf4j
public class DBWriteFunction extends DoFn<CustomerInformation, Boolean> implements Serializable {
  private static final long serialVersionUID = 7009974352577315488L;
  private MongoCollection<Document> collection;
  private Gson gson;
  private MongoClient mongoClient;

  @Setup
  public void setUp() {
    gson = new Gson();
    final MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017");
    mongoClient = new MongoClient(connectionString);
    final MongoDatabase database = mongoClient.getDatabase(Constants.DATABASE_NAME);
    collection = database.getCollection(Constants.COLLECTION_NAME);
  }

  @Teardown
  public void tearDown() {
    mongoClient.close();
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    CustomerInformation customerInformation = processContext.element();
    if (customerInformation.isHavingChildren()) {
      final FindIterable<Document> customerId =
          collection.find(eq("customerId", customerInformation.getCustomerId()));
      Document existingCustomerDocument = customerId.first();
      if (existingCustomerDocument != null && !existingCustomerDocument.isEmpty()) {
        int numberOfVisit = (Integer) existingCustomerDocument.get("numberOfVisit") + 1;
        Document updatedDocument = new Document();
        updatedDocument.put("numberOfVisit", numberOfVisit);
        updatedDocument.put(
            "lastButOneVisitTimeStamp", existingCustomerDocument.get("lastVisitTimeStamp"));
        updatedDocument.put("lastVisitTimeStamp", CommonUtils.currentDate());
        collection.updateOne(
            eq("customerId", customerInformation.getCustomerId()),
            new Document("$set", updatedDocument));

      } else {
        Document document = Document.parse(gson.toJson(customerInformation));
        collection.insertOne(document);
      }
      processContext.output(true);
    }
  }
}
