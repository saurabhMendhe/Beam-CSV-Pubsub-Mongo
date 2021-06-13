package com.beam.learning.common.constants;

import com.beam.learning.model.datamodel.CustomerInformation;
import com.beam.learning.common.utils.csv.Record;
import org.apache.beam.sdk.values.TupleTag;

/** Constants are defined her */
public class Constants {
  public static final TupleTag<Record> csvRecordTuple =
      new TupleTag<Record>() {
        private static final long serialVersionUID = 2496572311322345480L;
      };
  public static final TupleTag<CustomerInformation> eventModelTupleTag =
      new TupleTag<CustomerInformation>() {
        private static final long serialVersionUID = 2496572311322345480L;
      };
  public static final TupleTag<Boolean> writeTupleTag =
      new TupleTag<Boolean>() {
        private static final long serialVersionUID = 2496572311322345480L;
      };
  public static final TupleTag<String> errorTupleTag =
      new TupleTag<String>() {
        private static final long serialVersionUID = 2496575511322345480L;
      };
  public static final String HYPHEN_SEPARATOR = "-";
  public static final String PLUS_SEPARATOR = "+";
  public static final String SEMI_COLON = ";";
  public static final int PORT = 27017;
  public static final String DATABASE_NAME = "ikea_facilities";
  public static final String COLLECTION_NAME = "customer_data";
  public static final int AGE_UPPERBOUND = 45;
  public static final int AGE_LOWERBOUND = 18;
  public static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  private Constants() {}
}
