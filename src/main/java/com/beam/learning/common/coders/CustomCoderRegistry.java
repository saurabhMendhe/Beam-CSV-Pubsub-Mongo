package com.beam.learning.common.coders;

import com.beam.learning.common.utils.csv.Record;
import org.apache.beam.sdk.coders.CoderRegistry;

import java.io.Serializable;

/** A CustomCoderRegistry allows creating a Coder for a given Java class or type descriptor. */
public class CustomCoderRegistry implements Serializable {
  public static final CoderRegistry CODER_REGISTRY;
  private static final long serialVersionUID = -5756499636097910675L;

  static {
    CODER_REGISTRY = CoderRegistry.createDefault();
    CODER_REGISTRY.registerCoderForClass(Record.class, new CsvRecordCoder());
  }
}
