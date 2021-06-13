package com.beam.learning.common.utils.csv;

import lombok.ToString;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/** A CSV record parsed from a CSV file. */
@ToString
@DefaultCoder(SerializableCoder.class)
public final class Record implements Serializable, Iterable<String> {

  private static final String[] EMPTY_STRING_ARRAY = new String[0];
  private static final long serialVersionUID = 1L;
  private final long recordNumber;
  private final String[] values;
  private final transient Parser parser;

  /**
   * CSV Record for use record in the file
   *
   * @param parser Parser object
   * @param values Values of the rows
   * @param recordNumber row/record number in the file
   */
  Record(final Parser parser, final String[] values, final long recordNumber) {
    this.recordNumber = recordNumber;
    this.values = values != null ? values : EMPTY_STRING_ARRAY;
    this.parser = parser;
  }

  private List<String> toList() {
    return Arrays.asList(values);
  }

  public String get(final int i) {
    return values[i];
  }

  @Override
  public Iterator<String> iterator() {
    return toList().iterator();
  }
}
