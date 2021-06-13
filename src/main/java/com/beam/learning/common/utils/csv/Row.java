package com.beam.learning.common.utils.csv;

import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/** Get the values from reader of CSV file */
@Builder
@AllArgsConstructor
public class Row {
  private Reader reader;
  private Format format;

  /**
   * Get the next values from reader of CSV file
   *
   * @return Values of the record
   * @throws IOException
   */
  public String[] nextRow() throws IOException {
    BufferedReader bufferedReader = (BufferedReader) this.reader;
    final String s = bufferedReader.readLine();
    if (s != null) {
      return s.split(this.format.getDelimiter());
    }
    return new String[0];
  }
}
