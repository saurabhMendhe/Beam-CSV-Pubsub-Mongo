package com.beam.learning.common.utils.csv;

import lombok.AllArgsConstructor;
import lombok.Data;

/** Specifies the format of a CSV file and parses input. */
@Data
@AllArgsConstructor
public class Format {
  /** Tells how each values are separated */
  private String delimiter;

  /** Does the csv input file has the header or not */
  private boolean firstRecordAsHeader;
}
