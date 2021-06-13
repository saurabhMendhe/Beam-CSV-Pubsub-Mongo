package com.beam.learning.common.utils.csv;

import lombok.Data;
import org.junit.Assert;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Parses CSV files according to the specified format and based on Reader. */
@Data
public final class Parser implements Iterable<Record> {
  private final RecordIterator recordIterator;
  private String[] recordList = new String[0];
  private long recordNumber;
  private Row row;
  private String[] header;
  private Format format;

  /**
   * Creates the parser using the reader and format
   *
   * @param reader A Buffer Reader containing the CSV input
   * @param format the CSVFormat used for CSV parsing. Must not be null.
   * @throws IOException
   */
  public Parser(final Reader reader, final Format format) throws IOException {
    Assert.assertNotNull(reader);
    Assert.assertNotNull(format);
    this.format = format;
    this.row = new Row(reader, format);
    this.header = createHeader();
    this.recordIterator = new RecordIterator(null, this);
  }

  /**
   * Get the records in List
   *
   * @return
   * @throws IOException
   */
  public List<Record> getRecords() throws IOException {
    Record rec;
    final List<Record> records = new ArrayList<>();
    while ((rec = this.nextRecord()) != null) {
      records.add(rec);
    }
    return records;
  }

  /**
   * Returns the Iterator on the record.
   *
   * @return
   */
  @Override
  public Iterator<Record> iterator() {
    return recordIterator;
  }

  public Record nextRecord() throws IOException {
    Record result = null;
    this.recordList = this.row.nextRow();
    if (this.recordList.length != 0) {
      this.recordNumber++;
      result = new Record(this, this.recordList, this.recordNumber);
    }
    return result;
  }

  private String[] createHeader() throws IOException {
    if (this.format.isFirstRecordAsHeader()) {
      return this.row.nextRow();
    }
    return new String[0];
  }
}
