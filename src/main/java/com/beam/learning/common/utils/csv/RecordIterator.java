package com.beam.learning.common.utils.csv;

import lombok.Builder;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** Custom Iterator for the record that can be used to loop through the collection */
@Builder
public class RecordIterator implements Iterator<Record> {
  /** Tracks the current record */
  private Record current;

  /** Parser to get next record */
  private Parser parser;

  private Record getNextRecord() {
    try {
      return parser.nextRecord();
    } catch (final IOException e) {
      throw new IllegalStateException(
          e.getClass().getSimpleName() + " reading next record: " + e.toString(), e);
    }
  }

  @Override
  public boolean hasNext() {
    if (this.current == null) {
      this.current = this.getNextRecord();
    }

    return this.current != null;
  }

  @Override
  public Record next() {
    Record next = this.current;
    this.current = null;

    if (next == null) {
      next = this.getNextRecord();
      if (next == null) {
        throw new NoSuchElementException("No more CSV records available");
      }
    }

    return next;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
