package com.beam.learning.common.coders;

import com.beam.learning.common.utils.csv.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A Coder of type CsvRecord defines how to encode and decode values of type CsvRecord into byte
 * streams.
 */
@Slf4j
public class CsvRecordCoder extends Coder<Record> {

  private static final long serialVersionUID = -2748583335738784509L;

  @Override
  public void encode(Record value, OutputStream outStream) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(outStream);
    oos.writeObject(value);
    oos.flush();
  }

  @Override
  public Record decode(InputStream inStream) throws IOException {
    ObjectInputStream ois = new ObjectInputStream(inStream);
    Record csr = null;
    try {
      csr = (Record) ois.readObject();
    } catch (Exception exception) {
      log.error("Error while performing test action. Exception : {}", exception.getMessage());
    }

    return csr;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return new ArrayList<>();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // Nothing to implement
  }
}
