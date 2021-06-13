package com.beam.learning.transform.function;

import com.beam.learning.common.constants.Constants;
import com.beam.learning.common.enums.ErrorCodes;
import com.beam.learning.common.exceptions.BaseException;
import com.beam.learning.common.utils.csv.Format;
import com.beam.learning.common.utils.csv.Parser;
import com.beam.learning.common.utils.csv.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

@Slf4j
public class CSVParseFunction extends DoFn<PubsubMessage, Record> implements Serializable {
  private static final long serialVersionUID = 3166685197719754405L;
  private Format format;

  @Setup
  public void setUp() {
    format = new Format(Constants.SEMI_COLON, true);
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    final PubsubMessage input = processContext.element();
    try {
      final String eventPayloadStr = new String(input.getPayload(), StandardCharsets.UTF_8);
      final Reader inputString = new StringReader(eventPayloadStr);
      final BufferedReader reader = new BufferedReader(inputString);
      final Parser parser = new Parser(reader, format);
      log.info("Header : {}", parser.getHeader()[0]);
      final Iterator<Record> iterator = parser.iterator();
      while (iterator.hasNext()) {
        Record record = iterator.next();
        processContext.output(record);
      }
    } catch (Exception e) {
      BaseException baseException =
          new BaseException(
              String.valueOf(ErrorCodes.PARSING_ERROR),
              e.getMessage(),
              this.getClass().getSimpleName());
      processContext.output(Constants.errorTupleTag, baseException.toString());
    }
  }
}
