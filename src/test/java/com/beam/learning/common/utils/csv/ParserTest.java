package com.beam.learning.common.utils.csv;

import com.beam.learning.common.TestCommonUtils;
import com.beam.learning.common.exceptions.BaseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;

@Slf4j
public class ParserTest {

  @Before
  public void setUp() throws Exception {}

  /**
   * Scenario when header and customer info is present in the payload.
   *
   * @throws BaseException
   */
  @Test
  public void csvParserHappyScenario() throws BaseException {
    String payloadStr = TestCommonUtils.readFile("/input.csv");
    Reader inputString = new StringReader(payloadStr);
    BufferedReader reader = new BufferedReader(inputString);
    Format format = new Format(";", true);
    try {
      Parser parser = new Parser(reader, format);
      log.info("Header : {}", parser.getHeader()[0]);
      final Iterator<Record> iterator = parser.iterator();
      while (iterator.hasNext()) {
        Record record = iterator.next();
        Assert.assertNotNull(record);
      }
    } catch (final IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Scenario when only header is present in the payload, no customer info
   *
   * @throws BaseException
   */
  @Test
  public void csvParserWithCustomerInfoMissing() throws BaseException {
    String payloadStr = TestCommonUtils.readFile("/input2.csv");
    Reader inputString = new StringReader(payloadStr);
    BufferedReader reader = new BufferedReader(inputString);
    Format format = new Format(";", true);
    try {
      Parser parser = new Parser(reader, format);
      log.info("Header : {}", parser.getHeader()[0]);
      final Iterator<Record> iterator = parser.iterator();
      Assert.assertFalse(iterator.hasNext());

    } catch (final IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Scenario when only customer info is present and header is not-present in the payload
   *
   * @throws BaseException
   */
  @Test
  public void csvParserWithHeaderInfoMissing() throws BaseException {
    String payloadStr = TestCommonUtils.readFile("/input3.csv");
    Reader inputString = new StringReader(payloadStr);
    BufferedReader reader = new BufferedReader(inputString);
    Format format = new Format(";", false);
    try {
      Parser parser = new Parser(reader, format);
      Assert.assertNotNull(parser.getHeader());
      final Iterator<Record> iterator = parser.iterator();
      Assert.assertTrue(iterator.hasNext());
    } catch (final IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Scenario when customer info and header is not-present in the payload
   *
   * @throws BaseException
   */
  @Test
  public void csvParserWithHeaderAndCustomerInfoMissing() throws BaseException {
    String payloadStr = StringUtils.EMPTY;
    Reader inputString = new StringReader(payloadStr);
    BufferedReader reader = new BufferedReader(inputString);
    Format format = new Format(";", false);
    try {
      Parser parser = new Parser(reader, format);
      Assert.assertNotNull(parser.getHeader());
      final Iterator<Record> iterator = parser.iterator();
      Assert.assertFalse(iterator.hasNext());
    } catch (final IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
