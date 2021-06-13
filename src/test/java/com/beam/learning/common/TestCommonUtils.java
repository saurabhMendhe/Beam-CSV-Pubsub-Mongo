package com.beam.learning.common;

import com.beam.learning.common.exceptions.BaseException;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

@Slf4j
public class TestCommonUtils {

  public static String readFile(String filename) throws BaseException {
    byte[] bytes = new byte[0];
    try {
      final InputStream inputStream = TestCommonUtils.class.getResourceAsStream(filename);
      if (inputStream != null) {
        final Reader reader = new BufferedReader(new InputStreamReader(inputStream));

        char[] charArray = new char[8 * 1024];
        StringBuilder builder = new StringBuilder();
        int numCharsRead;
        while ((numCharsRead = reader.read(charArray, 0, charArray.length)) != -1) {
          builder.append(charArray, 0, numCharsRead);
        }
        bytes = builder.toString().getBytes(StandardCharsets.UTF_8);
        reader.close();
      }

    } catch (Exception exception) {
      log.error("Exception occurred : [CommonUtils:toString] ", exception);
      throw new BaseException(
          "CommonUtils", "Error while reading the file. Exception : " + exception, filename);
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
