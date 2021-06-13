package com.beam.learning.common.utils;

import com.beam.learning.common.constants.Constants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** Utility class for the common functionality */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommonUtils {

  /** @return the current date and time */
  public static String currentDate() {
    final LocalDateTime now = LocalDateTime.now();
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);
    return now.format(formatter);
  }
}
