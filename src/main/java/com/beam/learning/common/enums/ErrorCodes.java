package com.beam.learning.common.enums;

import java.io.Serializable;

/** Enums defined for the various errors occurred during the pipeline */
public enum ErrorCodes implements Serializable {
  PARSING_ERROR,
  CONVERSION_TO_DATA_MODEL_ERROR,
  PIPELINE_START_ERROR,
}
