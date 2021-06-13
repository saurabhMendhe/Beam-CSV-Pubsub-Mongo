package com.beam.learning.common.exceptions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;

/**
 * BaseExeption is created for Exceptions that are specific to the business logic and workflow.
 * These help the application users or the developers understand what the exact problem is.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
@SuppressWarnings("ALL")
public class BaseException extends Exception implements Serializable {

  private static final long serialVersionUID = -5512774183452093041L;
  static ObjectMapper objectMapper;

  static {
    objectMapper = new ObjectMapper();
  }

  private String errorCode;
  private String errorClass;
  private String errorMessage;
  private Object errorObject;

  public BaseException(String errorCode, String errorMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public BaseException(String errorCode, String errorMessage, String errorClass) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.errorClass = errorClass;
  }

  /**
   * Constructs a new exception with the specified detail message, exception cause & error code.
   *
   * @param errorCode the error code of exception.
   * @param errorMessage the detail message.
   * @param cause the cause of exception.
   */
  public BaseException(String errorCode, String errorMessage, Throwable cause) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.errorObject = cause;
  }

  @SneakyThrows
  /*Construct Json error object to be published on error pubsub topic*/
  public String toString() {

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.createObjectNode();
    JsonNode objectNode = mapper.createObjectNode();
    ((ObjectNode) objectNode).put("ErrorCode", this.errorCode);
    ((ObjectNode) objectNode).put("ErrorClass", this.errorClass);
    ((ObjectNode) objectNode).put("ErrorMessage", this.errorMessage);
    ((ObjectNode) objectNode).put("SupportingData", String.valueOf(this.errorObject));
    ((ObjectNode) jsonNode).put("BaseException", objectNode);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
  }
}
