package com.beam.learning.model.datamodel;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@DefaultCoder(SerializableCoder.class)
public class Age implements Serializable {
  private static final long serialVersionUID = 8448654239242371515L;
  private int low;
  private int high;
}
