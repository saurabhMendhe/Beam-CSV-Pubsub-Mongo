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
public class RawEventInformation implements Serializable {
  private static final long serialVersionUID = 8448654239242371515L;
  private String customerID;
  private String itemID;
  private String sex;
  private String age;
  private String profession;
  private String cityType;
  private String yearsInCity;
  private String haveChildren;
  private String itemCategory1;
  private String itemCategory2;
  private String itemCategory3;
  private double amount;
}
