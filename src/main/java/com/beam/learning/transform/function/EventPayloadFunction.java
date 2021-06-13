package com.beam.learning.transform.function;

import com.beam.learning.common.constants.Constants;
import com.beam.learning.common.enums.ErrorCodes;
import com.beam.learning.common.exceptions.BaseException;
import com.beam.learning.common.utils.CommonUtils;
import com.beam.learning.common.utils.csv.Record;
import com.beam.learning.model.datamodel.CustomerInformation;
import com.beam.learning.model.datamodel.RawEventInformation;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class EventPayloadFunction extends DoFn<Record, CustomerInformation> {
  private static final long serialVersionUID = -5109140019316516109L;

  /**
   * Convert the csv record to RawEvent information
   *
   * @param record Csv record
   * @return rawEvent of type RawEventInformation
   */
  public RawEventInformation convertToRawEventInformation(Record record) {
    if (record != null) {
      RawEventInformation rawEventInformation = new RawEventInformation();
      rawEventInformation.setCustomerID(record.get(0));
      rawEventInformation.setItemID(record.get(1));
      rawEventInformation.setSex(record.get(2));
      rawEventInformation.setAge(record.get(3));
      rawEventInformation.setProfession(record.get(4));
      rawEventInformation.setCityType(record.get(5));
      rawEventInformation.setYearsInCity(record.get(6));
      rawEventInformation.setHaveChildren(record.get(7));
      rawEventInformation.setItemCategory1(record.get(8));
      rawEventInformation.setItemCategory2(record.get(9));
      rawEventInformation.setItemCategory3(record.get(10));
      rawEventInformation.setAmount(Double.parseDouble(record.get(11)));
      return rawEventInformation;
    }
    return null;
  }

  /**
   * Convert the RawEvent information to Customer information
   *
   * @param rawEvent information
   * @return Customer information
   */
  public CustomerInformation convertToCustomerInformation(RawEventInformation rawEventInformation) {
    if (rawEventInformation != null) {
      CustomerInformation customerInformation = new CustomerInformation();
      customerInformation.setSex(rawEventInformation.getSex());
      customerInformation.setAge(rawEventInformation.getAge());
      customerInformation.setCustomerId(rawEventInformation.getCustomerID());
      customerInformation.setNumberOfChildren(rawEventInformation.getHaveChildren());
      customerInformation.setNumberOfVisit(1);
      customerInformation.setLastVisitTimeStamp(CommonUtils.currentDate());
      return customerInformation;
    }
    return null;
  }

  @Setup
  public void setup() {}

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    try {
      Record record = processContext.element();
      RawEventInformation rawEventInformation = this.convertToRawEventInformation(record);
      CustomerInformation customerInformation =
          this.convertToCustomerInformation(rawEventInformation);
      processContext.output(customerInformation);
    } catch (Exception e) {
      BaseException baseException =
          new BaseException(
              String.valueOf(ErrorCodes.CONVERSION_TO_DATA_MODEL_ERROR),
              e.getMessage(),
              this.getClass().getSimpleName());
      processContext.output(Constants.errorTupleTag, baseException.toString());
    }
  }
}
