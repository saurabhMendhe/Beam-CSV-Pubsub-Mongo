package com.beam.learning.model.datamodel;

import com.beam.learning.common.utils.CommonUtils;
import com.beam.learning.common.constants.Constants;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerInformation implements Serializable {
  private String customerId;
  private String numberOfChildren;
  private String sex;
  private Integer numberOfVisit;
  private String lastVisitTimeStamp;
  private Age age;
  private String lastButOneVisitTimeStamp;

  public boolean isHavingChildren() {
    boolean returnValue = false;
    if (("1".equalsIgnoreCase(this.numberOfChildren)
            || "yes".equalsIgnoreCase(this.numberOfChildren)
            || "true".equalsIgnoreCase(this.numberOfChildren)
            || "on".equalsIgnoreCase(this.numberOfChildren))
        && (this.age.getLow() > Constants.AGE_LOWERBOUND
            && this.age.getHigh() < Constants.AGE_UPPERBOUND)) {
      returnValue = true;
    }
    return returnValue;
  }

  public void setAge(String ageVal) {
    Age ageObj = new Age();
    if (StringUtils.isNotBlank(ageVal)) {
      if (ageVal.contains("-")) {
        String[] ages = ageVal.split(Constants.HYPHEN_SEPARATOR);
        ageObj.setLow(Integer.parseInt(ages[0]));
        ageObj.setHigh(Integer.parseInt(ages[1]));
      } else if (ageVal.contains("+")) {
        String ageStr = ageVal.replace(Constants.PLUS_SEPARATOR, "");
        ageObj.setLow(Integer.parseInt(ageStr));
        ageObj.setHigh(100);
      } else {
        ageObj.setLow(0);
        ageObj.setHigh(0);
      }
    }
    this.age = ageObj;
  }

  public CustomerInformation updateCustomerInformation(CustomerInformation existingCustomerInfo) {
    CustomerInformation newCustomerInformation = new CustomerInformation();
    newCustomerInformation.age = existingCustomerInfo.getAge();
    newCustomerInformation.setSex(existingCustomerInfo.getSex());
    newCustomerInformation.setCustomerId(existingCustomerInfo.getCustomerId());
    newCustomerInformation.setNumberOfChildren(existingCustomerInfo.getNumberOfChildren());
    newCustomerInformation.setLastButOneVisitTimeStamp(
        existingCustomerInfo.getLastVisitTimeStamp());
    newCustomerInformation.numberOfVisit = existingCustomerInfo.numberOfVisit + 1;
    newCustomerInformation.lastVisitTimeStamp = CommonUtils.currentDate();
    return newCustomerInformation;
  }
}
