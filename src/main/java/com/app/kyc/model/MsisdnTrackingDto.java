package com.app.kyc.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MsisdnTrackingDto {
    private String msisdn;
    private String firstName;
    private String middleName;
    private String lastName;
    private String status;
    private String createdOn;

    private Long serviceProviderId;          // NEW
    private String identificationNumber;      // NEW
    private String identificationType;

    public MsisdnTrackingDto(String msisdn, String s, String s1, String s2, String status, String createdOnStr) {
    }
}
