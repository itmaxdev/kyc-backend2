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
    private String lastName;
    private String status;
    private String createdOn;
}
