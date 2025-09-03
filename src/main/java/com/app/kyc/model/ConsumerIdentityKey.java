package com.app.kyc.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class ConsumerIdentityKey {
    private String firstName;
    private String lastName;
    private String idNumber;
    private String idType;
    private String msisdn; // optional, used only in some rules
}
