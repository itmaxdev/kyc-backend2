package com.app.kyc.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServiceProviderResponse {
    public Long id;
    public String name;
    public String address;
    public String companyPhoneNumber;
    public Boolean deleted;
    public String status;
    public Long approvedBy;
    public String color;
    public Long industryId;
    public String industryName;
}