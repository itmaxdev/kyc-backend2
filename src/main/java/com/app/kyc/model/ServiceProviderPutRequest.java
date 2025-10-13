package com.app.kyc.model;



import com.fasterxml.jackson.annotation.JsonInclude;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServiceProviderPutRequest {
    @NotBlank @Size(max = 120)
    public String name;

    @Size(max = 255)
    public String address;

    @Pattern(regexp = "^[0-9+()\\-\\s]{5,30}$", message = "Invalid phone number")
    public String companyPhoneNumber;

    @NotNull
    public String status;       // "Active"/"Inactive" or enum name or "1"/"0"

    @NotNull
    public Boolean deleted;

    public Long approvedBy;

    @Size(max = 32)
    public String color;        // e.g., "rgb(189, 4, 4)"

    @NotNull
    public Long industryId;
}

