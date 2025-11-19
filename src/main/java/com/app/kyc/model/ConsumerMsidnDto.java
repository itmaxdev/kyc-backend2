package com.app.kyc.model;

import com.app.kyc.Masking.Mask;
import com.app.kyc.Masking.MaskType;
import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.Service;
import com.app.kyc.entity.ServiceProvider;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Data
@Setter
@Getter
public class ConsumerMsidnDto {

    private Long id;
    @Mask(MaskType.PHONE)
    private String msisdn;
    private String registrationDate;
    @Mask(MaskType.NAME)
    private String firstName;
    @Mask(MaskType.NAME)
    private String middleName;
    @Mask(MaskType.NAME)
    private String lastName;
    @Mask(MaskType.IDENTITY)
    private String identificationNumber;
    private String identificationType;
    private Boolean isConsistent;
    private String consistentOn;

    private ServiceProvider serviceProvider;

    // ---------------- CONSTRUCTOR 1 (with anomalies) ----------------
    public ConsumerMsidnDto(Consumer consumer, List<Anomaly> anomlies) {

        this.id = consumer.getId();
        this.firstName = consumer.getFirstName();
        this.lastName = consumer.getLastName();

        this.registrationDate = consumer.getRegistrationDate();
        this.msisdn = consumer.getMsisdn();
        this.identificationNumber = consumer.getIdentificationNumber();
        this.identificationType = consumer.getIdentificationType();
        this.middleName = consumer.getMiddleName();
        this.serviceProvider=consumer.getServiceProvider();
        this.isConsistent = consumer.getIsConsistent();
        this.consistentOn=consumer.getConsistentOn();
    }

    // ---------------- CONSTRUCTOR 2 ----------------
    public ConsumerMsidnDto(Consumer consumer) {
        this.id = consumer.getId();
        this.firstName = consumer.getFirstName();
        this.lastName = consumer.getLastName();
        this.registrationDate = consumer.getRegistrationDate();
        this.msisdn = consumer.getMsisdn();
        this.identificationNumber = consumer.getIdentificationNumber();
        this.identificationType = consumer.getIdentificationType();
        this.middleName = consumer.getMiddleName();
        this.serviceProvider=consumer.getServiceProvider();
        this.isConsistent = consumer.getIsConsistent();
        this.consistentOn=consumer.getConsistentOn();
    }

    // ---------------- equals/hashCode ----------------
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerMsidnDto)) return false;
        ConsumerMsidnDto that = (ConsumerMsidnDto) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
