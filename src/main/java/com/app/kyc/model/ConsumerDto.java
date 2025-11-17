package com.app.kyc.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.app.kyc.Masking.Mask;
import com.app.kyc.Masking.MaskType;
import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.Service;
import com.app.kyc.entity.ServiceProvider;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
@Data
@Setter
@Getter
public class ConsumerDto {
    List<AnomlyDto> anomlies=new ArrayList<AnomlyDto>();
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
    private String gender;
    private String birthDate;
    private String birthPlace;
    private String address;
    @Mask(MaskType.PHONE)
    private String alternateMsisdn1;
    @Mask(MaskType.PHONE)
    private String alternateMsisdn2;
    @Mask(MaskType.IDENTITY)
    private String identificationNumber;
    private String identificationType;
    private String nationality;
    private Date identityValitidyDate;
    private String identityCapturePath;
    private String subscriberType;
    private ServiceProvider serviceProvider;
    private List<Service> services;
    private String notes;
    private String consistentOn;
    private String vendorCode;
    private Boolean isConsistent;
    private List<ConsumerHistoryDto> consumerHistory;
    public  ConsumerDto(Consumer consumer,List<Anomaly> anomlies){
        this.id = consumer.getId();
        this.firstName = consumer.getFirstName();
        this.lastName = consumer.getLastName();
        this.birthDate = consumer.getBirthDate();
        this.address = consumer.getAddress();
        this.nationality = consumer.getNationality();
        this.registrationDate = consumer.getRegistrationDate();
        this.msisdn = consumer.getMsisdn();
        this.birthPlace = consumer.getBirthPlace();
        this.identificationNumber = consumer.getIdentificationNumber();
        this.identificationType = consumer.getIdentificationType();
        this.identityCapturePath = consumer.getIdentityCapturePath();
        this.identityValitidyDate = consumer.getIdentityValitidyDate();
        this.gender = consumer.getGender();
//        this.notes = consumer.getNotes();
        this.subscriberType = consumer.getSubscriberType();
        this.serviceProvider = consumer.getServiceProvider();
        this.services = consumer.getServices();
        this.middleName=consumer.getMiddleName();
        this.alternateMsisdn1=consumer.getAlternateMsisdn1();
        this.alternateMsisdn2=consumer.getAlternateMsisdn2();
        this.isConsistent = consumer.getIsConsistent();
        this.consistentOn=consumer.getConsistentOn();
        this.vendorCode=consumer.getVendorCode();
        
        
        if(anomlies!=null&& anomlies.size()>0){
            this.anomlies = anomlies.stream()
            .map(c->new AnomlyDto(c.getId(),c.getNote(),c.getStatus(),c.getAnomalyType(),null, c.getUpdateBy(), c.getUpdatedOn(), c.getReportedOn()))
            .collect(Collectors.toList());
            
        }
    }
    
    public ConsumerDto(Consumer consumer){
        this.id = consumer.getId();
        this.firstName = consumer.getFirstName();
        this.lastName = consumer.getLastName();
        this.birthDate = consumer.getBirthDate();
        this.address = consumer.getAddress();
        this.nationality = consumer.getNationality();
        this.registrationDate = consumer.getRegistrationDate();
        this.msisdn = consumer.getMsisdn();
        this.birthPlace = consumer.getBirthPlace();
        this.identificationNumber = consumer.getIdentificationNumber();
        this.identificationType = consumer.getIdentificationType();
        this.identityCapturePath = consumer.getIdentityCapturePath();
        this.identityValitidyDate = consumer.getIdentityValitidyDate();
        this.gender = consumer.getGender();
//        this.notes = consumer.getNotes();
        this.subscriberType = consumer.getSubscriberType();
        this.middleName=consumer.getMiddleName();
        this.alternateMsisdn1=consumer.getAlternateMsisdn1();
        this.alternateMsisdn2=consumer.getAlternateMsisdn2();
        this.isConsistent = consumer.getIsConsistent();
        this.consistentOn=consumer.getConsistentOn();
        this.vendorCode=consumer.getVendorCode();

    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerDto)) return false;
        ConsumerDto that = (ConsumerDto) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

}
