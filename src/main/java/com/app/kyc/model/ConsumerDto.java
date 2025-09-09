package com.app.kyc.model;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.Service;
import com.app.kyc.entity.ServiceProvider;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
@Data
@Setter
@Getter
public class ConsumerDto {
    List<AnomlyDto> anomlies=new ArrayList<AnomlyDto>();
    private Long id;
    private String msisdn;
    private String registrationDate;
    private String firstName;
    private String middleName;
    private String lastName;
    private String gender;
    private String birthDate;
    private String birthPlace;
    private String address;
    private String alternateMsisdn1;
    private String alternateMsisdn2;
    private String identificationNumber;
    private String identificationType;
    private String nationality;
    private Date identityValitidyDate;
    private String identityCapturePath;
    private String subscriberType;
    private ServiceProvider serviceProvider;
    private List<Service> services;
    private String notes;
    private Boolean isConsistent;
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

    }

}
