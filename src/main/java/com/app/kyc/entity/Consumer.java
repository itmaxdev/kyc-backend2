package com.app.kyc.entity;

import java.util.*;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


@Entity
@Data
@Setter
@Getter
@Table(name = "consumers")
public class Consumer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
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
    private String createdOn;
    private String nationality;
    private Date identityValitidyDate;
    private String identityCapturePath;
    private String subscriberType;
    private Boolean isConsistent;
    private int consumerStatus;

    @OneToMany(cascade = {CascadeType.ALL}, mappedBy = "consumer")
    private List<ConsumerService> consumerService;

    @ManyToMany(cascade = {
            CascadeType.PERSIST,
            CascadeType.MERGE
    })
    @JoinTable(name = "consumers_services", joinColumns = @JoinColumn(name = "consumer_id"), inverseJoinColumns = @JoinColumn(name = "service_id"))
    private List<Service> services;

    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinTable(name = "consumers_anomalies", joinColumns = @JoinColumn(name = "consumer_id"), inverseJoinColumns = @JoinColumn(name = "anomaly_id"))
    List<Anomaly> anomalies = new ArrayList<>();


    @ManyToOne
    private ServiceProvider serviceProvider;


    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Consumer consumer = (Consumer) o;
        return msisdn.equals(consumer.msisdn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msisdn);
    }


}
