package com.app.kyc.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.persistence.*;
import java.util.*;

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

    @Column(name = "consistent_on")
    private String consistentOn;

    @Column(name = "vendor_code")
    private String vendorCode;

    @Column(name = "vodacom_transaction_id", unique = true, length = 200)
    private String vodacomTransactionId;

    @Column(name = "orange_transaction_id", unique = true, length = 200)
    private String orangeTransactionId;

    @Column(name = "airtel_transaction_id", length = 200)
    private String airtelTransactionId;

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "consumer")
    private List<ConsumerService> consumerService;

    @ManyToMany(cascade = {CascadeType.MERGE, CascadeType.PERSIST})
    @JoinTable(
            name = "consumers_services",
            joinColumns = @JoinColumn(name = "consumer_id"),
            inverseJoinColumns = @JoinColumn(name = "service_id")
    )
    private List<Service> services;

    /**
     * ✅ FIXED:
     *  - Removed CascadeType.ALL (no re-inserts)
     *  - Added unique constraint for safety
     *  - Lazy fetch to avoid unnecessary joins
     */
    @ManyToMany(fetch = FetchType.LAZY, cascade = {CascadeType.MERGE, CascadeType.PERSIST})
    @JoinTable(
            name = "consumers_anomalies",
            joinColumns = @JoinColumn(name = "consumer_id"),
            inverseJoinColumns = @JoinColumn(name = "anomaly_id"),
            uniqueConstraints = {
                    @UniqueConstraint(name = "uq_cons_anom", columnNames = {"consumer_id", "anomaly_id"})
            }
    )
    private List<Anomaly> anomalies = new ArrayList<>();

    @ManyToOne
    private ServiceProvider serviceProvider;

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Consumer)) return false;
        Consumer consumer = (Consumer) o;
        return Objects.equals(msisdn, consumer.msisdn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msisdn);
    }
}
