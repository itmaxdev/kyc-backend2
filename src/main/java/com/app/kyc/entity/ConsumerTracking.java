package com.app.kyc.entity;

import javax.persistence.*;
import java.util.Date;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Entity
@Data
@Setter
@Getter
@Table(name = "consumer_tracking")
public class ConsumerTracking {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "consistent_on", length = 50)
    private String consistentOn;

    /**
     * ✅ Fix 1:
     * Avoid cascade persist — we are referencing existing ServiceProvider.
     * Setting optional=false helps Hibernate manage it efficiently.
     */
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "service_provider_id", nullable = false)
    private ServiceProvider serviceProvider;

    @Column(name = "consumer_id", nullable = false)
    private Long consumerId;

    @Column(name = "is_consistent")
    private Boolean isConsistent;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "created_on")
    private Date createdOn;

    public ConsumerTracking() {
    }

    public ConsumerTracking(Long consumerId, ServiceProvider serviceProvider,
                            String consistentOn, Boolean isConsistent, Date createdOn) {
        this.consumerId = consumerId;
        this.serviceProvider = serviceProvider;
        this.consistentOn = consistentOn;
        this.isConsistent = isConsistent;
        this.createdOn = createdOn;
    }
}
