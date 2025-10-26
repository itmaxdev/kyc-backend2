package com.app.kyc.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;


@Entity
@Table(name = "consumers_anomalies", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"consumer_id", "anomaly_id"}, name = "uq_cons_anom")
})
@Data
@Getter
@Setter
public class ConsumerAnomaly {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "consumer_id", nullable = false)
    private Consumer consumer;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "anomaly_id", nullable = false)
    private Anomaly anomaly;

    @Column(length = 1000)
    private String notes;  // ← This is missing in DB
}