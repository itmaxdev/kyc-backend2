package com.app.kyc.entity;

import javax.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "anomaly_statistics")
public class AnomalyStatistics {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "anomaly_id", nullable = false)
    private Long anomalyId;

    @Column(name = "partially_resolved_percentage", precision = 5, scale = 2, nullable = false)
    private BigDecimal partiallyResolvedPercentage;

    @Column(name = "recorded_on", nullable = false)
    private LocalDateTime recordedOn = LocalDateTime.now();

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Long getAnomalyId() { return anomalyId; }
    public void setAnomalyId(Long anomalyId) { this.anomalyId = anomalyId; }

    public BigDecimal getPartiallyResolvedPercentage() { return partiallyResolvedPercentage; }
    public void setPartiallyResolvedPercentage(BigDecimal partiallyResolvedPercentage) {
        this.partiallyResolvedPercentage = partiallyResolvedPercentage;
    }

    public LocalDateTime getRecordedOn() { return recordedOn; }
    public void setRecordedOn(LocalDateTime recordedOn) { this.recordedOn = recordedOn; }
}
