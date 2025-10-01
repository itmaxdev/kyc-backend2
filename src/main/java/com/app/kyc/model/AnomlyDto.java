package com.app.kyc.model;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.AnomalyType;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.User;
import lombok.Getter;
import lombok.Setter;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class AnomlyDto {
    private Long id;

    @Getter
    @Setter
    private String formattedId;

    private String note;
    private Date reportedOn;
    private User reportedBy;
    private Date updatedOn;
    private String updateBy;
    private Integer effectedRecords;
    private AnomalyStatus status;
    private AnomalyType anomalyType;
    private List<ConsumerDto> consumers = new ArrayList<>();

    // ✅ Main constructor
    public AnomlyDto(Anomaly anomaly) {
        this.id = anomaly.getId();
        this.note = anomaly.getNote();
        this.status = anomaly.getStatus();
        this.anomalyType = anomaly.getAnomalyType();
        this.reportedOn = anomaly.getReportedOn();
        this.reportedBy = anomaly.getReportedBy();
        this.updatedOn = anomaly.getUpdatedOn();
        this.updateBy = anomaly.getUpdateBy();

        List<Consumer> consumers = anomaly.getConsumers();

        if (consumers != null && !consumers.isEmpty()) {
            this.consumers = consumers.stream()
                    .filter(c -> c.getConsumerStatus() == 0) // keep active only
                    .map(c -> new ConsumerDto(c, null))
                    .collect(Collectors.toList());

            // ✅ Prefer vendorCode
            String vendorCode = this.consumers.get(0).getVendorCode();
            if (vendorCode != null && !vendorCode.isBlank()) {
                this.formattedId = vendorCode;
            } else {
                // fallback
                this.formattedId = consumers.get(0).getServiceProvider().getName()
                        + "_" + new SimpleDateFormat("ddMMyyyy").format(anomaly.getReportedOn())
                        + "_" + this.id;
            }
        } else {
            // ✅ fallback when no consumers exist
            this.consumers = new ArrayList<>();
            this.formattedId = "ANOMALY_" + this.id;
        }
    }

    // ✅ Special constructor (status=6 case)
    public AnomlyDto(Anomaly anomaly, int temp) {
        this(anomaly); // reuse main constructor

        // Only override if vendorCode not available
        List<Consumer> consumers = anomaly.getConsumers();
        if ((this.formattedId == null || this.formattedId.isBlank())
                && consumers != null && !consumers.isEmpty()) {
            this.formattedId = consumers.get(0).getServiceProvider().getName()
                    + "_" + new SimpleDateFormat("ddMMyyyy").format(anomaly.getReportedOn())
                    + "_" + this.id;
        } else if (this.formattedId == null || this.formattedId.isBlank()) {
            this.formattedId = "ANOMALY_" + this.id;
        }
    }

    // ✅ Another constructor for manual building
    public AnomlyDto(Long id, String note, AnomalyStatus status, AnomalyType anomalyType,
                     List<Consumer> consumers, String updateBy, Date updatedOn, Date reportedOn) {
        this.id = id;
        this.note = note;
        this.status = status;
        this.anomalyType = anomalyType;
        this.updateBy = updateBy;
        this.updatedOn = updatedOn;
        this.reportedOn = reportedOn;

        if (consumers != null && !consumers.isEmpty()) {
            this.consumers = consumers.stream()
                    .map(c -> new ConsumerDto(c, null))
                    .collect(Collectors.toList());

            String vendorCode = this.consumers.get(0).getVendorCode();
            if (vendorCode != null && !vendorCode.isBlank()) {
                this.formattedId = vendorCode;
            } else {
                this.formattedId = consumers.get(0).getServiceProvider().getName()
                        + "_" + new SimpleDateFormat("ddMMyyyy").format(reportedOn)
                        + "_" + this.id;
            }
        } else {
            this.formattedId = "ANOMALY_" + this.id;
        }
    }

    public AnomlyDto() {
    }

    // Getters & Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public AnomalyStatus getStatus() {
        return status;
    }

    public void setStatus(AnomalyStatus status) {
        this.status = status;
    }

    public AnomalyType getAnomalyType() {
        return anomalyType;
    }

    public void setAnomalyType(AnomalyType anomalyType) {
        this.anomalyType = anomalyType;
    }

    public List<ConsumerDto> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<ConsumerDto> consumers) {
        this.consumers = consumers;
    }

    public User getReportedBy() {
        return reportedBy;
    }

    public void setReportedBy(User reportedBy) {
        this.reportedBy = reportedBy;
    }

    public Date getReportedOn() {
        return reportedOn;
    }

    public void setReportedOn(Date reportedOn) {
        this.reportedOn = reportedOn;
    }

    public Date getUpdatedOn() {
        return updatedOn;
    }

    public void setUpdatedOn(Date updatedOn) {
        this.updatedOn = updatedOn;
    }

    public String getUpdatedBy() {
        return updateBy;
    }

    public void setUpdateBy(String updatedBy) {
        this.updateBy = updatedBy;
    }

    public Integer getEffectedRecords() {
        return effectedRecords;
    }

    public void setEffectedRecords(Integer effectedRecords) {
        this.effectedRecords = effectedRecords;
    }
}
