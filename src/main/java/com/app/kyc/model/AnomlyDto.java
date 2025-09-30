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
    List<ConsumerDto> consumers = new ArrayList<ConsumerDto>();

    public AnomlyDto(Long id, String note, AnomalyStatus status, AnomalyType anomalyType, List<Consumer> consumers, String updateBy, Date updatedOn, Date reportedOn) {
        this.id = id;
        this.note = note;
        this.status = status;
        this.anomalyType = anomalyType;
        this.updateBy = updateBy;
        this.updatedOn = updatedOn;
        this.reportedOn = reportedOn;
        if (consumers != null && consumers.size() > 0) {

            this.consumers = consumers.stream()
                    .map(c -> new ConsumerDto(c, null))
                    .collect(Collectors.toList());
        }

    }

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
        this.formattedId = anomaly.getConsumers().get(0).getServiceProvider().getName() + "-" + new SimpleDateFormat("ddMMyyyy").format(anomaly.getReportedOn()) + "-" + this.id;
        if (consumers != null && consumers.size() > 0) {

            this.consumers = consumers.stream()
                    .filter(c -> c.getConsumerStatus() == 0)
                    .map(c -> new ConsumerDto(c, null))
                    .collect(Collectors.toList());
        }
    }

    //TODO Remove Extra Constructor
    public AnomlyDto(Anomaly anomaly, int temp) {
        this.id = anomaly.getId();
        this.note = anomaly.getNote();
        this.status = anomaly.getStatus();
        this.anomalyType = anomaly.getAnomalyType();
        this.reportedOn = anomaly.getReportedOn();
        this.reportedBy = anomaly.getReportedBy();
        this.updatedOn = anomaly.getUpdatedOn();
        this.updateBy = anomaly.getUpdateBy();
        this.formattedId = anomaly.getConsumers().get(0).getServiceProvider().getName() + "/" + new SimpleDateFormat("dd-MM-yyyy").format(anomaly.getReportedOn()) + "/" + this.id;
        List<Consumer> consumers = anomaly.getConsumers();
        if (consumers != null && consumers.size() > 0) {

            this.consumers = consumers.stream()
                    .map(c -> new ConsumerDto(c, null))
                    .collect(Collectors.toList());
        }
    }

    public AnomlyDto() {
    }

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
