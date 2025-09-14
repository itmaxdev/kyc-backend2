package com.app.kyc.model;

import com.app.kyc.entity.Anomaly;

import java.util.Date;

public class AnomalyTrackingDto {
    private Long id;
    private AnomlyDto anomlyDto;
    private Date createdOn;

    private AnomalyStatus status;

    private String note;

    private String updatedBy;

    private Date updatedOn;

    private String consistentOn;


    public AnomalyTrackingDto(Long id, Date createdOn, AnomalyStatus anomalyStatus, String note, Anomaly anomaly, String updatedBy, Date updatedOn,String consistentOn){
        this.id = id;
        this.createdOn = createdOn;
        this.status= anomalyStatus;
        this.note = note;
        this.updatedBy = updatedBy;
        this.updatedOn = updatedOn;
        this.consistentOn=consistentOn;

        this.anomlyDto = new AnomlyDto(anomaly,0);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public AnomlyDto getAnomlyDto() {
        return anomlyDto;
    }

    public void setAnomlyDto(AnomlyDto anomlyDto) {
        this.anomlyDto = anomlyDto;
    }

    public Date getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
    }

    public AnomalyStatus getStatus() {
        return status;
    }

    public void setStatus(AnomalyStatus status) {
        this.status = status;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }
    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Date getUpdatedOn() {
        return updatedOn;
    }

    public void setUpdatedOn(Date updatedOn) {
        this.updatedOn = updatedOn;
    }

    public String getConsistentOn() {
        return consistentOn;
    }

    public void setConsistentOn(String consistentOn) {
        this.consistentOn = consistentOn;
    }
}
