package com.app.kyc.entity;

import java.util.Date;

import javax.persistence.*;

import com.app.kyc.model.AnomalyStatus;

@Entity
@Table(name = "anomaly_tracking")
public class AnomalyTracking
{
   @Id
   @GeneratedValue(strategy = GenerationType.IDENTITY)
   private Long id;

   @ManyToOne(cascade = {CascadeType.ALL})
   private Anomaly anomaly;

   private Date createdOn;

   private AnomalyStatus status;

   private String note;

   private String updateBy;
   private Date updateOn;




   public AnomalyTracking()
   {
   }

   public AnomalyTracking(Anomaly anomaly, Date createdOn, AnomalyStatus status, String note,String updateBy, Date updateOn, String consistentOn)
   {
      this.anomaly = anomaly;
      this.createdOn = createdOn;
      this.status = status;
      this.note = note;
      this.updateBy = updateBy;
      this.updateOn = updateOn;

   }

   public Long getId()
   {
      return id;
   }

   public void setId(Long id)
   {
      this.id = id;
   }

   public Anomaly getAnomaly()
   {
      return anomaly;
   }

   public void setAnomaly(Anomaly anomaly)
   {
      this.anomaly = anomaly;
   }

   public Date getCreatedOn()
   {
      return createdOn;
   }

   public void setCreatedOn(Date createdOn)
   {
      this.createdOn = createdOn;
   }

   public AnomalyStatus getStatus()
   {
      return status;
   }

   public void setStatus(AnomalyStatus status)
   {
      this.status = status;
   }

   public String getNote()
   {
      return note;
   }

   public void setNote(String note)
   {
      this.note = note;
   }

   public String getUpdateBy() {
      return updateBy;
   }

   public void setUpdateBy(String updatedBy) {
      this.updateBy = updatedBy;
   }

   public Date getUpdateOn() {
      return updateOn;
   }

   public void setUpdatedOn(Date updatedOn) {
      this.updateOn = updatedOn;
   }

   }
