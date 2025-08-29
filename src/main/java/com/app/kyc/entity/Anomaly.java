package com.app.kyc.entity;

import java.util.*;

import javax.persistence.*;

import com.app.kyc.model.AnomalyStatus;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "anomalies")
public class Anomaly
{
   @Id
   @GeneratedValue(strategy = GenerationType.IDENTITY)
   private Long id;

   @Setter
   @Getter
   private String formattedId;
   
   @ManyToOne
   private User reportedBy;
   
   private Date reportedOn;
   
   private Date updatedOn;

   private String updateBy;
   
   private String note;
   
   private AnomalyStatus status;
   
   
   @ManyToOne
   @JoinColumn(name = "consumers_services_id")
   ConsumerService consumersService;
   
   @ManyToMany(cascade = CascadeType.ALL)
   @JoinTable(name = "consumers_anomalies", joinColumns = @JoinColumn(name = "anomaly_id"), inverseJoinColumns = @JoinColumn(name = "consumer_id"))
   List<Consumer> consumers = new ArrayList<>();

   @ManyToOne
   private AnomalyType anomalyType;
   
   public Long getId()
   {
      return id;
   }
   
   public void setId(Long id)
   {
      this.id = id;
   }
   
   public User getReportedBy()
   {
      return reportedBy;
   }
   
   public void setReportedBy(User reportedBy)
   {
      this.reportedBy = reportedBy;
   }
   
   public Date getReportedOn()
   {
      return reportedOn;
   }
   
   public void setReportedOn(Date reportedOn)
   {
      this.reportedOn = reportedOn;
   }
   
   public AnomalyType getAnomalyType()
   {
      return anomalyType;
   }
   
   public void setAnomalyType(AnomalyType anomalyType)
   {
      this.anomalyType = anomalyType;
   }
   
   public String getNote()
   {
      return note;
   }
   
   public void setNote(String note)
   {
      this.note = note;
   }
   
   public List<Consumer> getConsumers()
   {
      return consumers;
   }
   
   public void addConsumer(Consumer consumer)
   {
      if(!Objects.isNull(consumer)){
         consumers.add(consumer);
      }
    //consumer.getAnomalies().add(this);
   }
   
   public ConsumerService getConsumersServices()
   {
      return consumersService;
   }
   
   public void setConsumersServices(ConsumerService consumersService)
   {
      this.consumersService = consumersService;
   }
   
   public AnomalyStatus getStatus()
   {
      return status;
   }
   
   public void setStatus(AnomalyStatus status)
   {
      this.status = status;
   }
   
   public Date getUpdatedOn()
   {
      return updatedOn;
   }
   
   public void setUpdatedOn(Date updatedOn)
   {
      this.updatedOn = updatedOn;
   }

   public String getUpdateBy() {
      return updateBy;
   }

   public void setUpdateBy(String updatedBy) {
      this.updateBy = updatedBy;
   }
   
}
