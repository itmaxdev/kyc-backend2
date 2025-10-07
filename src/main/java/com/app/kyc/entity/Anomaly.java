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

   @Column(name = "anomaly_formatted_id", unique = true, length = 80)
   private String anomalyFormattedId;
   
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

   public String getFormattedId() {
      return formattedId;
   }

   public void setFormattedId(String formattedId) {
      this.formattedId = formattedId;
   }

   public String getAnomalyFormattedId() {
      return anomalyFormattedId;
   }

   public void setAnomalyFormattedId(String anomalyFormattedId) {
      this.anomalyFormattedId = anomalyFormattedId;
   }



   @PrePersist
   private void prePersistDefaults() {
      if (reportedOn == null) {
         reportedOn = new Date();
      }
   }

   /**
    * Runs after INSERT so the IDENTITY primary key is available.
    * Builds: {vendor}-{ddMMyyyy}-{zeroPaddedId}
    * e.g. vodacom-07102025-000123
    */
   @PostPersist
   private void assignFormattedId() {
      if (this.anomalyFormattedId != null && !this.anomalyFormattedId.isBlank()) return;

      String vendor = resolveVendorSlug(consumersService);    // "vodacom" (slug)
      String day = new java.text.SimpleDateFormat("ddMMyyyy").format(
              reportedOn != null ? reportedOn : new Date()
      );

      // use DB primary key to guarantee uniqueness system-wide
      String padded = String.format("%06d", this.id); // adjust width if you prefer 2, 4, 8, etc.

      this.anomalyFormattedId = vendor + "-" + day + "-" + padded;
      // Still within the same persistence context; Hibernate will UPDATE before commit.
   }


   private String resolveVendorSlug(ConsumerService cs) {
      // Try to derive the vendor from your model:
      // Option A (common in your schema): Consumer -> ServiceProvider -> name
      try {
         if (cs != null &&
                 cs.getConsumer() != null &&
                 cs.getConsumer().getServiceProvider() != null &&
                 cs.getConsumer().getServiceProvider().getName() != null) {
            return slugify(cs.getConsumer().getServiceProvider().getName());
         }
      } catch (Exception ignore) {}

      // Option B (if Service -> ServiceProvider -> name is where vendor lives):
      try {
         if (cs != null &&
                 cs.getService() != null &&
                 cs.getService().getServiceProvider() != null &&
                 cs.getService().getServiceProvider().getName() != null) {
            return slugify(cs.getService().getServiceProvider().getName());
         }
      } catch (Exception ignore) {}

      // Fallback
      return "vodacom";
   }

   private String slugify(String raw) {
      String s = raw == null ? "vodacom" : raw.trim().toLowerCase(Locale.ROOT);
      s = s.replaceAll("[^a-z0-9]+", "-");
      return s.replaceAll("^-+|-+$", "");
   }
}
