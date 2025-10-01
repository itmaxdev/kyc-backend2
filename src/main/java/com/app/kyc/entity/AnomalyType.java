package com.app.kyc.entity;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.app.kyc.model.AnomalyEntityType;
import com.app.kyc.model.AnomalySeverity;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "anomaly_types")
@Data
@Getter
@Setter
public class AnomalyType {
   @Id
   @GeneratedValue(strategy = GenerationType.IDENTITY)
   private Long id;

   private String name;
   private Long entity_id;
   private AnomalySeverity severity;
   private AnomalyEntityType targetEntityType;
   private Long createdBy;
   private Date createdOn;
   private String description;

   // ✅ Soft delete flag
   private Boolean deleted = false;
}

