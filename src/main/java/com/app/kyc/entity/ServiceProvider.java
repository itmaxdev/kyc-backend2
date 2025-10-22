package com.app.kyc.entity;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.app.kyc.model.ServiceProviderStatus;

@Entity
@Table(name = "service_providers")
public class ServiceProvider
{

   @Id
   @GeneratedValue(strategy = GenerationType.IDENTITY)
   private Long id;
   private String name;
   private String address;
   private String companyPhoneNumber;
   private boolean Deleted;


   @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
   private Date createdOn;

   private Long createdBy;
   private ServiceProviderStatus status;
   private Long approvedBy;
   private String color;

   @ManyToOne
   private Industry industry;

   public Long getId()
   {
      return id;
   }

   public void setId(Long id)
   {
      this.id = id;
   }

   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public boolean isDeleted()
   {
      return Deleted;
   }

   public void setDeleted(boolean deleted)
   {
      Deleted = deleted;
   }

   public Date getCreatedOn()
   {
      return createdOn;
   }

   public void setCreatedOn(Date createdOn)
   {
      this.createdOn = createdOn;
   }

   public Industry getIndustry()
   {
      return industry;
   }

   public void setIndustry(Industry industry)
   {
      this.industry = industry;
   }

   public Long getCreatedBy()
   {
      return createdBy;
   }

   public void setCreatedBy(Long createdBy)
   {
      this.createdBy = createdBy;
   }

   public String getAddress()
   {
      return address;
   }

   public void setAddress(String address)
   {
      this.address = address;
   }

   public String getCompanyPhoneNumber()
   {
      return companyPhoneNumber;
   }

   public void setCompanyPhoneNumber(String companyPhoneNumber)
   {
      this.companyPhoneNumber = companyPhoneNumber;
   }

   public ServiceProviderStatus getStatus()
   {
      return status;
   }

   public void setStatus(ServiceProviderStatus status)
   {
      this.status = status;
   }

   public Long getApprovedBy()
   {
      return approvedBy;
   }

   public void setApprovedBy(Long approvedBy)
   {
      this.approvedBy = approvedBy;
   }

   public String getColor() { return color; }

   public void setColor(String color) { this.color = color; }

   @Override
   public String toString()
   {
      return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
   }

}
