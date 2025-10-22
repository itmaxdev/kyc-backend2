package com.app.kyc.response;

import java.util.Date;

import com.app.kyc.entity.User;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class ServiceProviderUserResponseDTO
{
   Long Id;

   String serviceProviderName;

   String createdOn;

   String industryName;

   User createdBy;

   boolean hasServices;

   String color;

   public ServiceProviderUserResponseDTO(Long id, String serviceProviderName, String createdOn, String industryName, User createdBy, boolean hasServices, String color)
   {
      this.Id = id;
      this.serviceProviderName = serviceProviderName;
      this.createdOn = createdOn;
      this.industryName = industryName;
      this.createdBy = createdBy;
      this.hasServices = hasServices;
      this.color = color;
   }



}
