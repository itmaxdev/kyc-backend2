package com.app.kyc.model;

public enum UserStatus {

   Inactive(0, "Inactive"), Active(1, "Active");

   private Integer code;
   private String status;

   UserStatus(Integer code, String status)
   {
      this.code = code;
      this.status = status;
   }

   public Integer getCode()
   {
      return code;
   }

   public void setCode(Integer code)
   {
      this.code = code;
   }

   public String getStatus()
   {
      return status;
   }

   public void setStatus(String status)
   {
      this.status = status;
   }

}
