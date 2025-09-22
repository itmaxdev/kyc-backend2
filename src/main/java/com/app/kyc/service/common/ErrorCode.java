package com.app.kyc.service.common;

public enum ErrorCode {

   BAD_REQUEST(601, "Bad Request"), EMAIL_NOT_FOUND(651, "This email does not exist. Please try again"), PASSWORD_INCORRECT(652,
      "The password is incorrect, please retry again"), USER_INACTIVE(653, "Inactive user"), INVALID_CODE(654, "Invalid code"), EMAIL_TAKEN(662, "This email is already taken"),

   ALREADY_EXIST(655, "Object Already Exist"), NOT_FOUND(656, "Object Not Found"), INVALID_CHANGE_REQUEST(657, "Invalid Change Request"), INVALID_ADD_REQUEST(660,
      "Invalid Add Request"),

   REASON_IS_REQUIRED(658, "Rejection reason is required"), SERVICE_ALREADY_PROCESSED(659, "Service is already processed"),

   ANOMALIES_ALREADY_REPORTED(661, "Anomalies Already Reported"), SERVICE_PROVIDER_INACTIVE(662, "Inactive service provider"),
	
   OTP_INVALID(663, "Invalid OTP"),
   OTP_EXPIRED(664, "OTP Expired"),
   OTP_NOT_FOUND(665, "OTP not found"),
   OTP_CHANNEL_UNSUPPORTED(666, "Unsupported OTP channel");

   private final Integer code;
   private final String message;

   ErrorCode(Integer code, String message)
   {
      this.code = code;
      this.message = message;
   }

   public Integer getCode()
   {
      return code;
   }

   public String getMessage()
   {
      return message;
   }
}
