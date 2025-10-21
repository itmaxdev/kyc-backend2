package com.app.kyc.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AnomalySeverity {

   MINOR(1, "Minor"),
   WARNING(2, "Warning"),
   SERIOUS(3, "Serious"),
   CRITICAL(4, "Critical"),
   SEVERE(5, "Severe"),
   LOW(6, "Low"),
   MEDIUM(7, "Medium"),
   HIGH(8, "High");

   private final Integer code;
   private final String severity;

   AnomalySeverity(Integer code, String severity) {
      this.code = code;
      this.severity = severity;
   }

   public Integer getCode() {
      return code;
   }

   @JsonValue
   public String getSeverity() {
      return severity;
   }

   @JsonCreator
   public static AnomalySeverity fromValue(String value) {
      for (AnomalySeverity s : values()) {
         if (s.name().equalsIgnoreCase(value) || s.getSeverity().equalsIgnoreCase(value)) {
            return s;
         }
      }
      throw new IllegalArgumentException("Invalid severity value: " + value);
   }
}
