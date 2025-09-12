package com.app.kyc.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public enum AnomalyStatus {

   REPORTED(0, "Reported"), UNDER_INVESTIGATION(1, "In Progress"), QUESTION_SUBMITTED(2, "Question Submitted"), QUESTION_ANSWERED(3,
      "Question Answered"), RESOLUTION_SUBMITTED(4, "Resolution Submitted"), RESOLVED_PARTIALLY(5, "Resolved Partially"),RESOLVED_SUCCESSFULLY(6, "Resolved Fully"), WITHDRAWN(7, "Withdrawn");

   private Integer code;
   private String status;

   AnomalyStatus(Integer code, String status)
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

   // ✅ Convert to list of maps
   public static List<Map<String, Object>> asList() {
       return Arrays.stream(values())
               .map(s -> {
                   Map<String, Object> map = new HashMap<>();
                   map.put("code", s.getCode());
                   map.put("status", s.getStatus());
                   return map;
               })
               .collect(Collectors.toList());
   }	
}
