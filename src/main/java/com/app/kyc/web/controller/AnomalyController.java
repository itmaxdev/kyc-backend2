package com.app.kyc.web.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.app.kyc.Masking.MaskingContext;
import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.User;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.request.UpdateAnomalyStatusRequest;
import com.app.kyc.service.AnomalyService;
import com.app.kyc.service.UserService;
import com.app.kyc.web.security.SecurityHelper;

@RestController
@RequestMapping("/anomalies")
public class AnomalyController
{

   @Autowired
   AnomalyService anomalyService;

   @Autowired
   UserService userService;

   @Autowired
   SecurityHelper securityHelper;

   @GetMapping("/{id}")
   public ResponseEntity<?> getAnomalyById(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("AnomalyController/getAnomalyById");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(anomalyService.getAnomalyById(id));
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/anomalyDetails/{id}")
   public ResponseEntity<?> getAnomalyByIdWithDetails(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("AnomalyController/getAnomalyById");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles)) {
			 final String authorizationHeader = request.getHeader("Authorization");
			 String userName = null;
			 if(authorizationHeader != null && authorizationHeader.startsWith(("Bearer ")))
			 {
			    userName = securityHelper.getUserName(authorizationHeader.substring(7));
			 }
			 User user = userService.getUserByEmail(userName);
			 boolean ismask = userService.isUnmasked(user);
			 MaskingContext.setMasking(ismask);
			 
			 return ResponseEntity.ok(anomalyService.getAnomalyByIdWithDetails(id));
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getAll")
   public ResponseEntity<?> getAllAnomalies(HttpServletRequest request, @RequestParam(value = "params", required = false) String params) throws SQLException
   {
      //log.info("AnomalyController/getAllAnomalies");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> anomalies = anomalyService.getAllAnomalies(params);
            return ResponseEntity.ok(anomalies);
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PostMapping("/add")
   public ResponseEntity<?> addAnomaly(HttpServletRequest request, @RequestBody Anomaly anomaly) throws SQLException
   {
      //log.info("AnomalyController/addAnomaly");
      List<String> roles = new ArrayList<String>();
      roles.add("Compliance Admin");
      roles.add("SP Admin");
      roles.add("KYC Admin");
      roles.add("SP User");
      if(securityHelper.hasRole(request, roles))
      {
         final String authorizationHeader = request.getHeader("Authorization");
         String userName = null;
         if(authorizationHeader != null && authorizationHeader.startsWith(("Bearer ")))
         {
            userName = securityHelper.getUserName(authorizationHeader.substring(7));
         }
         anomaly.setReportedBy(userService.getUserByEmail(userName));
         anomalyService.addAnomaly(anomaly);
         return ResponseEntity.ok(anomaly);
      }
      else
         return ResponseEntity.ok("Not authorized");
   }

   @PutMapping("/update")
   public ResponseEntity<?> updateAnomaly(HttpServletRequest request, @RequestBody Anomaly anomaly) throws SQLException
   {
      //log.info("AnomalyController/updateAnomaly");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            Long id = anomaly.getId();
            if(id == null || id <= 0 || id.toString() == "") return ResponseEntity.ok("id is Required");
            if((anomalyService.getAnomalyById(id).getId() == null)) return ResponseEntity.ok("Service Type not found");
            anomalyService.updateAnomaly(anomaly);
            return ResponseEntity.ok("Anomaly updated successfully");
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PutMapping("/updateAnomalyStatus")
   public ResponseEntity<?> updateAnomalyStatus(HttpServletRequest request, @RequestBody UpdateAnomalyStatusRequest updateAnomalyStatusRequest) throws SQLException
   {
      //log.info("AnomalyController/updateAnomaly");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            final String authorizationHeader = request.getHeader("Authorization");
            String userName = null;
            if(authorizationHeader != null && authorizationHeader.startsWith(("Bearer ")))
            {
               userName = securityHelper.getUserName(authorizationHeader.substring(7));
            }
            anomalyService.updateAnomaly(updateAnomalyStatusRequest, userService.getUserByEmail(userName));
            return ResponseEntity.ok("Anomaly updated successfully");
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getAllByServiceProvider/{serviceProviderId}")
   public ResponseEntity<?> getAllAnomaliesByServiceProvider(HttpServletRequest request, @RequestParam(value = "params", required = false) String params, @PathVariable("serviceProviderId") Long serviceProviderId) throws SQLException
   {
      //log.info("AnomalyController/getAllAnomalies");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> anomalies = anomalyService.getAllAnomaliesByServiceProvider(serviceProviderId,params);
            return ResponseEntity.ok(anomalies);
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getAllByServiceProvider-v2")
   public ResponseEntity<?> getAllAnomaliesByServiceProviderAccordingToUser(HttpServletRequest request, @RequestParam(value = "params", required = false) String params) throws SQLException
   {
      //log.info("AnomalyController/getAllAnomalies");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            final String authorizationHeader = request.getHeader("Authorization");
            String userName = null;
            if(authorizationHeader != null && authorizationHeader.startsWith(("Bearer ")))
            {
               userName = securityHelper.getUserName(authorizationHeader.substring(7));
            }
            Map<String, Object> anomalies = anomalyService.getAllAnomaliesByServiceProvider(userService.getUserByEmail(userName).getServiceProvider().getId(),params);
            return ResponseEntity.ok(anomalies);
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getAnomalyStatus")
   public ResponseEntity<?> getAllAnomalyStatus(HttpServletRequest request) throws SQLException
   {
      //log.info("getAllAnomalyStatus");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("SP Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
        	 List<Map<String, Object>> statusList = AnomalyStatus.asList();
            return ResponseEntity.ok(statusList);
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }
}
