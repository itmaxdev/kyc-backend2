package com.app.kyc.web.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.app.kyc.model.AnomalyTypeDto;
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

import com.app.kyc.entity.AnomalyType;
import com.app.kyc.service.AnomalyTypeService;
import com.app.kyc.web.security.SecurityHelper;

@RestController
@RequestMapping("/anomalyType")
public class AnomalyTypeController
{

   @Autowired
   AnomalyTypeService anomalyTypeService;

   @Autowired
   SecurityHelper securityHelper;

   @GetMapping("/{id}")
   public ResponseEntity<?> getAnomalyTypeById(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("AnomalyTypeController/getAnomalyTypeById");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         if(securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(anomalyTypeService.getAnomalyTypeById(id));
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getAnomalyTypes")
   public ResponseEntity<?> getAnomalyTypes(HttpServletRequest request) {

      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if(securityHelper.hasRole(request, roles))
         {
            List<AnomalyTypeDto> anomalyTypes = anomalyTypeService.getAnomalyTypes();
            return ResponseEntity.ok(anomalyTypes);
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getAll")
   public ResponseEntity<?> getAllAnomalyTypes(HttpServletRequest request, @RequestParam(value = "params", required = false) String params) throws SQLException
   {
      //log.info("AnomalyTypeController/getAllAnomalyTypes");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> anomalyTypes = anomalyTypeService.getAllAnomalyTypes(params);
            return ResponseEntity.ok(anomalyTypes);
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
   public ResponseEntity<?> addAnomalyType(HttpServletRequest request, @RequestBody AnomalyType anomalyType) throws SQLException
   {
      //log.info("AnomalyTypeController/addAnomalyType");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         if(securityHelper.hasRole(request, roles))
         {
            final String authorizationHeader = request.getHeader("Authorization");
            String userName = null;
            if(authorizationHeader != null && authorizationHeader.startsWith(("Bearer ")))
            {
               userName = securityHelper.getUserName(authorizationHeader.substring(7));
            }
            anomalyTypeService.addAnomalyType(userName, anomalyType);
            return ResponseEntity.ok(anomalyType);
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

   @PutMapping("/update")
   public ResponseEntity<?> updateAnomalyType(HttpServletRequest request, @RequestBody AnomalyType anomalyType) throws SQLException
   {
      //log.info("AnomalyTypeController/updateAnomalyType");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         if(securityHelper.hasRole(request, roles))
         {
            final String authorizationHeader = request.getHeader("Authorization");
            String userName = null;
            if(authorizationHeader != null && authorizationHeader.startsWith(("Bearer ")))
            {
               userName = securityHelper.getUserName(authorizationHeader.substring(7));
            }
            anomalyTypeService.updateAnomalyType(userName, anomalyType);
            return ResponseEntity.ok("AnomalyType updated successfully");
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
