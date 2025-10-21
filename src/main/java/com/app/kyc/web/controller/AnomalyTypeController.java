package com.app.kyc.web.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.app.kyc.model.AnomalyTypeDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
         roles.add("KYC Admin");
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
         roles.add("KYC Admin");
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
         roles.add("KYC Admin");
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
         roles.add("KYC Admin");
         if(securityHelper.hasRole(request, roles))
         {
            final String authorizationHeader = request.getHeader("Authorization");
            String userName = null;
            if(authorizationHeader != null && authorizationHeader.startsWith(("Bearer ")))
            {
               userName = securityHelper.getUserName(authorizationHeader.substring(7));
            }
            AnomalyType anomalyType1 = anomalyTypeService.updateAnomalyType(userName, anomalyType);
            return ResponseEntity.ok(anomalyType1);
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


   @DeleteMapping("/{id}/soft-delete")
   public ResponseEntity<String> softDelete(HttpServletRequest request,@PathVariable Long id) {
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         if(securityHelper.hasRole(request, roles)) {
            anomalyTypeService.softDeleteAnomalyType(id);
            return ResponseEntity.ok("AnomalyType " + id + " marked as deleted");
         } else
            return ResponseEntity.ok("Not authorized");
         }
      catch(Exception e)
         {
            //log.info(e.getMessage());
            return ResponseEntity.ok(e.getMessage());
         }
   }

   // ✅ Fetch only active anomaly types
   @GetMapping
   public ResponseEntity<List<AnomalyType>> getAllActive() {
      return ResponseEntity.ok(anomalyTypeService.getAllActiveAnomalyTypes());
   }


   @PostMapping("/createAnomalyType")
   public ResponseEntity<?> createAnomalyType(@RequestBody AnomalyType anomalyType,
                                              HttpServletRequest request) {
      try {
         List<String> roles = new ArrayList<>();
         roles.add("KYC Admin");

         if (securityHelper.hasRole(request, roles)) {
            AnomalyType saved = anomalyTypeService.createAnomalyType(anomalyType);
            return new ResponseEntity<>(saved, HttpStatus.CREATED);
         } else {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body("Access denied: You do not have permission to create anomaly types.");
         }

      } catch (RuntimeException ex) {
         return ResponseEntity.badRequest().body(ex.getMessage());
      } catch (Exception e) {
         // log.error("Unexpected error while creating anomaly type", e);
         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                 .body("An unexpected error occurred: " + e.getMessage());
      }
   }

}
