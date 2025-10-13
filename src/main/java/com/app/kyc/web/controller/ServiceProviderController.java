package com.app.kyc.web.controller;

import java.sql.SQLException;
import java.util.*;

import javax.servlet.http.HttpServletRequest;

import com.app.kyc.model.ServiceProviderPutRequest;
import com.app.kyc.repository.ServiceProviderRepository;
import com.app.kyc.repository.ServiceRepository;
import com.app.kyc.service.AnomalyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.entity.User;
import com.app.kyc.service.ServiceProviderService;
import com.app.kyc.service.UserService;
import com.app.kyc.web.security.SecurityHelper;

@RestController
@RequestMapping("/serviceProviders")
public class ServiceProviderController {

   @Autowired
   ServiceProviderService serviceProviderService;

   @Autowired
   SecurityHelper securityHelper;

   @Autowired
   UserService userService;

   @Autowired
   AnomalyService anomalyService;
   @Autowired
   ServiceProviderRepository serviceProviderRepository;

   @GetMapping("/{id}")
   public ResponseEntity<?> getServiceProviderById(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException {
      //log.info("ServiceProviderController/getServiceProviderById");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         if (securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(serviceProviderService.getServiceProviderById(id));
         else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("allInfo/{id}")
   public ResponseEntity<?> getServiceProviderAllInfoById(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException {
      //log.info("ServiceProviderController/getServiceProviderById");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         if (securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(serviceProviderService.getServiceProviderAllInfoById(id));
         else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getByName/{name}")
   public ResponseEntity<?> getServiceProviderByName(HttpServletRequest request, @PathVariable("name") String name) throws SQLException {
      //log.info("ServiceProviderController/getServiceProviderByName");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         if (securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(serviceProviderService.getServiceProviderByName(name));
         else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   /*@GetMapping("/getAll/")
   public ResponseEntity<?> getAllServiceProviders(HttpServletRequest request, @RequestParam("params") String params) throws SQLException
   {
      //log.info("ServiceProviderController/getAllServiceProviders");


      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> serviceProviders = serviceProviderService.getAllServiceProviders(params);
            return ResponseEntity.ok(serviceProviders);
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
*/

   @GetMapping("/getAll/")
   public ResponseEntity<?> getAllServiceProviders(HttpServletRequest request,
                                                   @RequestParam("params") String params) throws SQLException {
      try {
         List<String> roles = new ArrayList<>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if (securityHelper.hasRole(request, roles)) {
            Map<String, Object> serviceProviders = serviceProviderService.getAllServiceProviders(params);
            return ResponseEntity.ok(serviceProviders);
         } else {
            return ResponseEntity.ok("Not authorized");
         }
      } catch (Exception e) {
         return ResponseEntity.ok(e.getMessage());
      }
   }


   @GetMapping("/getByUserIndustry/{userId}")
   public ResponseEntity<?> getServiceProvidersByUserIndustry(HttpServletRequest request, @PathVariable("userId") Long userId) throws SQLException {
      //log.info("ServiceProviderController/getAllServiceProviders");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if (securityHelper.hasRole(request, roles)) {
            Map<String, Object> serviceProviders = serviceProviderService.getAllServiceProvidersByIndustry(userId);
            return ResponseEntity.ok(serviceProviders);
         } else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PostMapping("/add")
   public ResponseEntity<?> addServiceProvider(HttpServletRequest request, @RequestBody ServiceProvider serviceProvider) throws SQLException {
      //log.info("ServiceProviderController/addServiceProvider");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("KYC Admin");
         if (securityHelper.hasRole(request, roles)) {
            if (serviceProviderService.getServiceProviderByName(serviceProvider.getName()) != null)
               return ResponseEntity.ok("Name already in use");
            final String authorizationHeader = request.getHeader("Authorization");
            String userName = null;
            if (authorizationHeader != null && authorizationHeader.startsWith(("Bearer "))) {
               userName = securityHelper.getUserName(authorizationHeader.substring(7));
            }
            if (userName == null) return ResponseEntity.ok("Invalid user");
            serviceProvider.setCreatedBy(userService.getUserByEmail(userName).getId());
            serviceProviderService.addServiceProvider(serviceProvider);
            return ResponseEntity.ok(serviceProvider);
         } else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PutMapping("/update")
   public ResponseEntity<?> updateServiceProvider(HttpServletRequest request, @RequestBody ServiceProvider serviceProvider) throws SQLException {
      //log.info("ServiceProviderController/updateServiceProvider");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("KYC Admin");
         if (securityHelper.hasRole(request, roles)) {
            serviceProviderService.updateServiceProvider(serviceProvider);
            return ResponseEntity.ok("ServiceProvider updated successfully");
         } else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @DeleteMapping("/deleteServiceProvider/{id}")
   public ResponseEntity<?> deleteServiceProvider(HttpServletRequest request, @PathVariable("id") long id) throws SQLException {
      //log.info("ServiceProviderController/changePassword");
      try {
         ServiceProvider serviceProvider = serviceProviderService.getServiceProviderById(id);
         if (serviceProvider.getId() == null) return ResponseEntity.ok("ServiceProvider not found");
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         if (securityHelper.hasRole(request, roles)) {
            serviceProviderService.deleteServiceProvider(id);
            return ResponseEntity.ok("ServiceProvider Deleted");
         } else
            return ResponseEntity.ok("Not authorized");

      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PatchMapping("/activate/{id}")
   public ResponseEntity<?> activateServiceProvider(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException {
      //log.info("ServiceController/activateService");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         if (securityHelper.hasRole(request, roles)) {
            String authTokenHeader = request.getHeader("Authorization");
            String jwt = "";
            if (authTokenHeader != null && authTokenHeader.startsWith(("Bearer "))) {
               jwt = authTokenHeader.substring(7);
            }
            User user = userService.getUserByEmail(securityHelper.getUserName(jwt));
            if (id == null || id <= 0 || id.toString() == "") return ResponseEntity.ok("id is Required");
            if (user.getId() == 0) return ResponseEntity.ok("Invalid user");
            serviceProviderService.activateService(id, user.getId());
            return ResponseEntity.ok("Service activated successfully");
         } else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PatchMapping("/deactivate/{id}")
   public ResponseEntity<?> deactivateServiceProvider(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException {
      //log.info("ServiceController/deactivateService");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         if (securityHelper.hasRole(request, roles)) {
            String authTokenHeader = request.getHeader("Authorization");
            String jwt = "";
            if (authTokenHeader != null && authTokenHeader.startsWith(("Bearer "))) {
               jwt = authTokenHeader.substring(7);
            }
            User user = userService.getUserByEmail(securityHelper.getUserName(jwt));
            if (id == null || id <= 0 || id.toString() == "") return ResponseEntity.ok("id is Required");
            if (user.getId() == 0) return ResponseEntity.ok("Invalid user");
            serviceProviderService.deactivateService(id);
            return ResponseEntity.ok("Service deactivated successfully");
         } else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }


   @PostMapping("/deleteData/{serviceProviderId}")
   public ResponseEntity<?> loadConsumers(HttpServletRequest request, @PathVariable("serviceProviderId") Long serviceProviderId) throws SQLException {
      //log.info("UserController/addUser");
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if (securityHelper.hasRole(request, roles)) {
            final String authorizationHeader = request.getHeader("Authorization");
            String userName = null;
            if (authorizationHeader != null && authorizationHeader.startsWith(("Bearer "))) {
               userName = securityHelper.getUserName(authorizationHeader.substring(7));
            }
            serviceProviderService.deleteData(serviceProviderId);
            return ResponseEntity.ok("all data data deleted");
         } else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }


   @DeleteMapping("/{id}/soft-delete")
   public ResponseEntity<?> softDeleteServiceProvider(HttpServletRequest request,@PathVariable Long id) {
      try {
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         if (securityHelper.hasRole(request, roles)) {

            ServiceProvider sp = serviceProviderRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("Service provider not found"));

            sp.setDeleted(true);
            serviceProviderRepository.save(sp);

            return ResponseEntity.ok("Service provider soft deleted successfully");
         } else
            return ResponseEntity.ok("Not authorized");
      } catch (Exception e) {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/{spId}/anomalies")
   public ResponseEntity<?> getAnomaliesByServiceProvider(HttpServletRequest request, @PathVariable Long spId) {
      try {
         List<String> allowedRoles = Arrays.asList("Compliance Admin");

         if (securityHelper.hasRole(request, allowedRoles)) {
            Map<String, Object> response = anomalyService.getAnomaliesByServiceProvider(spId);
            return ResponseEntity.ok(response);
         } else {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Collections.singletonMap("message", "Not authorized"));
         }

      } catch (Exception e) {
         // log.error("Error fetching anomalies for service provider {}: {}", spId, e.getMessage());
         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                 .body(Collections.singletonMap("error", "Failed to fetch anomalies: " + e.getMessage()));
      }
   }


   @PostMapping("/createServiceProvider")
   public ResponseEntity<?> createServiceProvider(@RequestBody ServiceProvider sp,
                                                  HttpServletRequest request) {
      try {
         List<String> roles = new ArrayList<>();
         roles.add("KYC Admin");

         if (securityHelper.hasRole(request, roles)) {
            ServiceProvider saved = serviceProviderService.createServiceProvider(sp);
            return new ResponseEntity<>(saved, HttpStatus.CREATED);
         } else {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body("Access denied: You do not have permission to create Service Providers.");
         }

      } catch (RuntimeException ex) {
         return ResponseEntity.badRequest().body(ex.getMessage());
      } catch (Exception e) {
         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                 .body("Unexpected error: " + e.getMessage());
      }
   }


   @PutMapping("/{id}")
   public ResponseEntity<?> update(
           @PathVariable Long id,
           @Validated @RequestBody ServiceProviderPutRequest req) {
      return ResponseEntity.ok(serviceProviderService.update(id, req));
   }
}