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

import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ConsumerService;
import com.app.kyc.entity.User;
import com.app.kyc.service.ConsumerServiceService;
import com.app.kyc.service.UserService;
import com.app.kyc.web.security.SecurityHelper;


import io.swagger.annotations.Api;

@RestController
@RequestMapping(value = "consumers")
@Api(value = "consumers", tags = {"consumers" })
public class ConsumerController
{

   //   private static final Logger Log = LoggerFactory.getLogger(ConsumerController.class);

   @Autowired
   com.app.kyc.service.ConsumerService consumerService;

   @Autowired
   ConsumerServiceService consumerServiceService;

   @Autowired
   UserService userService;

   @Autowired
   SecurityHelper securityHelper;

   @GetMapping("/{id}")
   public ResponseEntity<?> getConsumerById(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("ConsumerController/getConsumerById");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(consumerService.getConsumerById(id));
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/withSubscriptions/{id}")
   public ResponseEntity<?> getConsumerByIdwithSubscriptions(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("ConsumerController/getConsumerById");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(consumerService.getConsumerByIdwithSubscriptions(id));
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
   public ResponseEntity<?> getAllConsumers(HttpServletRequest request, @RequestParam(value = "params", required = false) String params) throws SQLException
   {
      //log.info("ConsumerController/getAllConsumers");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> consumers = consumerService.getAllConsumers(params);
            return ResponseEntity.ok(consumers);
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

   @GetMapping("/getAllByServiceIdAndUserId/{userId}/{serviceId}")
   public ResponseEntity<?> getAllByServiceIdAndUserId(HttpServletRequest request, @PathVariable("userId") Long userId, @PathVariable("serviceId") Long serviceId)
      throws SQLException
   {
      //log.info("ConsumerController/getAllConsumers");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> consumers = consumerService.getAllByServiceIdAndUserId(userId, serviceId);
            return ResponseEntity.ok(consumers);
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
   public ResponseEntity<?> addConsumer(HttpServletRequest request, @RequestBody Consumer consumer) throws SQLException
   {
      //log.info("UserController/addConsumer");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if(securityHelper.hasRole(request, roles))
         {
            if(consumer.getFirstName() == null || consumer.getFirstName() == "") return ResponseEntity.ok("FirstName is Required");
            consumerService.addConsumer(consumer);
            return ResponseEntity.ok(consumer);
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

   @PostMapping("/addService")
   public ResponseEntity<?> addConsumerService(HttpServletRequest request, @RequestBody ConsumerService consumerService) throws SQLException
   {
      //log.info("UserController/addUser");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if(securityHelper.hasRole(request, roles))
         {
            consumerServiceService.addConsumerService(consumerService);
            return ResponseEntity.ok(consumerService);
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

   @PostMapping("/loadConsumers/{serviceProviderId}")
   public ResponseEntity<?> loadConsumers(HttpServletRequest request, @PathVariable("serviceProviderId") Long serviceProviderId) throws SQLException
   {
      //log.info("UserController/addUser");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
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
            User user = userService.getUserByEmail(userName);
            consumerService.loadConsumers(serviceProviderId,user);
            return ResponseEntity.ok("all data imported successfully");
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
   public ResponseEntity<?> updateConsumer(HttpServletRequest request, @RequestBody Consumer consumer) throws SQLException
   {
      //log.info("UserController/addConsumer");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         if(securityHelper.hasRole(request, roles))
         {
            if(consumer.getFirstName() == null || consumer.getFirstName() == "") return ResponseEntity.ok("FirstName is Required");
            consumerService.updateConsumer(consumer);
            return ResponseEntity.ok(consumer);
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

   @GetMapping("flagged/getAll")
   public ResponseEntity<?> getAllflaggedConsumers(HttpServletRequest request,@RequestParam(value = "params", required = false) String params) throws SQLException
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
            Map<String, Object> consumers = consumerService.getAllFlaggedConsumers2(params);

            return ResponseEntity.ok(consumers);
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
