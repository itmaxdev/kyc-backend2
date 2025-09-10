package com.app.kyc.web.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.app.kyc.service.NotificationService;
import com.app.kyc.service.UserService;
import com.app.kyc.web.security.SecurityHelper;

@RestController
@RequestMapping("/notification")
public class NotificationController
{

   @Autowired
   NotificationService notificationService;

   @Autowired
   UserService userService;

   @Autowired
   SecurityHelper securityHelper;

   @GetMapping("/getAll/{userId}")
   public ResponseEntity<?> getAllNotifications(HttpServletRequest request, @PathVariable("userId") Long userId) throws SQLException
   {
      //log.info("NotificationController/getAllNotifications");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         roles.add("SP Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            return ResponseEntity.ok(notificationService.getAllNotifications(userId));
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

   @PutMapping("/markNotificationRead/{id}")
   public ResponseEntity<?> markNotificationRead(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("AnomalyController/updateAnomaly");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         roles.add("SP Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            notificationService.markNotificationRead(id);
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
   
   
   @PutMapping("/markNotificationRead/{id}")
   public ResponseEntity<?> markAllAsReadNotification(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("markAllAsReadNotification");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         roles.add("KYC Admin");
         roles.add("SP Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
         {
            notificationService.markAllAsReadByUserId(id);
            return ResponseEntity.ok("Anomaly markAsRead successfully");
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
