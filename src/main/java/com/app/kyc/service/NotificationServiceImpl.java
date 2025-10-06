package com.app.kyc.service;

import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.app.kyc.entity.Notification;
import com.app.kyc.entity.User;
import com.app.kyc.model.NotificationType;
import com.app.kyc.repository.NotificationRepository;
import com.app.kyc.util.EmailUtil;

@Service
public class NotificationServiceImpl implements NotificationService
{

   @Value("${spring.allow.email.notification:false}")
   private boolean allowEmailNotification;
   
   @Autowired
   private NotificationRepository notificationRepository;

   public Notification getNotificationById(Long id)
   {
      return notificationRepository.findById(id).get();
   }

   @Override
   public List<Notification> getAllNotifications(Long userId)
   {
      //return notificationRepository.findAllByUserIdAndMarkReadFalseOrderByCreatedOnDesc(userId);
      List<Notification> notifications =
              notificationRepository.findAllByUserIdAndMarkReadFalseOrderByCreatedOnDesc(userId);

      notifications.forEach(n -> {
         String msg = n.getMessage();
         if (msg != null && msg.toLowerCase().contains("on ")) {
            n.setMessage(maskNameAfterOn(msg));
         }
      });

      return notifications;
   }


   private String maskNameAfterOn(String message) {
      Pattern pattern = Pattern.compile("(?i)(?<=\\bon\\s)([a-zA-Z]+)\\s+([a-zA-Z]+)");
      Matcher matcher = pattern.matcher(message);

      if (matcher.find()) {
         String firstName = matcher.group(1);
         String lastName = matcher.group(2);

         String maskedFirst = maskWord(firstName);
         String maskedLast = maskWord(lastName);

         return matcher.replaceFirst(maskedFirst + " " + maskedLast);
      }

      return message; // fallback if no match
   }

   /** Mask a word like "jose" → "j***" */
   private String maskWord(String word) {
      if (word == null || word.isEmpty()) return word;
      if (word.length() == 1) return word + "*";
      return word.substring(0, 1) + "*".repeat(Math.max(1, word.length() - 1));
   }


   @Override
   public void addNotification(String message, User user, NotificationType notificationType, Long clickableId)
   {
      Notification notification = new Notification(message, user.getId(), clickableId, false, notificationType, new Date());
      notificationRepository.save(notification);
      
      if(allowEmailNotification) {
         EmailUtil.sendEmail(user.getEmail(), "You've Got A Notification - National KYC Platform", 
            "Hello, \r\nPlease note that you have recieved the following notification! Check it out:\r\n" + message + "\r\n\r\nBest,\r\n" + "National KYC");
      }
   }

   @Override
   public void markNotificationRead(Long id)
   {
      Notification notification = getNotificationById(id);
      notification.setMarkRead(true);
      notificationRepository.save(notification);
   }
   
	@Override
	public void markAllAsReadByUserId(Long id) {
		notificationRepository.markAllAsReadByUserId(id);
	}

}
