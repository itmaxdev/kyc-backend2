package com.app.kyc.service;

import java.util.List;

import com.app.kyc.entity.Notification;
import com.app.kyc.entity.User;
import com.app.kyc.model.NotificationType;

public interface NotificationService
{

   public Notification getNotificationById(Long id);

   public List<Notification> getAllNotifications(Long userId);

   public void addNotification(String message, User userI, NotificationType notificationType, Long clickableId);

   public void markNotificationRead(Long id);
   
   public void markAllAsReadByUserId(Long id);

}
