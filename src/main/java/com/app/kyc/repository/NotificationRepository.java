package com.app.kyc.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.Notification;

@Repository
public interface NotificationRepository extends JpaRepository<Notification, Long>
{

   List<Notification> findAllByUserId(Long userId);
   
   // Only unread notifications for a user
   List<Notification> findAllByUserIdAndMarkReadFalseOrderByCreatedOnDesc(Long userId);

}
