package com.app.kyc.repository;

import java.util.List;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.Notification;

@Repository
public interface NotificationRepository extends JpaRepository<Notification, Long>
{

   List<Notification> findAllByUserId(Long userId);
   
   // Only unread notifications for a user
   List<Notification> findAllByUserIdAndMarkReadFalseOrderByCreatedOnDesc(Long userId);
   
   @Modifying
   @Transactional
   @Query("UPDATE Notification n SET n.markRead = 1 WHERE n.userId = :userId AND n.markRead = 0")
   int markAllAsReadByUserId(@Param("userId") Long userId);

}
