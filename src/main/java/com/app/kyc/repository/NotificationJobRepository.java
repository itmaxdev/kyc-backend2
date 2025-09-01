package com.app.kyc.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.NotificationJob;

@Repository
public interface NotificationJobRepository extends JpaRepository<NotificationJob, Long> {
    List<NotificationJob> findByState(String state);
}

