package com.app.kyc.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.User;
import com.app.kyc.entity.UserUnmaskSession;

@Repository
public interface UserUnmaskSessionRepository extends JpaRepository<UserUnmaskSession, Long> {
    boolean existsByUserAndActiveTrueAndUnmaskExpiryAfter(User user, LocalDateTime now);
    List<UserUnmaskSession> findByActiveTrueAndUnmaskExpiryBefore(LocalDateTime now);
}