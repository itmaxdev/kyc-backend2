package com.app.kyc.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.ConsumerTracking;

@Repository
public interface ConsumerTrackingRepository extends JpaRepository<ConsumerTracking, Long>
{

}
