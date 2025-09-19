package com.app.kyc.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.ConsumerTracking;

@Repository
public interface ConsumerTrackingRepository extends JpaRepository<ConsumerTracking, Long>
{
	List<ConsumerTracking> findByConsumerId(Long consumerId);
	List<ConsumerTracking> findByConsumerIdOrderByCreatedOnDesc(Long consumerId);
}
