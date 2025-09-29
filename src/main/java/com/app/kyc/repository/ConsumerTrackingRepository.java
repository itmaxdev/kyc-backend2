package com.app.kyc.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.ConsumerTracking;
import com.app.kyc.entity.ServiceProvider;

@Repository
public interface ConsumerTrackingRepository extends JpaRepository<ConsumerTracking, Long>
{
	List<ConsumerTracking> findByConsumerId(Long consumerId);
	List<ConsumerTracking> findByConsumerIdOrderByCreatedOnDesc(Long consumerId);
	Optional<ConsumerTracking> findTopByConsumerIdAndServiceProviderOrderByCreatedOnDesc(Long consumerId, ServiceProvider provider);
}
