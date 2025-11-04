package com.app.kyc.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.ConsumerTracking;
import com.app.kyc.entity.ServiceProvider;

import javax.transaction.Transactional;

@Repository
public interface ConsumerTrackingRepository extends JpaRepository<ConsumerTracking, Long>
{
	List<ConsumerTracking> findByConsumerId(Long consumerId);
	List<ConsumerTracking> findByConsumerIdOrderByCreatedOnDesc(Long consumerId);
	Optional<ConsumerTracking> findTopByConsumerIdAndServiceProviderOrderByCreatedOnDesc(Long consumerId, ServiceProvider provider);
	ConsumerTracking findFirstByConsumerIdAndIsConsistentTrueOrderByCreatedOnDesc(Long consumerId);
	ConsumerTracking findFirstByConsumerIdAndIsConsistentFalseOrderByCreatedOnDesc(Long consumerId);

	@Modifying
	@Transactional
	@Query(value = """
    INSERT INTO consumer_tracking (
        consumer_id,
        service_provider_id,
        consistent_on,
        is_consistent,
        created_on
    )
    SELECT 
        c.id AS consumer_id,
        c.service_provider_id,
        CURDATE() AS consistent_on,
        1 AS is_consistent,
        NOW() AS created_on
    FROM consumers c
    LEFT JOIN consumer_tracking t 
        ON c.id = t.consumer_id
    WHERE t.consumer_id IS NULL
    """, nativeQuery = true)
	int insertMissingConsumerTracking();

}
