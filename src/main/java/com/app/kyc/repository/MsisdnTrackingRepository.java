package com.app.kyc.repository;



import com.app.kyc.entity.MsisdnTracking;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface MsisdnTrackingRepository extends JpaRepository<MsisdnTracking, Long> {

    boolean existsByMsisdn(String msisdn);

    @Modifying
    @Transactional
    @Query(value = """
    INSERT INTO msisdn_tracking (msisdn, first_name, last_name, status, created_on)
    SELECT 
        c.msisdn,
        c.first_name,
        c.last_name,
        c.status,
        c.created_on
    FROM consumers c
    WHERE LOWER(TRIM(c.status)) = 'recycled'
      AND NOT EXISTS (
            SELECT 1 
            FROM msisdn_tracking t 
            WHERE t.msisdn COLLATE utf8mb4_unicode_ci
                  = c.msisdn COLLATE utf8mb4_unicode_ci
      )
    """, nativeQuery = true)
    int insertRecycledMsisdns();

    List<MsisdnTracking> findByMsisdnOrderByCreatedOnDesc(String msisdn);
}
