package com.app.kyc.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.AnomalyTracking;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface AnomalyTrackingRepository extends JpaRepository<AnomalyTracking, Long>
{

   List<AnomalyTracking> findAllByAnomalyId(Long id);
   // Get all tracking records for an anomaly, latest first
   List<AnomalyTracking> findAllByAnomalyIdOrderByCreatedOnDesc(Long anomalyId);

   List<AnomalyTracking> findDistinctByAnomalyIdOrderByCreatedOnDesc(Long anomalyId);

   @Modifying
   @Transactional
   @Query(value = """
    INSERT INTO anomaly_tracking (
        anomaly_id,
        status,
        created_on,
        note,
        update_by,
        update_on
    )
    SELECT 
        a.id AS anomaly_id,
        a.status,
        a.reported_on AS created_on,
        a.note,
        a.update_by,
        a.updated_on AS update_on
    FROM anomalies a
    LEFT JOIN anomaly_tracking t 
        ON t.anomaly_id = a.id
    WHERE t.anomaly_id IS NULL
    """, nativeQuery = true)
   int insertMissingAnomaliesIntoTracking();





   @Query(value = """
    SELECT 
        c.id AS consumer_id,
        CAST(c.status AS CHAR) AS consumer_status,
        a.id AS anomaly_id,
        CAST(a.status AS CHAR) AS anomaly_status
    FROM consumers c
    INNER JOIN consumers_anomalies ca ON ca.consumer_id = c.id
    INNER JOIN anomalies a ON a.id = ca.anomaly_id
    WHERE c.service_provider_id = :spId
""", nativeQuery = true)
   List<Object[]> findConsumersWithExistingAnomalies(@Param("spId") Long spId);


   @Modifying
   @Transactional
   @Query(value = """
    UPDATE anomalies
    SET status = :statusCode,
        note = CONCAT(COALESCE(note, ''), ' | Auto-updated on re-upload: ', :remarks),
        updated_on = NOW(),
        update_by = :updatedBy
    WHERE id = :anomalyId
""", nativeQuery = true)
   void updateAnomalyStatus(@Param("anomalyId") Long anomalyId,
                            @Param("statusCode") int statusCode,
                            @Param("remarks") String remarks,
                            @Param("updatedBy") String updatedBy);

}
