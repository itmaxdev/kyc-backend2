package com.app.kyc.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import com.app.kyc.entity.AnomalyStatistics;
import java.util.List;
import java.util.Optional;

@Repository
public interface AnomalyStatisticsRepository extends JpaRepository<AnomalyStatistics, Long> {
    List<AnomalyStatistics> findByAnomalyIdOrderByRecordedOnDesc(Long anomalyId);
    List<AnomalyStatistics> findByAnomalyIdOrderByRecordedOnAsc(Long anomalyId);

    @Query(value = " SELECT s.* FROM anomaly_statistics s INNER JOIN ( SELECT anomaly_id, MIN(id) AS min_id FROM anomaly_statistics WHERE partially_resolved_percentage <> 100.00 GROUP BY anomaly_id) t ON s.id = t.min_id ", nativeQuery = true)
    List<AnomalyStatistics> findUniqueNonFullyResolved();

    @Query(value = "SELECT s.* FROM anomaly_statistics s WHERE s.anomaly_id = :anomalyId AND s.partially_resolved_percentage <> 100.00 ORDER BY s.id ASC LIMIT 1 ", nativeQuery = true)
    List<AnomalyStatistics> findFirstNonFullyResolvedByAnomalyId(@Param("anomalyId") Long anomalyId);


    Optional<AnomalyStatistics> findTopByAnomalyIdOrderByRecordedOnDesc(Long anomalyId);
}

