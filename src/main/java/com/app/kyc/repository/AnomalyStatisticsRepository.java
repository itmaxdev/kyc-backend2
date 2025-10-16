package com.app.kyc.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.app.kyc.entity.AnomalyStatistics;
import java.util.List;

@Repository
public interface AnomalyStatisticsRepository extends JpaRepository<AnomalyStatistics, Long> {
    List<AnomalyStatistics> findByAnomalyIdOrderByRecordedOnDesc(Long anomalyId);
    List<AnomalyStatistics> findByAnomalyIdOrderByRecordedOnAsc(Long anomalyId);

}
