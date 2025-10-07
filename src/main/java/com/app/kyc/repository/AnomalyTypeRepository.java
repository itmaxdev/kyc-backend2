package com.app.kyc.repository;

import java.util.List;

import com.app.kyc.model.DashboardObjectInterface;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.AnomalyType;

@Repository
public interface AnomalyTypeRepository extends JpaRepository<AnomalyType, Long>
{
   @Query(value = "SELECT * FROM anomaly_types WHERE created_by = ?", nativeQuery = true)
   List<AnomalyType> findByCreatedBy(@Param("createdById") Long createdById);

   @Query(value = "select * from anomaly_types where name = :name and target_entity_type = :targetEntityType and entity_id = :entityId", nativeQuery = true)
   List<AnomalyType> checkAnomalyTypeUnique(@Param("name") String name, @Param("targetEntityType") int targetEntityType, @Param("entityId") Long entityId);

   AnomalyType findFirstByName(String name);

   @Query(value = "SELECT t.name AS name, COUNT(a.id) AS value " +
           "FROM anomaly_types t " +
           "LEFT JOIN anomalies a ON a.anomaly_type_id = t.id " +
           "GROUP BY t.name",
           nativeQuery = true)
   List<DashboardObjectInterface> getAnomalyTypeCounts();



   List<AnomalyType> findByDeletedFalse();

   boolean existsByNameIgnoreCase(String name);

}
