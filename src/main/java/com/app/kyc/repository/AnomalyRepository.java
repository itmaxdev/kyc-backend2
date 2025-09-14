package com.app.kyc.repository;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.app.kyc.entity.*;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.model.DashboardObjectInterface;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import org.springframework.transaction.annotation.Transactional;

@Repository
public interface AnomalyRepository extends JpaRepository<Anomaly, Long>
{
   @Query(value = "SELECT * FROM anomalies WHERE anomaly_type_id = ?", nativeQuery = true)
   List<Anomaly> findByAnomalyType(@Param("anomalyTypeId") Long anomalyTypeId);

   @Query(value = "select * from anomalies where anomaly_type_id in (select id from anomaly_types where (target_entity_type = 2 and entity_id = :industryId) " + "or (target_entity_type = 1 and entity_id in (select id from service_types where industry_id = :industryId)))" + "and reported_on > :start and reported_on <= :end", nativeQuery = true)
   List<Anomaly> findAllByIndustryIdAndReportedOnGreaterThanAndReportedOnLessThanEqual(Long industryId, Date start, Date end);

   @Query(value = "select * from anomalies where consumers_services_id in " + "(select id from consumers_services where service_id in" + "(select id from services where service_type_id = :serviceTypeId))" + "and reported_on > :start and reported_on <= :end", nativeQuery = true)
   List<Anomaly> findAllAnomalyByServiceTypeIdAndReportedOnGreaterThanAndReportedOnLessThanEqual(Long serviceTypeId, Date start, Date end);

   @Query(value = "select * from anomalies where consumers_services_id in " + "(select id from consumers_services where service_id in" + "(select id from services where service_provider_id = :serviceProviderId and service_type_id = :serviceTypeId))" + "and reported_on > :start and reported_on <= :end", nativeQuery = true)
   List<Anomaly> findAllAnomalyByServiceProviderAndServiceTypeIdAndReportedOnGreaterThanAndReportedOnLessThanEqual(Long serviceProviderId, Long serviceTypeId, Date start,
      Date end);

   @Query(value = "select * from anomalies where consumers_services_id in " + "(select id from consumers_services where service_id in" + "(select id from services where service_provider_id = :serviceProviderId))", nativeQuery = true)
   Page<Anomaly> findAllAnomalyByServiceProviderId(Long serviceProviderId, org.springframework.data.domain.Pageable pageable);

   Page<Anomaly> findDistinctByConsumers_ServiceProviderId(@Param("serviceProviderId") Long serviceProviderId, org.springframework.data.domain.Pageable pageable);

   @Query(value = "SELECT " +
           "    COALESCE(AVG(resolveDay),0) AS resolveAvgDay " +
           "FROM ( " +
           "    SELECT " +
           "        a.id, " +
           "        DATEDIFF(a.updated_on, a.reported_on) AS resolveDay " +
           "    FROM anomalies a " +
           "    JOIN consumers_anomalies ca ON ca.anomaly_id = a.id " +
           "    JOIN consumers c ON c.id = ca.consumer_id " +
           "    WHERE a.status IN (5,6) " +
           "      AND a.reported_on > :start " +
           "      AND a.reported_on <= :end " +
           "      AND c.service_provider_id IN (:serviceProviderIds) " +
           "    GROUP BY a.id " +
           ") anomaly",
           nativeQuery = true
   )
   double getAverageResolutionTimeInHours(
           @Param("serviceProviderIds") List<Long> serviceProviderIds,
           @Param("start") java.util.Date start,
           @Param("end") java.util.Date end
   );
  
   @Query(value = "WITH RECURSIVE period_series AS ( "
	        + "    SELECT "
	        + "        CASE "
	        + "            WHEN :groupBy = 'DAY' THEN DATE(:start) "
	        + "            WHEN :groupBy = 'MONTH' THEN DATE_FORMAT(:start, '%Y-%m-01') "
	        + "            WHEN :groupBy = 'QUARTER' THEN MAKEDATE(YEAR(:start), 1) + INTERVAL QUARTER(:start)-1 QUARTER "
	        + "        END AS period "
	        + "    UNION ALL "
	        + "    SELECT "
	        + "        CASE "
	        + "            WHEN :groupBy = 'DAY' THEN DATE_ADD(period, INTERVAL 1 DAY) "
	        + "            WHEN :groupBy = 'MONTH' THEN DATE_ADD(period, INTERVAL 1 MONTH) "
	        + "            WHEN :groupBy = 'QUARTER' THEN DATE_ADD(period, INTERVAL 3 MONTH) "
	        + "        END "
	        + "    FROM period_series "
	        + "    WHERE "
	        + "        ( :groupBy = 'DAY' AND period < DATE(:end) ) "
	        + "        OR ( :groupBy = 'MONTH' AND period < DATE_FORMAT(:end, '%Y-%m-01') ) "
	        + "        OR ( :groupBy = 'QUARTER' AND period < MAKEDATE(YEAR(:end), 1) + INTERVAL QUARTER(:end)-1 QUARTER ) "
	        + ") "
	        + "SELECT "
	        + "    CASE "
	        + "        WHEN :groupBy = 'DAY' THEN DATE_FORMAT(ps.period, '%W') "
	        + "        WHEN :groupBy = 'MONTH' THEN DATE_FORMAT(ps.period, '%b') "
	        + "        WHEN :groupBy = 'QUARTER' THEN CONCAT(YEAR(ps.period), '-Q', QUARTER(ps.period)) "
	        + "    END AS period, "
	        + "    COALESCE(SUM(CASE WHEN a.id IS NOT NULL AND a.status IN (5,6) THEN 1 ELSE 0 END), 0) AS resolved, "
	        + "    COALESCE(SUM(CASE WHEN a.id IS NOT NULL AND a.status NOT IN (5,6) THEN 1 ELSE 0 END), 0) AS unresolved "
	        + "FROM period_series ps "
	        + "LEFT JOIN anomalies a "
	        + " ON ( "
	        + "     (:groupBy = 'DAY' AND DATE(a.reported_on) = ps.period) "
	        + "     OR (:groupBy = 'MONTH' AND DATE_FORMAT(a.reported_on, '%Y-%m') = DATE_FORMAT(ps.period, '%Y-%m')) "
	        + "     OR (:groupBy = 'QUARTER' AND YEAR(a.reported_on) = YEAR(ps.period) AND QUARTER(a.reported_on) = QUARTER(ps.period)) "
	        + "   ) "
	        + "   AND a.id IN ( "
	        + "       SELECT ca.anomaly_id "
	        + "       FROM consumers_anomalies ca "
	        + "       WHERE ca.consumer_id IN ( "
	        + "           SELECT c.id FROM consumers c WHERE c.service_provider_id IN (:serviceProviderIds) "
	        + "       ) "
	        + "   ) "
	        + "GROUP BY period ",
	       nativeQuery = true)
   List<Object[]> getResolutionMetrics( @Param("serviceProviderIds") List<Long> serviceProviderIds, @Param("start") Date start, @Param("end") Date end, @Param("groupBy") String groupBy);


   @Query(value = "select COALESCE(avg(difference),0) from (" + "select *, TIMESTAMPDIFF(HOUR, reported_on, updated_on) " + "as difference from anomalies where (status = 5 or status = 6) " + "and reported_on > :start and reported_on <= :end and consumers_services_id in " + "(select id from consumers_services where service_id in" + " (select id from services where service_provider_id = :serviceProviderId))) as a", nativeQuery = true)
   int getAverageResolutionTimeInHoursByServiceProvider(Long serviceProviderId, Date start, Date end);

   @Query(value = "select COALESCE(avg(difference),0) from (" + "select *, TIMESTAMPDIFF(HOUR, reported_on, updated_on) " + "as difference from anomalies where (status = 5 or status = 6) " + "and reported_on > :start and reported_on <= :end and consumers_services_id in " + "(select id from consumers_services where service_id in" + " (select id from services where service_type_id = :serviceTypeId))) as a", nativeQuery = true)
   int getAverageResolutionTimeInHoursByServiceType(Long serviceTypeId, Date start, Date end);

   @Query(value = "select COALESCE(avg(difference),0) from (" + "select *, TIMESTAMPDIFF(HOUR, reported_on, updated_on) " + "as difference from anomalies where (status = 5 or status = 6) " + "and reported_on > :start and reported_on <= :end and consumers_services_id in " + "(select id from consumers_services where service_id in " + "(select id from services where service_provider_id = :serviceProviderId and service_type_id = :serviceTypeId))) as a", nativeQuery = true)
   int getAverageResolutionTimeInHoursByServiceProviderAndServiceType(Long serviceProviderId, Long serviceTypeId, Date start, Date end);

   Anomaly findFirstByConsumersInAndAnomalyType(List<Consumer> consumers,AnomalyType anomalyType);



   int countById(Long anomalyId);

   @Query(value = "SELECT * FROM anomalies WHERE id IN (:anomalyId) and anomaly_type_id = :anomalyTypeId", nativeQuery = true)
   Anomaly findByIdAndAnomalyType_Id(@Param("anomalyId") List<Long> anomalyId, @Param("anomalyTypeId") Long anomalyTypeId);

    @Query(value = "SELECT * FROM anomalies WHERE id IN (:anomalyId) and anomaly_type_id = :anomalyTypeId", nativeQuery = true)
    Anomaly findByIdAndAnomalyTypeId(@Param("anomalyId") Long anomalyId, @Param("anomalyTypeId") Long anomalyTypeId);

    @Query(value = "select Distinct a from Anomaly a join ConsumerAnomaly ca on a.id = ca.anomaly.id join Consumer c on ca.consumer.id = c.id where c.consumerStatus in (:consumer_status) and a.status in (:anomalyStatus) and (:anomalyType is null or a.anomalyType.id = :anomalyType)")
   Page<Anomaly> findAllByConsumerStatus(Pageable pageable, @Param("consumer_status") List<Integer> consumer_status, @Param("anomalyStatus") List<AnomalyStatus> anomalyStatus,@Param("anomalyType") Long anomalyType);

   @Query(value = "select Distinct a from Anomaly a join ConsumerAnomaly ca on a.id = ca.anomaly.id join Consumer c on ca.consumer.id = c.id where (coalesce(:anomalyStatus, null) is null or a.status in (:anomalyStatus)) and (coalesce(:resolutionStatus, null) is null or a.status in (:resolutionStatus))  and (:anomalyType is null or a.anomalyType.id = :anomalyType)")
   Page<Anomaly> findAllByConsumersAll(Pageable pageable, @Param("anomalyStatus") List<AnomalyStatus> anomalyStatus, @Param("anomalyType") Long anomalyType, @Param("resolutionStatus") List<AnomalyStatus> resolutionStatus);

   @Query(value = "select Distinct a from Anomaly a join ConsumerAnomaly ca on a.id = ca.anomaly.id join Consumer c on ca.consumer.id = c.id where c.consumerStatus in (:consumer_status) and c.serviceProvider.id IN (:serviceProviderId) and (coalesce(:anomalyStatus, null) is null or a.status in (:anomalyStatus)) and (coalesce(:resolutionStatus, null) is null or a.status in (:resolutionStatus)) and (:anomalyType is null or a.anomalyType.id = :anomalyType)")
   Page<Anomaly> findAllByConsumerStatusAndServiceProviderId(Pageable pageable, @Param("consumer_status") List<Integer> consumer_status, @Param("serviceProviderId") List<Long> serviceProviderId, @Param("anomalyStatus") List<AnomalyStatus> anomalyStatus, @Param("anomalyType") Long anomalyType,@Param("resolutionStatus") List<AnomalyStatus> resolutionStatus);

   @Query("SELECT DISTINCT a " +
           "FROM Anomaly a " +
           "JOIN ConsumerAnomaly ca ON a.id = ca.anomaly.id " +
           "JOIN Consumer c ON ca.consumer.id = c.id " +
           "WHERE c.serviceProvider.id IN (:serviceProviderId)")
   Page<Anomaly> findAllByConsumerStatusAndServiceProviderIdOnly(
           Pageable pageable,
           @Param("serviceProviderId") List<Long> serviceProviderId);

   @Transactional
   void deleteAllByIdIn(List<Long> ids);

   @Query("select count(distinct a.id) from Anomaly a inner join a.consumers consumers " +
           "where consumers.serviceProvider.id in ?1 and a.status in ?2 and a.reportedOn between ?3 and ?4")
   long countDistinctByConsumers_ServiceProvider_IdInAndStatusInAndReportedOnBetween(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd);

   @Query("select count(distinct a.id) from Anomaly a inner join a.consumers consumers " +
           "where consumers.serviceProvider.id in ?1 and a.status in ?2 and a.reportedOn between ?3 and ?4")
   long countDistinctByConsumers_Withdrawn_ServiceProvider_IdInAndStatusInAndReportedOnBetween(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd);

   @Query("select CONCAT(YEAR(a.reportedOn),'-',MONTH(a.reportedOn)) as name, count(distinct (a.id)) as value from Anomaly a inner join a.consumers consumers " +
           "where consumers.serviceProvider.id in ?1 and a.status in ?2 and a.reportedOn between ?3 and ?4 group by name order by a.reportedOn")
   List<DashboardObjectInterface> countDistinctByConsumers_ServiceProvider_IdInAndStatusInAndReportedOnBetweenDateGroupByYearMonth(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd);

   @Query("select DATE(a.reportedOn) as name, count(distinct (a.id)) as value from Anomaly a inner join a.consumers consumers " +
      "where consumers.serviceProvider.id in ?1 and a.status in ?2 and a.reportedOn between ?3 and ?4 group by name order by a.reportedOn")
   List<DashboardObjectInterface> countDistinctByConsumers_ServiceProvider_IdInAndStatusInAndReportedOnBetweenDateGroupByYearMonthDate(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd);


   long countByStatusNotIn(List<AnomalyStatus> statuses);

}
