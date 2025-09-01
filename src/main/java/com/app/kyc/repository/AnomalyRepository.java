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

   @Query(
           value =
                   "SELECT COALESCE(AVG(TIMESTAMPDIFF(HOUR, a.reported_on, IFNULL(a.updated_on, NOW()))), 0) " +
                           "FROM anomalies a " +
                           "WHERE a.status IN (5, 6) " +
                           "  AND a.reported_on > :start " +
                           "  AND a.reported_on <= :end " +
                           "  AND a.anomaly_type_id IN ( " +
                           "      SELECT at.id " +
                           "      FROM anomaly_types at " +
                           "      WHERE (at.target_entity_type = 2 AND at.entity_id = :industryId) " +
                           "         OR (at.target_entity_type = 1 AND at.entity_id IN ( " +
                           "                SELECT st.id FROM service_types st WHERE st.industry_id = :industryId " +
                           "             )) " +
                           "  )",
           nativeQuery = true
   )
   double getAverageResolutionTimeInHours(
           @Param("industryId") Long industryId,
           @Param("start") java.util.Date start,
           @Param("end") java.util.Date end
   );


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

   @Query(value = "select Distinct a from Anomaly a join ConsumerAnomaly ca on a.id = ca.anomaly.id join Consumer c on ca.consumer.id = c.id where c.consumerStatus in (:consumer_status) and a.status in (:anomalyStatus)")
   Page<Anomaly> findAllByConsumerStatus(Pageable pageable, @Param("consumer_status") List<Integer> consumer_status, @Param("anomalyStatus") List<AnomalyStatus> anomalyStatus);

   @Query(value = "select Distinct a from Anomaly a join ConsumerAnomaly ca on a.id = ca.anomaly.id join Consumer c on ca.consumer.id = c.id where c.consumerStatus in (:consumer_status) and c.serviceProvider.id = :serviceProviderId and a.status in(:anomalyStatus)")
   Page<Anomaly> findAllByConsumerStatusAndServiceProviderId(Pageable pageable, @Param("consumer_status") List<Integer> consumer_status, @Param("serviceProviderId") Long serviceProviderId, @Param("anomalyStatus") List<AnomalyStatus> anomalyStatus);

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
