package com.app.kyc.repository;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.app.kyc.model.DashboardAnomalyStatusInterface;
import org.springframework.data.jpa.repository.JpaRepository;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.AnomalyType;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ConsumerAnomaly;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface ConsumerAnomalyRepository extends JpaRepository<ConsumerAnomaly, Long>
{
    List<ConsumerAnomaly> getAllByAnomalyAndConsumer(Anomaly anomaly,Consumer consumer);
    List<ConsumerAnomaly> findByAnomaly_AnomalyTypeAndConsumer(AnomalyType anomalyType,Consumer consumer);

    List<ConsumerAnomaly> findAllByConsumerIn(List<Consumer> consumers);

//    @Query(value = "select anomaly_id from consumers_anomalies where consumer_id IN (:consumerIds)",  nativeQuery = true)
//    List<Long> findAnomaliesIdByConsumer(@Param("consumerIds") List<Long> consumerIds);

    @Query(" select ca from ConsumerAnomaly ca join fetch ca.consumer c where ca.anomaly.id in :anomalyIds")
    List<ConsumerAnomaly> findAllByAnomalyIdInFetchConsumer(@Param("anomalyIds") Collection<Long> anomalyIds);

    @Query(value = "select a.id from consumers_anomalies ac join anomalies a on ac.anomaly_id = a.id where ac.consumer_id in (:consumerIds) AND a.status NOT IN (5, 6);",  nativeQuery = true)
    List<Long> findAnomaliesIdByConsumer(@Param("consumerIds") List<Long> consumerIds);

    @Query(value = "select a.id from consumers_anomalies ac join anomalies a on ac.anomaly_id = a.id where ac.consumer_id in (:consumerIds) AND a.status NOT IN (5, 6) AND a.anomaly_type_id = :anomalyTypeId",  nativeQuery = true)
    List<Long> findAnomaliesIdByConsumerAndAnomalyTypeId(@Param("consumerIds") List<Long> consumerIds, @Param("anomalyTypeId") Long anomalyTypeId);

    List<ConsumerAnomaly> findByConsumer_IdAndAnomaly_Id(Long consumer_id, Long anomaly_id);


    @Transactional
    List<ConsumerAnomaly> deleteAllByConsumerIn(List<Consumer> consumers);

    List<ConsumerAnomaly> findByAnomaly_Id(Long id);

    List<ConsumerAnomaly> findByAnomaly_IdAndConsumer_Id(Long anomaly_id, Long consumer_Id);


    @Query("select sp.id as serviceProviderId, count(distinct ca.anomaly) as anomalyCount, a.status as status, sp.name as name from ConsumerAnomaly ca join Anomaly a on ca.anomaly.id = a.id join Consumer c on ca.consumer.id = c.id join ServiceProvider sp on c.serviceProvider.id = sp.id where (a.reportedOn between :startDate and :endDate) and sp.id in (:serviceProviderIds) group by c.serviceProvider.id, a.status")
    List<DashboardAnomalyStatusInterface> countAnomaliesByServiceProviderAndAnomalyStatus(@Param("startDate")Date startDate, @Param("endDate")Date endDate, @Param("serviceProviderIds") List<Long> serviceProviderIds);
}
