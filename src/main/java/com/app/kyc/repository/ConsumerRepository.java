package com.app.kyc.repository;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.response.FlaggedConsumersListDTO;

@Repository
public interface ConsumerRepository extends JpaRepository<Consumer, Long>
{
    @Query(value = "select g.id, g.consumerId, g.name, g.serviceName, g.serviceProviderId, (select name from service_providers where id = g.serviceProviderId) as serviceProviderName,\r\n" +
            " g.flaggedDate, g.anomalyEntityType, g.anomalyStatus, g.note, g.anomalyId, CONCAT(u.first_name, \" \", u.last_name) as reporterName \r\n" +
            "from users as u join ( select (select id from consumers where id = consumer_id) as consumerId, (select CONCAT(first_name, \" \", last_name) \r\n" +
            " from consumers where id = consumer_id) as name, (select name from services where id = service_id) as serviceName ,\r\n" +
            " (select service_provider_id from services where id = service_id) as serviceProviderId,\r\n" +
            " b.* from consumers_services as a join (select consumers_services_id, reported_on as flaggedDate, reported_by_id, \r\n" +
            " (select target_entity_type from anomaly_types where id = anomaly_type_id) as anomalyEntityType, status as anomalyStatus, id as anomalyId, \r\n" +
            "   note, id from anomalies) as b on a.id = b.consumers_services_id) as g on u.id = reported_by_id", nativeQuery = true)
    List<FlaggedConsumersListDTO> getAllFlaggedConsumers();


    // ANY status (used above). If you prefer active-only, add "AND c.consumer_status = 0".
    @Query(value = "SELECT * FROM consumers c WHERE c.service_provider_id = :spId AND TRIM(c.identification_type)   = TRIM(:type) AND TRIM(c.identification_number) = TRIM(:number)", nativeQuery = true)
    List<Consumer> findByIdKeyNormalizedAnyStatus(@Param("spId") Long spId,
                                                  @Param("type") String type,
                                                  @Param("number") String number);

    @Query("select count(c) from Consumer c")
    long countAllUnsafe(); // optional

    @Query("select coalesce(sum(case when c.isConsistent = true then 1 else 0 end),0) from Consumer c")
    long countConsistent();

    @Query("select coalesce(sum(case when c.isConsistent = false or c.isConsistent is null then 1 else 0 end),0) from Consumer c")
    long countInconsistent();


    long countByIsConsistent(boolean isConsistent);
    long countByIsConsistentIsNull(); // counts rows where isConsistent is NULL

    @Query("select count(c) from Consumer c where c.serviceProvider.id = :spId")
    long countByServiceProviderId(@Param("spId") Long spId);

    @Query("select coalesce(sum(case when c.isConsistent = true then 1 else 0 end),0) " +
            "from Consumer c where c.serviceProvider.id = :spId")
    long countConsistentByServiceProviderId(@Param("spId") Long spId);

    @Query("select coalesce(sum(case when c.isConsistent = false or c.isConsistent is null then 1 else 0 end),0) " +
            "from Consumer c where c.serviceProvider.id = :spId")
    long countInconsistentByServiceProviderId(@Param("spId") Long spId);


    Page<Consumer> findByServiceProvider_Id(Long serviceProviderId, Pageable pageable);

    Consumer findByIdAndConsumerStatus(long id, int consumerStatus);


    Optional<Consumer> findByIdAndConsumerStatusIn(Long id, Collection<Integer> statuses);

    @Query(value = "select * from consumers where consumer_status = :consumer_status and service_provider_id in (" +
            "select id from service_providers where industry_id = :industryId) " +
            "and created_on > :start and created_on <= :end", nativeQuery = true)
    List<Consumer> findAllConsumersByCreatedOnGreaterThanAndCreatedOnLessThanEqual(@Param("consumer_status")int consumer_status, Long industryId, Date start, Date end);

    @Query(value = "select count(*) from consumers where consumer_status = :consumer_status and service_provider_id in (" +
            "select id from service_providers where industry_id = :industryId) and created_on > :start and created_on <= :end", nativeQuery = true)
    int countByIndustryIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(@Param("consumer_status")int consumer_status, Long industryId, Date start, Date end);

    @Query(value = "select * from consumers where consumer_status = :consumer_status and id in (" +
            "select distinct consumer_id from consumers_services where service_id in " +
            "(select id from services where service_type_id = :serviceTypeId)) " +
            "and created_on > :start and created_on <= :end", nativeQuery = true)
    List<Consumer> findAllConsumersByServiceTypeAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(@Param("consumer_status")int consumer_status, Long serviceTypeId, Date start, Date end);

    List<Consumer> findAllConsumersByServiceProviderIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqualAndConsumerStatus(Long serviceProviderId, Date start, Date end, int consumer_status);

    @Query(value = "select * from consumers where consumer_status = :consumer_status and id in (" +
            "select distinct consumer_id from consumers_services where service_id in " +
            "(select id from services where service_type_id = :serviceTypeId)) " +
            "and service_provider_id = :serviceProviderId " +
            "and created_on > :start and created_on <= :end", nativeQuery = true)
    List<Consumer> findAllConsumersByServiceProviderIdAndServiceTypeIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(@Param("consumer_status")int consumer_status,Long serviceProviderId, Long serviceTypeId, Date start, Date end);

    List<Consumer> findAllConsumersByServiceProviderIdAndConsumerStatus(Long serviceProviderId, int consumer_status);

    @Query(value = "select * from consumers where consumer_status = :consumer_status and service_provider_id in (" +
            "select service_provider_id from services where id = :serviceId and created_by = :userId)", nativeQuery = true)
    List<Consumer> getAllByServiceIdAndUserId(@Param("consumer_status")int consumer_status, Long userId, Long serviceId);

    @Query(value = "SELECT * FROM consumers", nativeQuery = true)
    List<DashboardObjectInterface> getAllCustomers();

    @Query(value = "select (select name from service_providers where id = service_provider_id) as name, count(distinct consumers.id) as value, service_provider_id as serviceproviderid from consumers" +
            " where service_provider_id in :serviceProvidersIdList" +
            " and registration_date between :start and :end and consumer_status=0" +
            " group by name, serviceproviderid", nativeQuery = true)
    List<DashboardObjectInterface> getAndCountDistinctConsumersGroupedByServiceProviderId(@Param("serviceProvidersIdList") Collection<Long> serviceProvidersIdList, Date start, Date end);

    Page<Consumer> findByIsConsistentFalseAndConsumerStatusAndServiceProvider_Id(Pageable pageable, int consumerStatus, Long id);
    Page<Consumer> findByIsConsistentTrueAndConsumerStatusAndServiceProvider_Id(Pageable pageable, int consumerStatus, Long Id);




    Page<Consumer> findAll(Pageable pageable);
    Page<Consumer> findByIsConsistentTrue(Pageable pageable);
    Page<Consumer> findByIsConsistentFalse(Pageable pageable);

    long countByIsConsistentTrue();
    long countByIsConsistentFalse();


    @Transactional
    void deleteByServiceProvider(ServiceProvider serviceProvider);

    List<Consumer> findByFirstNameAndLastNameAndIdentificationTypeAndIdentificationNumberAndServiceProvider(String firstName,String lastName, String identificationType, String identificationNumber, ServiceProvider serviceProvider);

    List<Consumer> findByMsisdnAndIdentificationTypeAndIdentificationNumberAndServiceProvider(String msisdn, String identificationType, String identificationNumber, ServiceProvider serviceProvider);

    List<Consumer> getAllByAnomalies(Anomaly anomaly);

    int countById(Long id);
    int countByServiceProvider_Id(Long Id);

    /**
     * Prefer using {@link #findFirstByMsisdn(String)} or {@link #findIdByMsisdn(String)}.
     */
    @Deprecated
    List<Consumer> findByMsisdn(String msisdn);

    List<Consumer> findByMsisdnAndConsumerStatus(String msisdn, int consumerStatus);

    @Query(value = "SELECT * FROM consumers c WHERE c.msisdn in (:msisdn) and c.consumer_status = :consumer_status and c.id not in(select consumer_id from consumers_anomalies);", nativeQuery = true)
    List<Consumer> findConsumersNotInCA(@Param("msisdn") String msisdn, @Param("consumer_status") int consumer_status);

    @Query(value = "select cc.id  from consumers cc where cc.msisdn in (:msisdn)  and  cc.consumer_status=:consumer_status", nativeQuery = true)
    List<Long> findConsumerIdsByMsisdnAndConsumerStatus(@Param("msisdn") String msisdn, @Param("consumer_status") int consumer_status);

    @Query(value = "select * from consumers cc where cc.msisdn in (:msisdn)  and  cc.consumer_status=:consumer_status and cc.identification_type = :id_type and cc.identification_number = :id_number and cc.service_provider_id = :sp_id", nativeQuery = true)
    List<Consumer> findConsumerIdsByMsisdnAndConsumerStatusAndIdNumberAndIdTypeAndServiceProviderID(@Param("msisdn") String msisdn, @Param("consumer_status") int consumer_status, @Param("id_type") String id_type, @Param("id_number") String id_number, @Param("sp_id") Long sp_id);

    @Query(value = "update consumers set consumer_status = :flag where id in (:id)",nativeQuery = true)
    @Modifying
    @Transactional
    void updatePreviousConsumersStatus(@Param("flag") int flag, @Param("id") Long id);

    @Query(value = "update consumers set is_consistent  = :flag where id in (:id)",nativeQuery = true)
    @Modifying
    @Transactional
    void markConsumersConsistent(@Param("flag") int flag, @Param("id") List<Long> id);
    
    List<Consumer> findByIdentificationTypeAndIdentificationNumberAndServiceProvider(String identificationType, String identificationNumber, ServiceProvider serviceProvider);

    List<Consumer> findByIdentificationTypeAndIdentificationNumberAndServiceProviderAndConsumerStatus(String identificationType, String identificationNumber, ServiceProvider serviceProvider, int consumerStatus);

    List<Consumer> findAllByServiceProvider_Id(Long id);

    @Transactional
    void deleteAllByIdIn(List<Long> ids);

    @Query("select count(distinct c.id) from Consumer c where c.serviceProvider.id in ?1 and c.registrationDate between ?2 and ?3 and c.consumerStatus = 0")
    long countDistinctByServiceProvider_IdInAndCreatedOnBetween(Collection<Long> ids, Date registrationDateStart, Date registrationDateEnd);

    @Query(value="select count(distinct(concat(c.identification_type,c.identification_number))) sum from consumers c where c.service_provider_id in :ids and c.registration_date between :createdOnStart and :createdOnEnd and is_consistent = :isConsistent and c.consumer_status = :consumerStatus",nativeQuery = true)
    long countConsumersByServiceProvider_IdInAndRegistrationDateBetweenAndIsConsistentAndConsumerStatus(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, Boolean isConsistent, int consumerStatus);

    @Query(value="select count(*) sum from consumers c where c.service_provider_id in :ids and c.registration_date between :createdOnStart and :createdOnEnd and c.consumer_status = :consumerStatus",nativeQuery = true)
    long countSubscribersByServiceProvider_IdInAndRegistrationDateBetweenAndConsumerStatus(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, int consumerStatus);

    long countByServiceProvider_IdInAndRegistrationDateBetweenAndConsumerStatus(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, int consumerStatus);

    @Query(value="select CONCAT(YEAR(c.registration_date),'-',MONTH(c.registration_date)) as name, count((c.id)) as value from (select cc.registration_date, cc.id, cc.service_provider_id from consumers cc group by cc.identification_type,cc.identification_number) c where c.service_provider_id in ?1 and c.registration_date between ?2 and ?3 group by name order by c.registration_date",nativeQuery = true)
    List<DashboardObjectInterface> countByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonth(Collection<Long> ids, Date createdOnStart, Date createdOnEnd);

    @Query(value = "select DATE(c.registrationDate) as name, count((c.id)) as value from (select cc.registration_date, cc.id, cc.service_provider_id from consumers cc group by cc.identification_type,cc.identification_number) c where c.service_provider_id in ?1 and c.registration_date between ?2 and ?3 group by name order by name",nativeQuery = true)
    List<DashboardObjectInterface> countByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonthDate(Collection<Long> ids, Date createdOnStart, Date createdOnEnd);

    @Query("select CONCAT(YEAR(c.registrationDate),'-',MONTH(c.registrationDate)) as name, CAST(count(c.id) as int) as value from Consumer c " +
            "where c.serviceProvider.id in ?1 and c.registrationDate between ?2 and ?3 and c.consumerStatus = ?4 group by name order by c.registrationDate")
    List<DashboardObjectInterface> countDistinctByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonth(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, int consumerStatus);

    @Query("select DATE(c.registrationDate) as name, CAST(count(distinct c.id) as int) as value from Consumer c " +
            "where c.serviceProvider.id in ?1 and c.registrationDate between ?2 and ?3 and c.consumerStatus = ?4 group by name order by name")
    List<DashboardObjectInterface> countDistinctByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonthDate(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, int consumerStatus);

    @Query(value = "SELECT COUNT(*) FROM consumers", nativeQuery = true)
    long getTotalConsumers();

    @Query(value = "SELECT sp.name AS operatorName, COUNT(c.id) AS total " +
            "FROM consumers c " +
            "JOIN service_providers sp ON c.service_provider_id = sp.id " +
            "GROUP BY sp.name ", nativeQuery = true)
    List<Object[]> getConsumersPerOperator();

    @Query(
            value =
                    "(" +
                            "  SELECT CONCAT('Incomplete Data for ', sp.name) AS name, " +
                            "         COALESCE(SUM(CASE WHEN NULLIF(TRIM(c.msisdn), '') IS NULL THEN 1 ELSE 0 END), 0) AS value " +
                            "  FROM service_providers sp " +
                            "  LEFT JOIN consumers c ON c.service_provider_id = sp.id " +
                            "  WHERE (:providersIsNull = TRUE OR sp.name IN (:providers)) " +
                            "  GROUP BY sp.name " +
                            ") " +
                            "UNION ALL " +
                            "(" +
                            "  SELECT CONCAT('Duplicate Records for ', sp.name) AS name, " +
                            "         COALESCE(COUNT(d.msisdn), 0) AS value " +
                            "  FROM service_providers sp " +
                            "  LEFT JOIN consumers c ON c.service_provider_id = sp.id " +
                            "  LEFT JOIN ( " +
                            "      SELECT service_provider_id, msisdn " +
                            "      FROM consumers " +
                            "      WHERE NULLIF(TRIM(msisdn), '') IS NOT NULL " +
                            "      GROUP BY service_provider_id, msisdn " +
                            "      HAVING COUNT(*) > 1 " +
                            "  ) d ON d.service_provider_id = sp.id AND d.msisdn = c.msisdn " +
                            "  WHERE (:providersIsNull = TRUE OR sp.name IN (:providers)) " +
                            "  GROUP BY sp.name " +
                            ") " +
                            "UNION ALL " +
                            "(" +
                            "  SELECT CONCAT('Exceeding Threshold for ', sp.name) AS name, " +
                            "         GREATEST(COUNT(c.id) - :threshold, 0) AS value " +
                            "  FROM service_providers sp " +
                            "  LEFT JOIN consumers c ON c.service_provider_id = sp.id " +
                            "  WHERE (:providersIsNull = TRUE OR sp.name IN (:providers)) " +
                            "  GROUP BY sp.name " +
                            ") " +
                            "ORDER BY name",
            nativeQuery = true
    )
    List<DashboardObjectInterface> getMsisdnAnomalyTypesRollup(
            @Param("providers") List<String> providers,
            @Param("providersIsNull") boolean providersIsNull,
            @Param("threshold") int threshold
    );
    
    
	@Query(value = "(SELECT CONCAT('Incomplete Data for ', sp.name) AS name," 
			+ "       SUM("
			+ "           CASE WHEN "
			+ "                   (   NULLIF(TRIM(c.msisdn), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.registration_date), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.first_name), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.middle_name), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.last_name), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.gender), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.birth_date), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.birth_place), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.address), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.alternate_msisdn1), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.alternate_msisdn2), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.identification_type), '') IS NULL"
			+ "                    OR NULLIF(TRIM(c.identification_number), '') IS NULL"
			+ "                   ) THEN 1 ELSE 0 END"
			+ "       ) AS value" 
			+ " FROM service_providers sp"
			+ " INNER JOIN consumers c ON c.service_provider_id = sp.id AND c.is_consistent = 0"
			+ " WHERE (:providersIsNull = TRUE OR sp.name IN (:providers)) GROUP BY sp.name)"
			
			+ " UNION ALL" 
			+ " (SELECT CONCAT('Duplicate Records for ', sp.name) AS name,"
			+ " COALESCE(COUNT(d.msisdn), 0) AS value" 
			+ " FROM service_providers sp"
			+ " LEFT JOIN consumers c ON c.service_provider_id = sp.id" 
			+ " LEFT JOIN ("
			+ " SELECT service_provider_id, msisdn" 
			+ " FROM consumers"
			+ " WHERE NULLIF(TRIM(msisdn), '') IS NOT NULL AND is_consistent = 0 GROUP BY service_provider_id, msisdn"
			+ " HAVING COUNT(*) > 1 ) d ON d.service_provider_id = sp.id AND d.msisdn = c.msisdn"
			+ " WHERE (:providersIsNull = TRUE OR sp.name IN (:providers))"
			+ " GROUP BY sp.name )" 
			
			+ " UNION ALL"
			+ " (SELECT CONCAT('Exceeding Threshold for ', sp.name) AS name,"
			+ " COALESCE(COUNT(d.identification_number), 0) AS value FROM service_providers sp"
			+ " LEFT JOIN consumers c ON c.service_provider_id = sp.id" 
			+ " LEFT JOIN ( SELECT service_provider_id, identification_number,identification_type FROM consumers"
			+ " WHERE NULLIF(TRIM(identification_number), '') IS NOT NULL AND NULLIF(TRIM(identification_type), '') IS NOT NULL AND is_consistent = 0"
			+ " GROUP BY service_provider_id, identification_number ,identification_type"
			+ " HAVING COUNT(*) > 1 ) d ON d.service_provider_id = sp.id AND d.identification_number = c.identification_number AND d.identification_type = c.identification_type"
			+ " WHERE (:providersIsNull = TRUE OR sp.name IN (:providers))"
			+ " GROUP BY sp.name)", nativeQuery = true)
    List<DashboardObjectInterface> getAnomalyCountsByAnomalyTypes(
            @Param("providers") List<String> providers,
            @Param("providersIsNull") boolean providersIsNull
    );

    // ===== Added Optional-based helpers for upserts by msisdn =====

    /** Returns the first matched consumer by msisdn (msisdn should be unique). */
    Optional<Consumer> findFirstByMsisdn(String msisdn);

    /** Efficient lookup used to set id before merge so it becomes UPDATE instead of INSERT. */
    @Query("select c.id from Consumer c where c.msisdn = :msisdn")
    Optional<Long> findIdByMsisdn(@Param("msisdn") String msisdn);

    /** Quick existence check by business key. */
    boolean existsByMsisdn(String msisdn);
}
