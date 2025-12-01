package com.app.kyc.repository;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.app.kyc.entity.MsisdnTracking;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
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
public interface ConsumerRepository
        extends JpaRepository<Consumer, Long>, JpaSpecificationExecutor<Consumer> {

    @Query("SELECT c FROM Consumer c WHERE c.serviceProvider = :sp")
    List<Consumer> findByServiceProvider(@Param("sp") ServiceProvider serviceProvider);

    @Query(value = "select g.id, g.consumerId, g.name, g.serviceName, g.serviceProviderId, (select name from service_providers where id = g.serviceProviderId) as serviceProviderName,\r\n" +
            " g.flaggedDate, g.anomalyEntityType, g.anomalyStatus, g.note, g.anomalyId, CONCAT(u.first_name, \" \", u.last_name) as reporterName \r\n" +
            "from users as u join ( select (select id from consumers where id = consumer_id) as consumerId, (select CONCAT(first_name, \" \", last_name) \r\n" +
            " from consumers where id = consumer_id) as name, (select name from services where id = service_id) as serviceName ,\r\n" +
            " (select service_provider_id from services where id = service_id) as serviceProviderId,\r\n" +
            " b.* from consumers_services as a join (select consumers_services_id, reported_on as flaggedDate, reported_by_id, \r\n" +
            " (select target_entity_type from anomaly_types where id = anomaly_type_id) as anomalyEntityType, status as anomalyStatus, id as anomalyId, \r\n" +
            "   note, id from anomalies) as b on a.id = b.consumers_services_id) as g on u.id = reported_by_id", nativeQuery = true)
    List<FlaggedConsumersListDTO> getAllFlaggedConsumers();

    @Query(value = "SELECT id FROM anomalies WHERE anomaly_formatted_id LIKE CONCAT(:prefix, '%')", nativeQuery = true)
    List<Long> findIdsByFormattedIdPrefix(@Param("prefix") String prefix);



    // ANY status (used above). If you prefer active-only, add "AND c.consumer_status = 0".
    @Query(value = "SELECT * FROM consumers c WHERE c.service_provider_id = :spId AND TRIM(c.identification_type)   = TRIM(:type) AND TRIM(c.identification_number) = TRIM(:number)", nativeQuery = true)
    List<Consumer> findByIdKeyNormalizedAnyStatus(@Param("spId") Long spId,
                                                  @Param("type") String type,
                                                  @Param("number") String number);

    Optional<Consumer> findByVodacomTransactionId(String vodacomTransactionId);

    Optional<Consumer> findByAirtelTransactionId(String airtelTransactionId);




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

    List<Consumer> findByServiceProvider_Id(Long serviceProviderId);


    List<Consumer> findByServiceProviderId(Long serviceProviderId);

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
    long countByIsConsistentTrueAndStatus(String status);
    long countByIsConsistentFalseAndStatus(String status);

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

    List<Consumer> findAllByMsisdn(String msisdn);

    List<Consumer> findByMsisdnAndConsumerStatus(String msisdn, int consumerStatus);

    List<Consumer> findByMsisdnAndServiceProvider_Id(String msisdn, Long serviceProviderId);

    List<Consumer> findByIdentificationTypeAndIdentificationNumberAndServiceProvider_Id(
            String idType, String idNumber, Long serviceProviderId);

    long countByMsisdn(String msisdn);

    // Count how many consumers share the same ID + ID type
    long countByIdentificationNumberAndIdentificationType(String identificationNumber, String identificationType);


    @Query(value = "SELECT * FROM consumers c WHERE c.msisdn in (:msisdn) and c.consumer_status = :consumer_status and c.id not in(select consumer_id from consumers_anomalies);", nativeQuery = true)
    List<Consumer> findConsumersNotInCA(@Param("msisdn") String msisdn, @Param("consumer_status") int consumer_status);

    @Query(value = "select cc.id  from consumers cc where cc.msisdn in (:msisdn)  and  cc.consumer_status=:consumer_status", nativeQuery = true)
    List<Long> findConsumerIdsByMsisdnAndConsumerStatus(@Param("msisdn") String msisdn, @Param("consumer_status") int consumer_status);

    @Query(value = "select * from consumers cc where cc.msisdn in (:msisdn)  and  cc.consumer_status=:consumer_status and cc.identification_type = :id_type and cc.identification_number = :id_number and cc.service_provider_id = :sp_id", nativeQuery = true)
    List<Consumer> findConsumerIdsByMsisdnAndConsumerStatusAndIdNumberAndIdTypeAndServiceProviderID(@Param("msisdn") String msisdn, @Param("consumer_status") int consumer_status, @Param("id_type") String id_type, @Param("id_number") String id_number, @Param("sp_id") Long sp_id);

    @Query(value = "select * from consumers cc where cc.msisdn in (:msisdn) and cc.identification_type = :id_type and cc.identification_number = :id_number and cc.service_provider_id = :sp_id", nativeQuery = true)
    List<Consumer> findConsumerIdsByMsisdnAndIdNumberAndIdTypeAndServiceProviderID(@Param("msisdn") String msisdn, @Param("id_type") String id_type, @Param("id_number") String id_number, @Param("sp_id") Long sp_id);

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

    // Repository method
    List<Consumer> findByIdentificationTypeAndIdentificationNumberAndServiceProviderAndConsumerStatusIn(
            String identificationType,
            String identificationNumber,
            ServiceProvider serviceProvider,
            Collection<Integer> statuses);

    List<Consumer> findAllByServiceProvider_Id(Long id);

    @Transactional
    void deleteAllByIdIn(List<Long> ids);

    @Query("select count(distinct c.id) from Consumer c where c.serviceProvider.id in ?1 and c.registrationDate between ?2 and ?3 and c.consumerStatus = 0")
    long countDistinctByServiceProvider_IdInAndCreatedOnBetween(Collection<Long> ids, Date registrationDateStart, Date registrationDateEnd);

    @Query(value="select count(distinct(concat(c.identification_type,c.identification_number))) sum from consumers c where c.service_provider_id in :ids and c.registration_date between :createdOnStart and :createdOnEnd and is_consistent = :isConsistent and c.consumer_status = :consumerStatus",nativeQuery = true)
    long countConsumersByServiceProvider_IdInAndRegistrationDateBetweenAndIsConsistentAndConsumerStatus(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, Boolean isConsistent, int consumerStatus);

   /* @Query(value="select count(concat(c.id)) sum from consumers c where c.service_provider_id in :ids and c.created_on between :createdOnStart and :createdOnEnd and is_consistent = :isConsistent and status = :status",nativeQuery = true)
    long countConsumersByServiceProvider_IdInAndCreatedonDateBetweenAndIsConsistentAndStatus(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, Boolean isConsistent,String status);
*/
    @Query(value="""
    SELECT COUNT(c.id)
    FROM consumers c
    WHERE c.service_provider_id IN :ids
      AND STR_TO_DATE(c.created_on, '%Y-%m-%d %H:%i:%s')
            BETWEEN :createdOnStart AND :createdOnEnd
      AND c.is_consistent = :isConsistent
      AND c.`status` = :status
""", nativeQuery = true)
    long countConsumersByServiceProvider_IdInAndCreatedonDateBetweenAndIsConsistentAndStatus(
            @Param("ids") Collection<Long> ids,
            @Param("createdOnStart") Date start,
            @Param("createdOnEnd") Date end,
            @Param("isConsistent") int isConsistent,
            @Param("status") String status);


    @Query(value="select count(*) sum from consumers c where c.service_provider_id in :ids and c.registration_date between :createdOnStart and :createdOnEnd and c.consumer_status = :consumerStatus",nativeQuery = true)
    long countSubscribersByServiceProvider_IdInAndRegistrationDateBetweenAndConsumerStatus(Collection<Long> ids, Date createdOnStart, Date createdOnEnd, int consumerStatus);

    @Query(value = """
    SELECT COUNT(*)
    FROM consumers c
    WHERE c.service_provider_id IN (:serviceProviderIds)
      AND STR_TO_DATE(c.created_on, '%Y-%m-%d %H:%i:%s')
            BETWEEN :startDate AND :endDate
      AND (
            (:isConsistent = true  AND LOWER(c.status) = 'accepted')
         OR (:isConsistent = false AND LOWER(c.status) = 'recycled')
      )
""", nativeQuery = true)
    long countConsumersByServiceProviderBetweenDatesAndStatus(
            @Param("serviceProviderIds") Collection<Long> serviceProviderIds,
            @Param("startDate") Date startDate,
            @Param("endDate") Date endDate,
            @Param("isConsistent") boolean isConsistent);


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

    @Query(value = """
    SELECT COUNT(*) 
    FROM consumers c
    WHERE c.service_provider_id IN (?1)
      AND DATE(STR_TO_DATE(c.created_on, '%Y-%m-%d %H:%i:%s'))
            BETWEEN DATE(?2) AND DATE(?3)
      AND c.`status` = ?4
""", nativeQuery = true)
    long getTotalConsumersAndStatus(
            Collection<Long> ids,
            Date createdOnStart,
            Date createdOnEnd,
            String status);

    @Query(value = "SELECT sp.name AS operatorName, COUNT(c.id) AS total " +
            "FROM consumers c " +
            "JOIN service_providers sp ON c.service_provider_id = sp.id " +
            "where c.service_provider_id IN (?1) " +
            "AND STR_TO_DATE(c.created_on, '%Y-%m-%d') " +
    		"BETWEEN STR_TO_DATE(?2, '%Y-%m-%d') " +
    		"AND STR_TO_DATE(?3, '%Y-%m-%d') " +
            "GROUP BY sp.name ", nativeQuery = true)
    List<Object[]> getConsumersPerOperator(Collection<Long> ids, Date createdOnStart, Date createdOnEnd);

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
    
    
	@Query(" SELECT at.name AS name, COUNT(a.id) AS value"
			+ " FROM Anomaly a JOIN a.anomalyType at"
			+ " WHERE a.reportedOn > ?3 AND a.reportedOn <= ?4"
			+ " AND EXISTS (SELECT 1 FROM a.consumers ca WHERE (?2 = TRUE OR ca.serviceProvider.name IN (?1)))"
			+ " GROUP BY at.name ORDER BY value DESC")
    List<DashboardObjectInterface> getAnomalyCountsByAnomalyTypes(List<String> providers,boolean providersIsNull,Date createdOnStart, Date createdOnEnd);

    // ===== Added Optional-based helpers for upserts by msisdn =====

    /** Returns the first matched consumer by msisdn (msisdn should be unique). */
    Optional<Consumer> findFirstByMsisdn(String msisdn);

    /** Efficient lookup used to set id before merge so it becomes UPDATE instead of INSERT. */
    @Query("select c.id from Consumer c where c.msisdn = :msisdn")
    Optional<Long> findIdByMsisdn(@Param("msisdn") String msisdn);

    /** Quick existence check by business key. */
    boolean existsByMsisdn(String msisdn);

    long countByIsConsistentTrueAndServiceProvider_Id(Long serviceProviderId);
    long countByIsConsistentTrueAndServiceProvider_IdAndStatus(Long serviceProviderId,String status);
    long countByIsConsistentFalseAndServiceProvider_Id(Long serviceProviderId);
    long countByIsConsistentFalseAndServiceProvider_IdAndStatus(Long serviceProviderId,String status);

    long countByIsConsistentAndServiceProvider_IdAndStatus(Boolean isConsistent, Long serviceProviderId, String status);



    @Query(value = "SELECT COUNT(*) FROM consumers "
            + "WHERE is_consistent = 1 AND service_provider_id = :spId AND status = :status",
            nativeQuery = true)
    long countConsistentNative(@Param("spId") Long spId, @Param("status") String status);

    @Query(value = "SELECT COUNT(*) FROM consumers "
            + "WHERE is_consistent = 0 AND service_provider_id = :spId AND status = :status",
            nativeQuery = true)
    long countInconsistentNative(@Param("spId") Long spId, @Param("status") String status);


    @Query(value = "SELECT COUNT(*) FROM consumers "
            + "WHERE is_consistent = 1 AND status = :status",
            nativeQuery = true)
    long countConsistentNativeWithOutSP( @Param("status") String status);

    @Query(value = "SELECT COUNT(*) FROM consumers "
            + "WHERE is_consistent = 0  AND status = :status",
            nativeQuery = true)
    long countInconsistentNativeWithOutSP(@Param("status") String status);
    @Query(value = "SELECT id, is_consistent FROM consumers "
            + "WHERE service_provider_id = :spId AND status = :status",
            nativeQuery = true)
    List<Object[]> debugConsistency(@Param("spId") Long spId, @Param("status") String status);

    Page<Consumer> findByIsConsistentTrueAndConsumerStatusIn(
            Pageable pageable, Collection<Integer> statuses);

    Page<Consumer> findByIsConsistentTrueAndConsumerStatusInAndServiceProvider_Id(
            Pageable pageable, Collection<Integer> statuses, Long serviceProviderId);


    Page<Consumer> findByIsConsistentFalseAndConsumerStatusIn(
            Pageable pageable, Collection<Integer> statuses);

    Page<Consumer> findByIsConsistentFalseAndConsumerStatusInAndServiceProvider_Id(
            Pageable pageable, Collection<Integer> statuses, Long serviceProviderId);



    Optional<Consumer> findByFirstNameAndLastNameAndIdentificationNumberAndIdentificationType(
            String firstName, String lastName, String idNumber, String idType);

    Optional<Consumer> findByMsisdnAndLastNameAndIdentificationNumberAndIdentificationType(
            String msisdn, String lastName, String idNumber, String idType);

    Optional<Consumer> findByFirstNameAndMsisdnAndIdentificationNumberAndIdentificationType(
            String firstName, String msisdn, String idNumber, String idType);

    Optional<Consumer> findByMsisdnAndLastNameAndFirstNameAndIdentificationType(
            String msisdn, String lastName, String firstName, String idType);

    Optional<Consumer> findByFirstNameAndLastNameAndIdentificationNumberAndMsisdn(
            String firstName, String lastName, String idNumber, String msisdn);
    
    @Query(value = "SELECT "
    		+ "    service_provider_id,"
    		+ "    sp.name as NAME,"
    		+ "    SUM(CASE WHEN is_consistent = 1 THEN 1 ELSE 0 END) AS consistent_count,"
    		+ "    SUM(CASE WHEN is_consistent = 0 THEN 1 ELSE 0 END) AS non_consistent_count "
    		+ "FROM consumers c "
    		+ "join service_providers sp on c.service_provider_id = sp.id "
    		+ "where c.service_provider_id IN (?1) "
    		+ "AND STR_TO_DATE(c.created_on, '%Y-%m-%d') "
    		+ "BETWEEN STR_TO_DATE(?2, '%Y-%m-%d') AND STR_TO_DATE(?3, '%Y-%m-%d') "
    		+ "GROUP BY c.service_provider_id ", nativeQuery = true)
    List<Object[]> getConsumersByServiceProvider(Collection<Long> ids, Date createdOnStart, Date createdOnEnd);


    Optional<Consumer> findByOrangeTransactionId(String orangeTransactionId);

    @Modifying
    @Transactional
    @Query(value = """
            LOAD DATA LOCAL INFILE :filePath
            INTO TABLE consumers
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ';' 
            OPTIONALLY ENCLOSED BY '"' 
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 ROWS
            (
                @numero_contrat,
                @msisdn,
                @date_creation,
                @nom,
                @prenom,
                @genre,
                @date_naissance,
                @lieu_naissance,
                @adresse,
                @type_piece,
                @numero_piece,
                @etat
            )
            SET
                service_provider_id = 20,  -- Orange
                orange_transaction_id     = LEFT(NULLIF(TRIM(@numero_contrat), ''), 50),

                first_name          = LEFT(NULLIF(TRIM(@prenom), ''), 100),
                last_name           = LEFT(NULLIF(TRIM(@nom), ''), 100),
                gender              = NULLIF(TRIM(@genre), ''),

                birth_date = CASE
                                WHEN @date_naissance REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
                                THEN STR_TO_DATE(@date_naissance, '%Y/%m/%d')
                                ELSE NULL
                             END,

                registration_date = CASE
                                        WHEN @date_creation REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
                                        THEN STR_TO_DATE(@date_creation, '%Y/%m/%d %H:%i')
                                        ELSE NULL
                                    END,

                birth_place         = LEFT(NULLIF(TRIM(@lieu_naissance), ''), 255),
                address             = LEFT(NULLIF(TRIM(@adresse), ''), 255),
                identification_type   = LEFT(NULLIF(TRIM(@type_piece), ''), 100),
                identification_number = LEFT(NULLIF(TRIM(@numero_piece), ''), 50),
                
                msisdn              = LEFT(NULLIF(TRIM(@msisdn), ''), 20),

                status = CASE 
                           WHEN LOWER(TRIM(@etat)) IN ('active','actif','accepted','1','true')
                                THEN 'accepted'
                           ELSE 'recycled'
                         END,

          
                consumer_status = CASE 
                                    WHEN LOWER(TRIM(@etat)) IN ('active','actif','accepted','1','true')
                                        THEN 1 
                                    ELSE 0 
                                  END,

                is_consistent = CASE
                                 WHEN LOWER(TRIM(@etat)) IN ('active','actif','accepted','1','true')
                                     THEN TRUE
                                 ELSE FALSE
                               END,

                created_on          = NOW();
        """, nativeQuery = true)
    void loadOrangeCsv(@Param("filePath") String filePath);


    @Modifying
    @Transactional
    @Query(value = """
    UPDATE consumers c
    LEFT JOIN (
        SELECT msisdn, COUNT(*) AS cnt
        FROM consumers
        WHERE msisdn IS NOT NULL AND msisdn <> ''
        GROUP BY msisdn
    ) msd ON msd.msisdn = c.msisdn
    LEFT JOIN (
        SELECT identification_number, identification_type, COUNT(*) AS cnt
        FROM consumers
        WHERE identification_number IS NOT NULL AND identification_number <> ''
          AND identification_type IS NOT NULL AND identification_type <> ''
        GROUP BY identification_number, identification_type
    ) idd ON idd.identification_number = c.identification_number 
         AND idd.identification_type = c.identification_type
    SET 
        -- MANDATORY FIELD CHECKS
        c.is_consistent = CASE
            WHEN c.msisdn IS NULL OR c.msisdn = '' THEN FALSE
            WHEN c.registration_date IS NULL THEN FALSE
            WHEN c.first_name IS NULL OR c.first_name = '' THEN FALSE
            WHEN c.last_name IS NULL OR c.last_name = '' THEN FALSE
            WHEN c.middle_name IS NULL OR c.middle_name = '' THEN FALSE
            WHEN c.gender IS NULL OR c.gender = '' THEN FALSE
            WHEN c.birth_date IS NULL THEN FALSE
            WHEN c.birth_place IS NULL OR c.birth_place = '' THEN FALSE
            WHEN c.address IS NULL OR c.address = '' THEN FALSE
            WHEN c.identification_type IS NULL OR c.identification_type = '' THEN FALSE
            WHEN c.identification_number IS NULL OR c.identification_number = '' THEN FALSE
            WHEN c.alternate_msisdn1 IS NULL OR c.alternate_msisdn1 = '' THEN FALSE
            WHEN c.alternate_msisdn2 IS NULL OR c.alternate_msisdn2 = '' THEN FALSE

            -- DUPLICATE MSISDN
            WHEN msd.cnt > 1 THEN FALSE

            -- DUPLICATE ID TYPE + ID NUMBER
            WHEN idd.cnt > 2 THEN FALSE

            ELSE TRUE
        END,

        c.consistent_on = CASE
            WHEN c.is_consistent = TRUE 
                THEN IF(c.consistent_on IS NULL OR c.consistent_on = 'N/A', CURDATE(), c.consistent_on)
            ELSE 'N/A'
        END;
""", nativeQuery = true)
    void bulkUpdateConsistency();


    @Modifying
    @Transactional
    @Query(value = """
    LOAD DATA LOCAL INFILE :filePath
    IGNORE
    INTO TABLE consumers
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ',' 
    OPTIONALLY ENCLOSED BY '"' 
    LINES TERMINATED BY '\\r\\n'
    IGNORE 1 ROWS
    (
        @msisdn,
        @date_enr,
        @firstname,
        @middlename,
        @lastname,
        @gender,
        @date_naissance,
        @lieu_de_naissance,
        @address_no,
        @street_name,
        @district_name,
        @commune_name,
        @city_town,
        @emergency_contact_1,
        @emergency_contact_2,
        @card_type,
        @card_id,
        @trx_id,
        @status
    )
    SET
        service_provider_id = 24,
        vodacom_transaction_id = LEFT(NULLIF(TRIM(@trx_id), ''), 64),
        msisdn              = LEFT(NULLIF(TRIM(@msisdn), ''), 20),
        first_name          = LEFT(NULLIF(TRIM(@firstname), ''), 100),
        middle_name         = LEFT(NULLIF(TRIM(@middlename), ''), 100),
        last_name           = LEFT(NULLIF(TRIM(@lastname), ''), 100),
        gender              = NULLIF(TRIM(@gender), ''),
        birth_date          = CASE 
                                WHEN @date_naissance REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
                                  THEN STR_TO_DATE(@date_naissance, '%m/%d/%Y')
                                WHEN @date_naissance REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' 
                                  THEN STR_TO_DATE(@date_naissance, '%d-%m-%Y')
                                ELSE NULL
                              END,
        registration_date   = CASE
                                WHEN @date_enr REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
                                  THEN STR_TO_DATE(@date_enr, '%m/%d/%Y')
                                WHEN @date_enr REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' 
                                  THEN STR_TO_DATE(@date_enr, '%d-%m-%Y')
                                ELSE NULL
                              END,
        birth_place         = LEFT(NULLIF(TRIM(@lieu_de_naissance), ''), 255),
        address             = CONCAT_WS(', ',
                                NULLIF(TRIM(@address_no), ''),
                                NULLIF(TRIM(@street_name), ''),
                                NULLIF(TRIM(@district_name), ''),
                                NULLIF(TRIM(@commune_name), ''),
                                NULLIF(TRIM(@city_town), '')
                              ),
        alternate_msisdn1   = LEFT(NULLIF(TRIM(@emergency_contact_1), ''), 20),
        alternate_msisdn2   = LEFT(NULLIF(TRIM(@emergency_contact_2), ''), 20),
        identification_type = LEFT(NULLIF(TRIM(@card_type), ''), 100),
        identification_number = LEFT(NULLIF(TRIM(@card_id), ''), 50),
        status               = LEFT(NULLIF(TRIM(@status), ''), 20),
        consumer_status     = CASE
                                WHEN LOWER(TRIM(@status)) IN ('accepted', 'actif', 'active', '1') THEN 1
                                WHEN LOWER(TRIM(@status)) IN ('rejected', 'resilie', 'inactive', '0', 'suspendu') THEN 0
                                ELSE 0
                              END,
        created_on          = NOW(),
        is_consistent       = FALSE;
""", nativeQuery = true)
    void loadVodacomCsv(@Param("filePath") String filePath);


    @Modifying
    @Transactional
    @Query(value = """
LOAD DATA LOCAL INFILE :filePath
REPLACE INTO TABLE consumers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS
(
    @subscriberdetailsid,
    @msisdn,
    @firstname,
    @middlename,
    @lastname,
    @dateofbirth,
    @placeofbirth,
    @nationality,
    @gender,
    @addressid,
    @idtype,
    @subscriberid,
    @permanentaddress,
    @idnumber,
    @kycstatus,
    @alternateno,
    @alternateno2,
    @finalstatus,
    @createdon,
    @updatedon,
    @ISOLDUSERDETAILS,
    @junk1,
    @junk2,
    @junk3,
    @junk4,
    @junk5
)
SET
    service_provider_id = 27,

    airtel_transaction_id = LEFT(NULLIF(TRIM(@subscriberdetailsid), ''), 64),

    /* ------------------------- MSISDN FIX ------------------------- */
    msisdn = CASE 
                WHEN @msisdn REGEXP '^[0-9]+(\\.[0-9]+)?E[+-]?[0-9]+$'
                    THEN FORMAT(@msisdn, 0)
                ELSE LEFT(NULLIF(TRIM(@msisdn), ''), 20)
             END,

    first_name      = LEFT(NULLIF(TRIM(@firstname), ''), 100),
    middle_name     = LEFT(NULLIF(TRIM(@middlename), ''), 100),
    last_name       = LEFT(NULLIF(TRIM(@lastname), ''), 100),

    /* ----------------------- BIRTH DATE FIX ----------------------- */
    birth_date = CASE
                    WHEN REGEXP_REPLACE(@dateofbirth, '[^0-9]', '') REGEXP '^[0-9]{8}$' THEN
                        CASE
                            /* Format 19880916 → YYYY-MM-DD */
                            WHEN REGEXP_REPLACE(@dateofbirth, '[^0-9]', '') BETWEEN '10000000' AND '99999999'
                                AND SUBSTRING(REGEXP_REPLACE(@dateofbirth, '[^0-9]', ''),1,4) BETWEEN '1900' AND '2100'
                                THEN STR_TO_DATE(REGEXP_REPLACE(@dateofbirth, '[^0-9]', ''), '%Y%m%d')

                            /* Format 16091988 → DDMMYYYY */
                            ELSE STR_TO_DATE(REGEXP_REPLACE(@dateofbirth, '[^0-9]', ''), '%d%m%Y')
                        END
                    ELSE NULL
                 END,

    birth_place     = LEFT(NULLIF(TRIM(@placeofbirth), ''), 255),
    nationality     = LEFT(NULLIF(TRIM(@nationality), ''), 100),
    gender          = LEFT(NULLIF(TRIM(@gender), ''), 20),

    /* ----------------------- ADDRESS FIX -------------------------- */
    address = CASE 
                WHEN @permanentaddress IS NULL OR TRIM(@permanentaddress) = '' 
                    THEN LEFT(NULLIF(TRIM(@addressid), ''), 255)
                ELSE LEFT(TRIM(@permanentaddress), 255)
              END,

    identification_type   = LEFT(NULLIF(TRIM(@idtype), ''), 100),
    identification_number = LEFT(NULLIF(TRIM(@idnumber), ''), 50),

    alternate_msisdn1 = LEFT(NULLIF(TRIM(@alternateno), ''), 20),
    alternate_msisdn2 = LEFT(NULLIF(TRIM(@alternateno2), ''), 20),

    /* ------------------ RAW STATUS FROM CSV ----------------------- */
    status = LOWER(TRIM(@ISOLDUSERDETAILS)),

    /* ------------------ CONSUMER STATUS LOGIC --------------------- */
    consumer_status = CASE
                        WHEN LOWER(TRIM(@ISOLDUSERDETAILS)) IN ('completed','false','1') 
                            THEN 1     -- accepted
                        WHEN LOWER(TRIM(@ISOLDUSERDETAILS)) IN ('rejected','true','0','resilie','suspendu')
                            THEN 0     -- recycled
                        ELSE 0
                      END,

    /* ---------------------- REGISTRATION DATE --------------------- */
    registration_date = CURDATE(),

    created_on = NOW(),
    is_consistent = FALSE;
""", nativeQuery = true)
    void loadAirtelCsv(@Param("filePath") String filePath);




    @Query(value = "SELECT COUNT(*) FROM consumers WHERE service_provider_id = :spId", nativeQuery = true)
    long countPreviousConsumers(@Param("spId") Long spId);

    long countByStatus(String status);
    long countByStatusAndServiceProvider_Id(String status, Long spId);

    @Query("SELECT c.msisdn FROM Consumer c WHERE c.id = :id")
    String findRawMsisdnById(@Param("id") Long id);




}
