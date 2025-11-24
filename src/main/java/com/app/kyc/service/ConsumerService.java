package com.app.kyc.service;

import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.User;
import com.app.kyc.model.ConsumerDto;
import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.response.ConsumersDetailsResponseDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.springframework.data.jpa.repository.Query;

import java.io.IOException;
import java.util.*;

public interface ConsumerService
{
   ConsumerDto getConsumerById(Long id);
    ConsumerDto getConsumerByMsisdnId(Long id);


   Map<String, Object> getAllConsumers(String params) throws JsonMappingException, JsonProcessingException;
    Map<String, Object> getAllConsumersforMsisdn(String params) throws JsonMappingException, JsonProcessingException;


   Map<String, Object> getAllFlaggedConsumers(String params);

   void addConsumer(Consumer consumer);

   Consumer updateConsumer(Consumer consumer);

   List<List<String>> loadConsumers(Long serviceProviderId, User user) throws IOException;

   int countConsumersByIndustryId(Long industryId, Date start, Date end);

   List<Consumer> getConsumersByServiceTypeId(Long serviceTypeId, Date startDate, Date endDate);

   List<Consumer> getConsumersByServiceProviderIdAndDateRange(Long serviceProviderId, Date startDate, Date endDate);

   List<Consumer> getConsumersByServiceProviderId(Long serviceProviderId);

   List<Consumer> getConsumersByServiceProviderIdAndServiceTypeId(Long serviceProviderId, Long serviceTypeId, Date startDate, Date endDate);

   Map<String, Object> getAllByServiceIdAndUserId(Long userId, Long serviceId);

   Map<String, Object> getAllFlaggedConsumers2(String params) throws JsonMappingException, JsonProcessingException;

   ConsumersDetailsResponseDTO getConsumerByIdwithSubscriptions(Long id);

   List<Consumer> getConsumersByCreatedOnGreaterThanAndCreatedOnLessThanEqual(Long industryId, Date start, Date end);

   List<DashboardObjectInterface> getAndCountConsumersGroupedByServiceProviderId(List<Long> serviceProvidersIdList, Date start, Date end);

   List<DashboardObjectInterface> getAndCountDistinctConsumersGroupedByServiceProviderId(List<Long> serviceProvidersIdList, Date start, Date end);

   public long countConsumersByServiceProvidersBetweenDates(Collection<Long> serviceProvidersIds, Date createdOnStart, Date createdOnEnd, boolean isConsistent, int consumerStatus);
   
   public long countSubscribersByServiceProvidersBetweenDates(Collection<Long> serviceProvidersIds, Date createdOnStart, Date createdOnEnd, int consumerStatus);

   long countDistinctConsumerByServiceProvidersBetweenDates(Collection<Long> serviceProvidersIds, Date createdOnStart, Date createdOnEnd);

   List<DashboardObjectInterface> getAndCountConsumersByServiceProviderBetweenDatesGroupByMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd);
   
   List<DashboardObjectInterface> getAndCountConsumersByServiceProviderBetweenDatesGroupByDateMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd);

   List<DashboardObjectInterface> getAndCountDistinctConsumersByServiceProviderBetweenDatesGroupByMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd, int consumerStatus);
   
   List<DashboardObjectInterface> getAndCountDistinctConsumersByServiceProviderBetweenDatesGroupByDateMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd, int consumerStatus);

  long getTotalConsumers(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd);

   List<Object[]> getConsumersPerOperator(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd);

   List<DashboardObjectInterface> buildAnomalyTypes(List<Long> serviceProviderIds, int i);
   
   List<DashboardObjectInterface> buildAnomalyTypes(List<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd);

    // List<Object[]> getConsumersPerOperatorBreakdown();*/
   
   List<Object[]> getConsumersbyServiceProvider(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd);

    @Query("""
    SELECT c.serviceProvider.name AS operator,
           SUM(CASE WHEN LOWER(c.status) = 'accepted' THEN 1 ELSE 0 END) AS consistentCount,
           SUM(CASE WHEN LOWER(c.status) = 'recycled' THEN 1 ELSE 0 END) AS inconsistentCount
    FROM Consumer c
    WHERE c.serviceProvider.id IN :serviceProviderIds
      AND c.createdOn BETWEEN :createdOnStart AND :createdOnEnd
    GROUP BY c.serviceProvider.name
""")
    /*List<Object[]> getConsumersbyServiceProvider(Collection<Long> serviceProviderIds,
                                                 Date createdOnStart,
                                                 Date createdOnEnd);*/

    Map<String, Object> getConsumersByServiceProvider(Long spId);


}
