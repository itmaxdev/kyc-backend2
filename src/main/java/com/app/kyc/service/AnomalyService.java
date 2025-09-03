package com.app.kyc.service;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.User;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.model.AnomlyDto;
import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.request.UpdateAnomalyStatusRequest;
import com.app.kyc.response.AnomalyDetailsResponseDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public interface AnomalyService
{

   public AnomlyDto getAnomalyById(Long id);

   public AnomalyDetailsResponseDTO getAnomalyByIdWithDetails(Long id);

   public Map<String, Object> getAllAnomalies(String params) throws JsonMappingException, JsonProcessingException;

   public List<Anomaly> getAnomaliesByAnomalyTypeId(Long id);

   public void addAnomaly(Anomaly anomaly);

   public Anomaly updateAnomaly(Anomaly anomaly);

   int countAnomaliesByAnomalyId(Long anomalyId);

   public List<Anomaly> getAnomalyByReportedOnGreaterThanAndReportedOnLessThanEqual(Long industryId, Date startDate, Date endDate);

   List<Anomaly> getAnomalyByIndustryId(Long industryId, Date start, Date end);

   public List<Anomaly> getAnomalyByServiceTypeId(Long id, Date startDate, Date endDate);

   public List<Anomaly> getAnomalyByServiceProviderId(Long id);

   public List<Anomaly> getAnomalyByServiceProviderAndServiceTypeId(Long id, Long id2, Date startDate, Date endDate);

   public Map<String, Object> getAllAnomaliesByServiceProvider(Long serviceProviderId, String params) throws JsonMappingException, JsonProcessingException;

   public void updateAnomaly(UpdateAnomalyStatusRequest updateAnomalyStatusRequest, User user);

   int getAverageResolutionTimeInHoursByServiceProvider(Long serviceProviderId, Date startDate, Date endDate);

   int getAverageResolutionTimeInHoursByServiceType(Long serviceTypeId, Date startDate, Date endDate);

   int getAverageResolutionTimeInHoursByServiceProviderAndServiceType(Long serviceProviderId, Long serviceTypeId, Date startDate, Date endDate);

   double getAverageResolutionTimeInHours(Long industryId, List<Long> serviceProviderIds , Date startDate, Date endDate);

   int getAnomaliesReportedByServiceProvidersAndDates(List<Long> serviceProviderIds, List<AnomalyStatus> statuses, Date startDate, Date endDate);

   int getAnomaliesReportedWithdrawnByServiceProvidersAndDates(List<Long> serviceProviderIds, List<AnomalyStatus> statuses, Date startDate, Date endDate);

   List<DashboardObjectInterface> getAnomaliesByServiceProviderAndStatusGroupByMonthYear(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd);
   
   List<DashboardObjectInterface> getAnomaliesByServiceProviderAndStatusGroupByDateMonthYear(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd);

   long countByStatusNotIn(List<AnomalyStatus> list);

   List<Object[]> countByAnomalyType();
   
   List<Object[]> getResolutionMetrics(Long industryId, List<Long> serviceProviderIds, Date startDate, Date endDate);
}
