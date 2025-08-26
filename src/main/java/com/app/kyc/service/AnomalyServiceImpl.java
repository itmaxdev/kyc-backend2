package com.app.kyc.service;

import java.util.*;
import java.util.stream.Collectors;

import com.app.kyc.entity.*;
import com.app.kyc.model.*;
import com.app.kyc.repository.ConsumerAnomalyRepository;
import com.app.kyc.response.AnomalyHasSubscriptionsResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.stereotype.Service;

// import com.app.kyc.entity.ConsumerService;
import com.app.kyc.repository.AnomalyRepository;
import com.app.kyc.repository.AnomalyTrackingRepository;
import com.app.kyc.repository.ConsumerRepository;
import com.app.kyc.request.UpdateAnomalyStatusRequest;
import com.app.kyc.response.AnomalyDetailsResponseDTO;
import com.app.kyc.util.PaginationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

@Service
public class AnomalyServiceImpl implements AnomalyService
{

   @Autowired
   private AnomalyRepository anomalyRepository;

   @Autowired
   private ConsumerRepository consumerRepository;

   @Autowired
   private AnomalyTrackingRepository anomalyTrackingRepository;

   @Autowired
   private NotificationService notificationService;
   
   @Autowired
   private UserService userService;

   @Autowired
   private ConsumerAnomalyRepository consumerAnomalyRepository;


   public AnomlyDto getAnomalyById(Long id)
   {
      Anomaly anomaly=anomalyRepository.findById(id).get();
      AnomlyDto anomlyDto= new AnomlyDto(anomaly);
      return anomlyDto;
   }

   @Override
   public Map<String, Object> getAllAnomalies(String params) throws JsonMappingException, JsonProcessingException
   {
      List<AnomlyDto> pageAnomalies = anomalyRepository.findAll(PaginationUtil.getPageable(params))
              .stream()
              .filter(c-> c.getStatus().getCode() != 5 && c.getStatus().getCode() != 6)
              .map(c->new AnomlyDto(c)).collect(Collectors.toList());

      List<AnomalyHasSubscriptionsResponseDTO> anomalyHasSubscriptionsResponseDTO = new ArrayList<>();
      for(AnomlyDto a : pageAnomalies)
      {
         int countSucscriptions = this.countAnomaliesByAnomalyId(a.getId());
         anomalyHasSubscriptionsResponseDTO.add(new AnomalyHasSubscriptionsResponseDTO(a, countSucscriptions > 0 ? true : false));
      }
      Map<String, Object> anomaliesWithCount = new HashMap<String, Object>();
      anomaliesWithCount.put("data", anomalyHasSubscriptionsResponseDTO);
      anomaliesWithCount.put("count", new PageImpl<>(pageAnomalies).getTotalElements());
      return anomaliesWithCount;
   }

   @Override
   public void addAnomaly(Anomaly anomaly)
   {
      anomaly.setReportedOn(new Date());
      anomaly.setStatus(AnomalyStatus.REPORTED);
      anomalyRepository.save(anomaly);
      
      List<Consumer> consumers = consumerRepository.getAllByAnomalies(anomaly);
      Consumer consumer = consumers.get(0);
      List<User> spUsers = userService.getByServiceProviderId(consumer.getServiceProvider().getId());
      
      for (User u : spUsers) {
         List<String> consumerNames = new ArrayList<String>();
         for(Consumer c : consumers){
            consumerNames.add(c.getFirstName() + " " + c.getLastName());
         }
         String notificationMessage = "A new anomaly has been reported for " + consumer.getFirstName() + " " + consumer.getLastName();
         notificationService.addNotification(notificationMessage, u, NotificationType.ANOMALY_REPORTED, anomaly.getId());
      }
   }

   @Override
   public Anomaly updateAnomaly(Anomaly anomaly)
   {
      return anomalyRepository.save(anomaly);
   }

   @Override
   public int countAnomaliesByAnomalyId(Long anomalyId) {
      return (int) anomalyRepository.countById(anomalyId);
   }

   @Override
   public List<Anomaly> getAnomaliesByAnomalyTypeId(Long id)
   {
      return anomalyRepository.findByAnomalyType(id);
   }

   @Override
   public List<Anomaly> getAnomalyByIndustryId(Long industryId, Date start, Date end)
   {
      return anomalyRepository.findAllByIndustryIdAndReportedOnGreaterThanAndReportedOnLessThanEqual(industryId, start, end);
   }

   @Override
   public List<Anomaly> getAnomalyByReportedOnGreaterThanAndReportedOnLessThanEqual(Long industryId, Date startDate, Date endDate)
   {
      return anomalyRepository.findAllByIndustryIdAndReportedOnGreaterThanAndReportedOnLessThanEqual(industryId, startDate, endDate);
   }

   @Override
   public List<Anomaly> getAnomalyByServiceTypeId(Long serviceTypeId, Date startDate, Date endDate)
   {
      return anomalyRepository.findAllAnomalyByServiceTypeIdAndReportedOnGreaterThanAndReportedOnLessThanEqual(serviceTypeId, startDate, endDate);
   }

   @Override
   public List<Anomaly> getAnomalyByServiceProviderId(Long serviceProviderId)
   {
      return anomalyRepository.findAllAnomalyByServiceProviderId(serviceProviderId, null).toList();
   }

   @Override
   public List<Anomaly> getAnomalyByServiceProviderAndServiceTypeId(Long serviceProviderId, Long serviceTypeId, Date startDate, Date endDate)
   {
      return anomalyRepository.findAllAnomalyByServiceProviderAndServiceTypeIdAndReportedOnGreaterThanAndReportedOnLessThanEqual(serviceProviderId, serviceTypeId, startDate,
         endDate);
   }

   @Override
   public Map<String, Object> getAllAnomaliesByServiceProvider(Long serviceProviderId, String params) throws JsonMappingException, JsonProcessingException
   {
      // Page<Anomaly> anomalies = anomalyRepository.findAllAnomalyByServiceProviderId(serviceProviderId, PaginationUtil.getPageable(params));
      Page<Anomaly> anomalies = anomalyRepository.findDistinctByConsumers_ServiceProviderId(serviceProviderId, PaginationUtil.getPageable(params));
      List<AnomlyDto> data = anomalies
              .filter(a -> (a.getStatus().getCode() != 5 && a.getStatus().getCode() != 6) )
              .stream()
              .map(c->new AnomlyDto(c)).collect(Collectors.toList());

      List<AnomlyDto> updateData = data.stream().filter(a -> a.getConsumers().size() != 0).collect(Collectors.toList());


      updateData.forEach(anomlyDto -> {
         anomlyDto.getConsumers().forEach(c -> {
            List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(anomlyDto.getId(), c.getId());
            temp.forEach(t -> {
               anomlyDto.setUpdatedOn(new Date());
               if (Objects.nonNull(t.getNotes())) {
                  c.setNotes(t.getNotes());
               }
               if (Objects.nonNull(t.getAnomaly().getReportedBy().getFirstName()) || Objects.nonNull(t.getAnomaly().getReportedBy().getLastName())) {
                  anomlyDto.setUpdateBy(t.getAnomaly().getReportedBy().getFirstName() + " " + t.getAnomaly().getReportedBy().getLastName());
               }
            });
         });
      });
      Map<String, Object> anomaliesWithCount = new HashMap<String, Object>();
      anomaliesWithCount.put("data", updateData);
      anomaliesWithCount.put("count", anomalies.getTotalElements());
      return anomaliesWithCount;
   }

   @Override
   public void updateAnomaly(UpdateAnomalyStatusRequest updateAnomalyStatusRequest, User user)
   {
      Anomaly anomaly = anomalyRepository.findById(updateAnomalyStatusRequest.getAnomalyId()).get();
      AnomalyTracking anomalyTracking = new AnomalyTracking(anomaly, new Date(), updateAnomalyStatusRequest.getStatus(), updateAnomalyStatusRequest.getNote(), user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
      anomalyTrackingRepository.save(anomalyTracking);

      anomaly.setStatus(updateAnomalyStatusRequest.getStatus());
      anomaly.setUpdatedOn(new Date());
      anomalyRepository.save(anomaly);
      List<Consumer> consumers = consumerRepository.getAllByAnomalies(anomaly);
      Consumer consumer = consumers.get(0);
      String message = "";
      boolean spUsrCheck = false;
      if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.WITHDRAWN)){
         message = "The anomaly for " + consumer.getFirstName() + " "  + consumer.getLastName() + " has been marked as withdrawn";
         spUsrCheck = true;

      }
      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.RESOLVED_SUCCESSFULLY)){
         message = "The anomaly for " + consumer.getFirstName() + " "  + consumer.getLastName() + " has been marked as resolved";
         spUsrCheck = true;
      }
      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.QUESTION_ANSWERED)){
         message = "Question has been answered for anomaly " + consumer.getFirstName() + " "  + consumer.getLastName();
         spUsrCheck = true;

      }
      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.QUESTION_SUBMITTED)){
         message = consumer.getServiceProvider().getName() + " has raised a question for anomaly reported on " + consumer.getFirstName() + " " + consumer.getLastName();
      }
      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.RESOLUTION_SUBMITTED)){
         message = consumer.getServiceProvider().getName() + " has raised a resolution for anomaly reported on " + consumer.getFirstName() + " " + consumer.getLastName();
      }
      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.UNDER_INVESTIGATION)){
         message = consumer.getServiceProvider().getName() + " has raised a under investigation for anomaly reported on " +consumer.getFirstName() + " " + consumer.getLastName();
      }
      if(spUsrCheck){
         List<User> spUsers = userService.getByServiceProviderId(consumer.getServiceProvider().getId());
         
         for (User u : spUsers) {
            notificationService.addNotification(message, u, NotificationType.ANOMALY_REPORTED, anomaly.getId());
         }
      }
      else{
         notificationService.addNotification(message,anomaly.getReportedBy(), NotificationType.ANOMALY_REPORTED, anomaly.getId());
      }
   }

   @Override
   public AnomalyDetailsResponseDTO getAnomalyByIdWithDetails(Long id)
   {
      Anomaly anomaly=anomalyRepository.findById(id).get();
      AnomlyDto anomlyDto;
      if(anomaly.getStatus().getCode() == 5){
         anomlyDto= new AnomlyDto(anomaly, 0);
      }
      else{
         anomlyDto= new AnomlyDto(anomaly);
      }

      List<AnomalyTrackingDto> anomalyTracking = anomalyTrackingRepository.findAllByAnomalyId(id)
              .stream()
              .map(c-> new AnomalyTrackingDto(c.getId(),c.getCreatedOn(),c.getStatus(),c.getNote(),c.getAnomaly(), c.getUpdateBy(), c.getUpdateOn()))
              .collect(Collectors.toList());



      anomlyDto.getConsumers().forEach(c -> {
         if(Objects.isNull(c.getFirstName()))
            c.setFirstName("");
         if(Objects.isNull(c.getLastName()))
            c.setLastName("");
         List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(anomlyDto.getId(), c.getId());
            temp.forEach(t -> {
               if (Objects.nonNull(t.getNotes())) {
                  c.setNotes(t.getNotes());
               }
            });
         });

      anomalyTracking.forEach(a -> {
         if (a.getAnomlyDto().getAnomalyType().getId() == 1){
            a.getAnomlyDto().getConsumers().forEach(c ->{
               List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(a.getId(), c.getId());
               temp.forEach(t -> {
                  if (Objects.nonNull(t.getNotes())){
                     c.setNotes(t.getNotes());
                  }
               });
            });
         }
      });

//      AnomalyDetailsResponseDTO response = new AnomalyDetailsResponseDTO(anomlyDto, anomalyTracking);
//      List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByAnomaly_Id(anomlyDto.getId());
//      temp.forEach(t -> {
//         response.getAnomaly().getConsumers().get(0).setNotes(t.getNotes());
//      });

      return new AnomalyDetailsResponseDTO(anomlyDto, anomalyTracking);
   }

   @Override
   public double getAverageResolutionTimeInHours(Long industryId, Date startDate, Date endDate)
   {
      return anomalyRepository.getAverageResolutionTimeInHours(industryId, startDate, endDate);
   }

   @Override
   public int getAnomaliesReportedByServiceProvidersAndDates(List<Long> serviceProviderIds, List<AnomalyStatus> statuses, Date startDate, Date endDate) {
      //return (int) anomalyRepository.countDistinctByConsumers_ServiceProvider_IdInAndStatusInAndReportedOnBetween(serviceProviderIds ,statuses ,startDate, endDate);
      return (int) anomalyRepository.countDistinctMsisdns(serviceProviderIds ,statuses ,startDate, endDate);
   }

   @Override
   public int getAnomaliesReportedWithdrawnByServiceProvidersAndDates(List<Long> serviceProviderIds, List<AnomalyStatus> statuses, Date startDate, Date endDate) {
      return (int) anomalyRepository.countDistinctByConsumers_Withdrawn_ServiceProvider_IdInAndStatusInAndReportedOnBetween(serviceProviderIds ,statuses ,startDate, endDate);
   }

   @Override
   public List<DashboardObjectInterface> getAnomaliesByServiceProviderAndStatusGroupByMonthYear(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd) {
      return anomalyRepository.countDistinctByConsumers_ServiceProvider_IdInAndStatusInAndReportedOnBetweenDateGroupByYearMonth(ids,statuses, reportedOnStart,reportedOnEnd);
   }

   @Override
   public List<DashboardObjectInterface> getAnomaliesByServiceProviderAndStatusGroupByDateMonthYear(Collection<Long> ids, Collection<AnomalyStatus> statuses, Date reportedOnStart, Date reportedOnEnd) {
      return anomalyRepository.countDistinctByConsumers_ServiceProvider_IdInAndStatusInAndReportedOnBetweenDateGroupByYearMonthDate(ids,statuses, reportedOnStart,reportedOnEnd);
   }

   @Override
   public long countByStatusNotIn(List<AnomalyStatus> list){
      return anomalyRepository.countByStatusNotIn(list);
   }

   @Override
   public List<Object[]> countByAnomalyType() {
      return null;
   }

   @Override
   public int getAverageResolutionTimeInHoursByServiceProvider(Long serviceProviderId, Date startDate, Date endDate)
   {
      return anomalyRepository.getAverageResolutionTimeInHoursByServiceProvider(serviceProviderId, startDate, endDate);
   }

   @Override
   public int getAverageResolutionTimeInHoursByServiceType(Long serviceTypeId, Date startDate, Date endDate)
   {
      return anomalyRepository.getAverageResolutionTimeInHoursByServiceType(serviceTypeId, startDate, endDate);
   }

   @Override
   public int getAverageResolutionTimeInHoursByServiceProviderAndServiceType(Long serviceProviderId, Long serviceTypeId, Date startDate, Date endDate)
   {
      return anomalyRepository.getAverageResolutionTimeInHoursByServiceProviderAndServiceType(serviceProviderId, serviceTypeId, startDate, endDate);
   }

   
}
