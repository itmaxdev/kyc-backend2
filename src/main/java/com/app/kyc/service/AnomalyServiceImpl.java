package com.app.kyc.service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.stereotype.Service;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.AnomalyTracking;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ConsumerAnomaly;
import com.app.kyc.entity.NotificationJob;
import com.app.kyc.entity.User;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.model.AnomalyTrackingDto;
import com.app.kyc.model.AnomlyDto;
import com.app.kyc.model.ConsumerDto;
import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.model.NotificationType;
// import com.app.kyc.entity.ConsumerService;
import com.app.kyc.repository.AnomalyRepository;
import com.app.kyc.repository.AnomalyTrackingRepository;
import com.app.kyc.repository.ConsumerAnomalyRepository;
import com.app.kyc.repository.ConsumerRepository;
import com.app.kyc.repository.NotificationJobRepository;
import com.app.kyc.request.UpdateAnomalyStatusRequest;
import com.app.kyc.response.AnomalyDetailsResponseDTO;
import com.app.kyc.response.AnomalyHasSubscriptionsResponseDTO;
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
   
   @Autowired
   private NotificationJobRepository notificationJobRepository;


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

//   @Override
//   public void updateAnomaly(UpdateAnomalyStatusRequest updateAnomalyStatusRequest, User user)
//   {
//      Anomaly anomaly = anomalyRepository.findById(updateAnomalyStatusRequest.getAnomalyId()).get();
//      AnomalyTracking anomalyTracking = new AnomalyTracking(anomaly, new Date(), updateAnomalyStatusRequest.getStatus(), updateAnomalyStatusRequest.getNote(), user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
//      anomalyTrackingRepository.save(anomalyTracking);
//
//      anomaly.setStatus(updateAnomalyStatusRequest.getStatus());
//      anomaly.setUpdatedOn(new Date());
//      anomalyRepository.save(anomaly);
//      List<Consumer> consumers = consumerRepository.getAllByAnomalies(anomaly);
//      Consumer consumer = consumers.get(0);
//      String message = "";
//      boolean spUsrCheck = false;
//      if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.WITHDRAWN)){
//         message = "The anomaly for " + consumer.getFirstName() + " "  + consumer.getLastName() + " has been marked as withdrawn";
//         spUsrCheck = true;
//
//      }
//      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.RESOLVED_FULLY)){
//         message = "The anomaly for " + consumer.getFirstName() + " "  + consumer.getLastName() + " has been marked as resolved";
//         spUsrCheck = true;
//      }
//      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.QUESTION_ANSWERED)){
//         message = "Question has been answered for anomaly " + consumer.getFirstName() + " "  + consumer.getLastName();
//         spUsrCheck = true;
//
//      }
//      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.QUESTION_SUBMITTED)){
//         message = consumer.getServiceProvider().getName() + " has raised a question for anomaly reported on " + consumer.getFirstName() + " " + consumer.getLastName();
//      }
//      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.RESOLUTION_SUBMITTED)){
//         message = consumer.getServiceProvider().getName() + " has raised a resolution for anomaly reported on " + consumer.getFirstName() + " " + consumer.getLastName();
//      }
//      else if(updateAnomalyStatusRequest.getStatus().equals(AnomalyStatus.UNDER_INVESTIGATION)){
//         message = consumer.getServiceProvider().getName() + " has raised a under investigation for anomaly reported on " +consumer.getFirstName() + " " + consumer.getLastName();
//      }
//      if(spUsrCheck){
//         List<User> spUsers = userService.getByServiceProviderId(consumer.getServiceProvider().getId());
//         
//         for (User u : spUsers) {
//            notificationService.addNotification(message, u, NotificationType.ANOMALY_REPORTED, anomaly.getId());
//         }
//      }
//      else{
//         notificationService.addNotification(message,anomaly.getReportedBy(), NotificationType.ANOMALY_REPORTED, anomaly.getId());
//      }
//   }
   
   @Override
   public void updateAnomaly(UpdateAnomalyStatusRequest request, User user) {
       Anomaly anomaly = anomalyRepository.findById(request.getAnomalyId())
               .orElseThrow(() -> new RuntimeException("Anomaly not found"));

       // Save anomaly tracking
       AnomalyTracking anomalyTracking = new AnomalyTracking(
               anomaly,
               new Date(),
               request.getStatus(),
               request.getNote(),
               user.getFirstName() + " " + user.getLastName(),
               anomaly.getUpdatedOn()
       );
       anomalyTrackingRepository.save(anomalyTracking);

       // Update anomaly
       anomaly.setStatus(request.getStatus());
       anomaly.setUpdatedOn(new Date());
       anomalyRepository.save(anomaly);

       //Save job (async processing later)
       notificationJobRepository.save(new NotificationJob(anomaly.getId(), request.getStatus()));
   }

   /*@Override
   public AnomalyDetailsResponseDTO getAnomalyByIdWithDetails(Long id)
   {
      Anomaly anomaly=anomalyRepository.findById(id).get();
      AnomlyDto anomlyDto;
      if(anomaly.getStatus().getCode() == 5){
         anomlyDto= new AnomlyDto(anomaly, 0);
         anomlyDto.setUpdateBy("System");
      }
      else{
         anomlyDto= new AnomlyDto(anomaly);
         anomlyDto.setUpdateBy("System");
      }

      List<AnomalyTrackingDto> anomalyTracking = anomalyTrackingRepository.findAllByAnomalyId(id)
              .stream()
              .map(c-> new AnomalyTrackingDto(c.getId(),c.getCreatedOn(),c.getStatus(),c.getNote(),c.getAnomaly(), c.getUpdateBy(), c.getUpdateOn()))
              .collect(Collectors.toList());



//      anomlyDto.getConsumers().forEach(c -> {
//         if(Objects.isNull(c.getFirstName()))
//            c.setFirstName("");
//         if(Objects.isNull(c.getLastName()))
//            c.setLastName("");
//         List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(anomlyDto.getId(), c.getId());
//            temp.forEach(t -> {
//               if (Objects.nonNull(t.getNotes())) {
//                  c.setNotes(t.getNotes());
//               }
//            });
//         });
      
      List<ConsumerDto> consumerDtos = new ArrayList<ConsumerDto>();
      List<ConsumerAnomaly> consumerAnomalyList = consumerAnomalyRepository.findByAnomaly_Id(anomlyDto.getId());
      consumerAnomalyList.forEach(c -> {
    	  ConsumerDto consumerDto = new ConsumerDto(c.getConsumer());
    	  consumerDtos.add(consumerDto);
      });
      
      anomlyDto.setConsumers(consumerDtos);

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
   }*/

   @Override
   public AnomalyDetailsResponseDTO getAnomalyByIdWithDetails(Long id) {
      // Safely fetch anomaly
      Anomaly anomaly = anomalyRepository.findById(id)
              .orElseThrow(() -> new RuntimeException("Anomaly not found with id: " + id));

      //todo for test
     AnomlyDto anomlyDto;
      if (anomaly.getStatus().getCode() == 6) {
         anomlyDto = new AnomlyDto(anomaly, 0);
      } else {
         anomlyDto = new AnomlyDto(anomaly);
      }

      // ✅ Fetch anomaly tracking (deduplicate by status)
      List<AnomalyTrackingDto> anomalyTracking =
              anomalyTrackingRepository.findDistinctByAnomalyIdOrderByCreatedOnDesc(id)
                      .stream()
                      .map(c -> {
                         String finalNote;
                         if (c.getNote() != null && !c.getNote().isBlank()) {
                            finalNote = c.getNote();
                         } else if (AnomalyStatus.REPORTED == c.getStatus()) {
                            finalNote = "Anomaly flagged by " + c.getUpdateBy();
                         } else if (AnomalyStatus.RESOLVED_PARTIALLY == c.getStatus()) {
                            finalNote = "Anomaly Resolved Partially by " + c.getUpdateBy();
                         } else if (AnomalyStatus.RESOLVED_FULLY == c.getStatus()) {
                            finalNote = "Anomaly Resolved by " + c.getUpdateBy();
                         } else {
                            finalNote = "";
                         }

                         return new AnomalyTrackingDto(
                                 c.getId(),
                                 c.getCreatedOn(),
                                 c.getStatus(),
                                 finalNote,
                                 c.getAnomaly(),
                                 c.getUpdateBy(),
                                 c.getUpdateOn()
                         );
                      })
                      // 🔹 Deduplicate: keep only one entry per status (latest, because of DESC order)
                      .collect(Collectors.collectingAndThen(
                              Collectors.toMap(
                                      AnomalyTrackingDto::getStatus,
                                      dto -> dto,
                                      (existing, replacement) -> existing,
                                      LinkedHashMap::new
                              ),
                              m -> {
                                 List<AnomalyTrackingDto> list = new ArrayList<>(m.values());

                                 // 🔹 Apply post-filter
                                 boolean hasFully = list.stream().anyMatch(d -> d.getStatus() == AnomalyStatus.RESOLVED_FULLY);
                                 boolean hasPartially = list.stream().anyMatch(d -> d.getStatus() == AnomalyStatus.RESOLVED_PARTIALLY);

                                 if (hasFully) {
                                    list.removeIf(d -> d.getStatus() == AnomalyStatus.RESOLVED_PARTIALLY);
                                 } else if (hasPartially) {
                                    list.removeIf(d -> d.getStatus() == AnomalyStatus.RESOLVED_FULLY);
                                 }

                                 return list;
                              }
                      ));


      // ✅ Fetch consumers linked to anomaly
      // ✅ Fetch consumers linked to anomaly (deduplicate by consumerId, ignore duplicate notes)
      List<ConsumerDto> consumerDtos = consumerAnomalyRepository.findByAnomaly_Id(id)
              .stream()
              .collect(Collectors.toMap(
                      ca -> ca.getConsumer().getId(),  // use consumerId as key
                      ca -> {
                         ConsumerDto dto = new ConsumerDto(ca.getConsumer());

                         // ✅ set notes if present
                         if (Objects.nonNull(ca.getNotes())) {
                            dto.setNotes(ca.getNotes());
                         }

                         // ✅ handle consistentOn
                         String consistentOn = ca.getConsumer().getConsistentOn();
                         if (consistentOn == null || consistentOn.isBlank()) {
                            dto.setConsistentOn("N/A");
                         } else {
                            dto.setConsistentOn(consistentOn);
                         }

                         return dto;
                      },
                      (existing, duplicate) -> existing, // keep the first occurrence
                      LinkedHashMap::new
              ))
              .values()
              .stream()
              .collect(Collectors.toList());

      long consistentCount = consumerDtos.stream()
              .filter(c -> c.getConsistentOn() != null && !"N/A".equalsIgnoreCase(c.getConsistentOn()))
              .count();

      long inconsistentCount = consumerDtos.size() - consistentCount;

      long total = anomalyTracking.size();
      long partialCount = anomalyTracking.stream()
              .filter(t -> t.getStatus() == AnomalyStatus.RESOLVED_PARTIALLY)
              .count();

      double partiallyResolvedPercentage = total > 0
              ? (partialCount * 100.0) / total
              : 0.0;

      anomlyDto.setConsumers(consumerDtos);


      // ✅ Enrich tracking data with consumer notes (if anomaly type = 1)
      anomalyTracking.forEach(tracking -> {
         if (tracking.getAnomlyDto().getAnomalyType().getId() == 1) {
            anomlyDto.getConsumers().forEach(consumer -> {
               List<ConsumerAnomaly> temp = consumerAnomalyRepository
                       .findByAnomaly_IdAndConsumer_Id(id, consumer.getId());
               temp.forEach(t -> {
                  if (Objects.nonNull(t.getNotes())) {
                     consumer.setNotes(t.getNotes());
                  }
               });
            });
         }
      });

      return new AnomalyDetailsResponseDTO(anomlyDto, anomalyTracking,consistentCount,inconsistentCount,partiallyResolvedPercentage);
   }



   @Override
   public double getAverageResolutionTimeInHours(Long industryId, List<Long> serviceProviderIds, Date startDate, Date endDate)
   {
      return anomalyRepository.getAverageResolutionTimeInHours(serviceProviderIds, startDate, endDate);
   }

   @Override
   public int getAnomaliesReportedByServiceProvidersAndDates(List<Long> serviceProviderIds, List<AnomalyStatus> statuses, Date startDate, Date endDate) {
      return (int) anomalyRepository.countDistinctByConsumers_ServiceProvider_IdInAndStatusInAndReportedOnBetween(serviceProviderIds ,statuses ,startDate, endDate);
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

   @Override
   public List<Object[]> getResolutionMetrics(Long industryId, List<Long> serviceProviderIds, Date startDate, Date endDate)
   {
	  String groupBy = decideGroupBy(startDate,endDate);
      return anomalyRepository.getResolutionMetrics(serviceProviderIds ,startDate, endDate,groupBy);
   }
   
	private String decideGroupBy(Date start, Date end) {
		long diffInMillies = end.getTime() - start.getTime();
		long days = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS) + 1;
		if (days < 30) {
			return "DAY"; // short ranges → daily
		} else if (days < 365) {
			return "MONTH"; // medium ranges → monthly
		} else {
			return "QUARTER"; // long ranges → quarterly
		}
	}
}
