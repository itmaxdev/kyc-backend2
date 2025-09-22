package com.app.kyc.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.PersistenceContext;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.app.kyc.Masking.MaskingUtil;
import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.AnomalyTracking;
import com.app.kyc.entity.AnomalyType;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ConsumerAnomaly;
import com.app.kyc.entity.ConsumerTracking;
import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.entity.User;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.model.AnomlyDto;
import com.app.kyc.model.ConsumerDto;
import com.app.kyc.model.ConsumerHistoryDto;
import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.model.ExceedingConsumers;
import com.app.kyc.model.Pagination;
import com.app.kyc.repository.AnomalyRepository;
import com.app.kyc.repository.AnomalyTrackingRepository;
import com.app.kyc.repository.AnomalyTypeRepository;
import com.app.kyc.repository.ConsumerAnomalyRepository;
import com.app.kyc.repository.ConsumerRepository;
import com.app.kyc.repository.ConsumerSpecifications;
import com.app.kyc.repository.ConsumerTrackingRepository;
import com.app.kyc.repository.ServiceProviderRepository;
import com.app.kyc.response.ConsumersDetailsResponseDTO;
import com.app.kyc.response.ConsumersHasSubscriptionsResponseDTO;
import com.app.kyc.response.FlaggedConsumersListDTO;
import com.app.kyc.util.AnomalyCollection;
import com.app.kyc.util.PaginationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ConsumerServiceImpl implements ConsumerService {
    @Autowired
    private ServiceProviderRepository serviceProviderRepository;

    @Autowired
    private ConsumerAnomalyRepository consumerAnomalyRepository;

    @Autowired
    AnomalyRepository anomalyRepository;

    @PersistenceContext
    EntityManager em;

    @Autowired
    AnomalyTypeRepository anomalyTypeRepository;

    @Autowired
    ConsumerRepository consumerRepository;

    @Autowired
    ConsumerServiceService consumerServiceService;

    @Autowired
    private AnomalyTrackingRepository anomalyTrackingRepository;
    
    @Autowired
    private ConsumerTrackingRepository consumerTrackingRepository;

    static final Integer DEFAULT_FIRST_ROW = 0;

    List<Consumer> consumers = new ArrayList<>();

    /*public ConsumerDto getConsumerById(Long id) {
        Optional<Consumer> consumer = Optional.ofNullable(consumerRepository.findByIdAndConsumerStatus(id, 0));
        ConsumerDto consumerDto = null;
        if (consumer.isPresent()) {
            consumerDto = new ConsumerDto(consumer.get(), consumer.get().getAnomalies());
        }

        return consumerDto;
    }*/


    public ConsumerDto getConsumerById(Long id) {
        Optional<Consumer> consumer = consumerRepository.findByIdAndConsumerStatusIn(id, Arrays.asList(0, 1));

        if (!consumer.isPresent()) {
            consumer = consumerRepository.findById(id);
        }
        
        return consumer.map(c -> {
            // existing anomalies
            ConsumerDto dto = new ConsumerDto(c, c.getAnomalies());
       
            List<ConsumerHistoryDto> history = new ArrayList<>();

			// Only process if anomalies exist
            if (c.getAnomalies() != null && !c.getAnomalies().isEmpty()) {
                List<ConsumerTracking> trackings = consumerTrackingRepository.findByConsumerIdOrderByCreatedOnDesc(c.getId());
                for (ConsumerTracking t : trackings) {
                    if (t != null) {
                    	String note = MaskingUtil.maskName(c.getFirstName()) + " " + MaskingUtil.maskName(c.getMiddleName()) + " " + MaskingUtil.maskName(c.getLastName()) + " linked to ";
                        String formattedId = c.getServiceProvider().getName()
							        + "-" + new SimpleDateFormat("ddMMyyyy").format(c.getAnomalies().get(0).getReportedOn()) 
							        + "-" + c.getAnomalies().get(0).getId();
						

                        String consistencyStatus = t.getIsConsistent() == true ? "Consistent" : "Inconsistent";
                        String inconsistentOn = c.getCreatedOn();
                        String consistentOn = t.getConsistentOn() != null ? t.getConsistentOn() : "N/A";

                        history.add(new ConsumerHistoryDto(consistencyStatus, note , inconsistentOn, consistentOn , formattedId));
                    }
                }
            }

            dto.setConsumerHistory(history);

            return dto;
        }).orElse(null);
    }




    @Transactional(readOnly = true)
    public Map<String, Object> getAllConsumers(String params)
            throws JsonMappingException, JsonProcessingException {

        // Pageable (null-safe) + cap page size
        final Pageable requested = Optional.ofNullable(PaginationUtil.getPageable(params))
                .orElse(PageRequest.of(0, 50));
        final int MAX_PAGE_SIZE = 5000;
        final Pageable pageable = PageRequest.of(
                requested.getPageNumber(),
                Math.min(requested.getPageSize(), MAX_PAGE_SIZE),
                requested.getSort()
        );

        // Filters
        final Pagination pagination = PaginationUtil.getFilterObject(params);
        final String type = Optional.ofNullable(pagination)
                .map(Pagination::getFilter).map(f -> f.getType())
                .map(String::trim).map(String::toUpperCase)
                .orElse("ALL");
        final Long spId = Optional.ofNullable(pagination)
                .map(Pagination::getFilter).map(f -> f.getServiceProviderID())
                .orElse(null);
        
        final String searchText = Optional.ofNullable(pagination)              
                .map(Pagination::getFilter)                            
                .map(f -> f.getSearchText())                          
                .map(String::trim)                                     
                .map(String::toLowerCase)                              
                .orElse(null);

        // Statuses we allow
        final List<Integer> allowedStatuses = Arrays.asList(0, 1);

        // Counters
        final long allCount, consistentCount, inconsistentCount;
        if (spId != null) {
            allCount          = consumerRepository.countByServiceProviderId(spId);
            consistentCount   = consumerRepository.countByIsConsistentTrueAndServiceProvider_Id(spId);
            inconsistentCount = consumerRepository.countByIsConsistentFalseAndServiceProvider_Id(spId);
        } else {
            allCount          = consumerRepository.count();
            consistentCount   = consumerRepository.countByIsConsistentTrue();
            inconsistentCount = consumerRepository.countByIsConsistentFalse();
        }

        // ===== Fetch THREE independent pages =====
        // A) ALL
//        final Page<Consumer> allPage = (spId != null)
//                ? consumerRepository.findByServiceProvider_Id(spId, pageable)
//                : consumerRepository.findAll(pageable);

        // B) CONSISTENT
//        final Page<Consumer> consistentPage = (spId != null)
//                ? consumerRepository.findByIsConsistentTrueAndConsumerStatusInAndServiceProvider_Id(pageable, allowedStatuses, spId)
//                : consumerRepository.findByIsConsistentTrueAndConsumerStatusIn(pageable, allowedStatuses);

        // C) INCONSISTENT
//        final Page<Consumer> inconsistentPage = (spId != null)
//                ? consumerRepository.findByIsConsistentFalseAndConsumerStatusInAndServiceProvider_Id(pageable, allowedStatuses, spId)
//                : consumerRepository.findByIsConsistentFalseAndConsumerStatusIn(pageable, allowedStatuses);
        
     
		final Page<Consumer> filterData;
		long filterCount;
		if ("CONSISTENT".equals(type)) {
			filterCount  = consumerRepository.count(
	                ConsumerSpecifications.withFilters(spId, searchText, true ,allowedStatuses)
			        );
			filterData = consumerRepository
					.findAll(ConsumerSpecifications.withFilters(spId, searchText, true, allowedStatuses), pageable);
		} else if ("INCONSISTENT".equals(type)) {
			filterCount = consumerRepository.count(
	                ConsumerSpecifications.withFilters(spId, searchText, false ,allowedStatuses)
			        );
			filterData = consumerRepository
					.findAll(ConsumerSpecifications.withFilters(spId, searchText, false, allowedStatuses), pageable);
		} else {
			filterCount = consumerRepository.count(
	                ConsumerSpecifications.withFilters(spId, searchText, null ,null)
	        );
			filterData = consumerRepository.findAll(ConsumerSpecifications.withFilters(spId, searchText, null, null),
					pageable);
		}
		
		final List<ConsumersHasSubscriptionsResponseDTO> finalData = toDtoPage(dedup(filterData.getContent()));

        // Map each slice independently (with de-dup just in case)
//        final List<ConsumersHasSubscriptionsResponseDTO> allData          = toDtoPage(dedup(allPage.getContent()));
//        final List<ConsumersHasSubscriptionsResponseDTO> consistentData   = toDtoPage(dedup(consistentPage.getContent()));
//        final List<ConsumersHasSubscriptionsResponseDTO> inconsistentData = toDtoPage(dedup(inconsistentPage.getContent()));

        // "data" shaped by filter.type
//        final List<ConsumersHasSubscriptionsResponseDTO> dataBucket =
//                "CONSISTENT".equals(type)   ? consistentData :
//                        "INCONSISTENT".equals(type) ? inconsistentData :
//                                allData;

        Map<String, Object> resp = new HashMap<>();
        resp.put("count", allCount);
        resp.put("consistentCount", consistentCount);
        resp.put("inconsistentCount", inconsistentCount);
        resp.put("filterCount", filterCount);
        resp.put("data", finalData);               // page of ALL / CONSISTENT / INCONSISTENT based on type
        //resp.put("consistentData", consistentData); // always a consistent page
        //resp.put("inconsistentData", inconsistentData); // always an inconsistent page
        return resp;
    }

// --- helpers ---

    private List<Consumer> dedup(List<Consumer> items) {
        if (items == null || items.isEmpty()) return Collections.emptyList();
        return items.stream()
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(Consumer::getId, c -> c, (a, b) -> a, LinkedHashMap::new),
                        m -> new ArrayList<>(m.values())
                ));
    }

    private List<ConsumersHasSubscriptionsResponseDTO> toDtoPage(List<Consumer> consumers) {
        if (consumers == null || consumers.isEmpty()) return Collections.emptyList();

        // Bulk anomalies for this slice (avoid touching lazy collections)
        final List<ConsumerAnomaly> sliceAnomalies = consumerAnomalyRepository.findAllByConsumerIn(consumers);

        // Build notes map for anomalyTypeId = 1 (adjust if needed)
        final long NOTES_ANOMALY_TYPE_ID = 1L;
        final Map<Long, String> notesByConsumerId = new HashMap<>();
        for (ConsumerAnomaly ca : sliceAnomalies) {
            if (ca == null || ca.getAnomaly() == null || ca.getAnomaly().getAnomalyType() == null || ca.getConsumer() == null) continue;
            if (!Objects.equals(ca.getAnomaly().getAnomalyType().getId(), NOTES_ANOMALY_TYPE_ID)) continue;
            if (ca.getNotes() == null) continue;
            notesByConsumerId.putIfAbsent(ca.getConsumer().getId(), ca.getNotes());
        }

        final List<ConsumersHasSubscriptionsResponseDTO> data = new ArrayList<>(consumers.size());
        for (Consumer c : consumers) {
            ConsumerDto dto = new ConsumerDto(c, Collections.emptyList());
            // IMPORTANT: ensure ConsumerDto has a Boolean isConsistent field
            dto.setIsConsistent(c.getIsConsistent());
            if (dto.getFirstName() == null) dto.setFirstName("");
            if (dto.getLastName()  == null) dto.setLastName("");
            String notes = notesByConsumerId.get(c.getId());
            if (notes != null) dto.setNotes(notes);

            boolean hasSubs = consumerServiceService.countConsumersByConsumerId(c.getId()) > 0;
            data.add(new ConsumersHasSubscriptionsResponseDTO(dto, hasSubs));
        }
        return data;
    }


    public void addConsumer(Consumer consumer) {
        consumerRepository.save(consumer);
    }

    public Consumer updateConsumer(Consumer consumer) {
        return consumerRepository.save(consumer);
    }

    @Override
    public Map<String, Object> getAllFlaggedConsumers(String params) {
        List<FlaggedConsumersListDTO> consumers = consumerRepository.getAllFlaggedConsumers();
        Map<String, Object> consumersWithCount = new HashMap<String, Object>();
        consumersWithCount.put("data", consumers);
        consumersWithCount.put("count", consumers.size());
        return consumersWithCount;
    }

    @Override
    public int countConsumersByIndustryId(Long industryId, Date start, Date end) {
        return (int) consumerRepository.countByIndustryIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(0, industryId, start, end);
    }

    @Override
    public List<Consumer> getConsumersByCreatedOnGreaterThanAndCreatedOnLessThanEqual(Long industryId, Date start, Date end) {
        return consumerRepository.findAllConsumersByCreatedOnGreaterThanAndCreatedOnLessThanEqual(0, industryId, start, end);
    }

    @Override
    public List<DashboardObjectInterface> getAndCountConsumersGroupedByServiceProviderId(List<Long> serviceProvidersIdList, Date start, Date end) {
        return null;
    }

    @Override
    public List<Consumer> getConsumersByServiceTypeId(Long serviceTypeId, Date start, Date end) {
        return consumerRepository.findAllConsumersByServiceTypeAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(0, serviceTypeId, start, end);
    }

    @Override
    public List<Consumer> getConsumersByServiceProviderIdAndDateRange(Long serviceProviderId, Date start, Date end) {
        return consumerRepository.findAllConsumersByServiceProviderIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqualAndConsumerStatus(serviceProviderId, start, end, 0);
    }

    @Override
    public List<Consumer> getConsumersByServiceProviderId(Long serviceProviderId) {
        return consumerRepository.findAllConsumersByServiceProviderIdAndConsumerStatus(serviceProviderId, 0);
    }

    @Override
    public List<Consumer> getConsumersByServiceProviderIdAndServiceTypeId(Long serviceProviderId, Long serviceTypeId, Date start, Date end) {
        return consumerRepository.findAllConsumersByServiceProviderIdAndServiceTypeIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(0, serviceProviderId, serviceTypeId, start, end);
    }

    @Override
    public Map<String, Object> getAllByServiceIdAndUserId(Long userId, Long serviceId) {
        List<ConsumerDto> consumers = consumerRepository.getAllByServiceIdAndUserId(0, userId, serviceId)
        .stream()
        .map(c -> new ConsumerDto(c, null)).collect(Collectors.toList());

        List<ConsumersHasSubscriptionsResponseDTO> consumersHasSubscriptionsResponseDTOS = new ArrayList<>();

        for (ConsumerDto a : consumers) {
            int countSucscriptions = consumerRepository.countById(a.getId());
            consumersHasSubscriptionsResponseDTOS.add(new ConsumersHasSubscriptionsResponseDTO(a, countSucscriptions > 0 ? true : false));
        }


        Map<String, Object> consumersWithCount = new HashMap<String, Object>();
        consumersWithCount.put("data", consumersHasSubscriptionsResponseDTOS);
        consumersWithCount.put("count", consumers.size());
        return consumersWithCount;
    }

    @Override
    public ConsumersDetailsResponseDTO getConsumerByIdwithSubscriptions(Long id) {
        Optional<Consumer> consumer = Optional.ofNullable(consumerRepository.findByIdAndConsumerStatus(id, 0));
        ConsumerDto consumerDto = new ConsumerDto(consumer.get());
        @SuppressWarnings("unchecked")
        List<com.app.kyc.entity.ConsumerService> consumerServices = (List<com.app.kyc.entity.ConsumerService>) consumerServiceService.getAllConsumerServices(id).get("data");
        ConsumersDetailsResponseDTO response = new ConsumersDetailsResponseDTO(consumerDto, consumerServices);
        return response;
    }
    
   /* @Override
    public List<DashboardObjectInterface> getAndCountConsumersGroupedByServiceProviderId(List<Long> serviceProvidersIdList, Date start, Date end) {
        return consumerRepository.getAndCountConsumersGroupedByServiceProviderId(serviceProvidersIdList, start, end);
    }*/

    @Override
    public List<DashboardObjectInterface> getAndCountDistinctConsumersGroupedByServiceProviderId(List<Long> serviceProvidersIdList, Date start, Date end) {
        return consumerRepository.getAndCountDistinctConsumersGroupedByServiceProviderId(serviceProvidersIdList, start, end);
    }

    @Override
    public long countConsumersByServiceProvidersBetweenDates(Collection<Long> serviceProvidersIds, Date createdOnStart, Date createdOnEnd, boolean isConsistent, int consumerStatus){
        return consumerRepository.countConsumersByServiceProvider_IdInAndCreatedonDateBetweenAndIsConsistent(serviceProvidersIds,  createdOnStart, createdOnEnd, isConsistent);
    }

    @Override
    public long countSubscribersByServiceProvidersBetweenDates(Collection<Long> serviceProvidersIds, Date createdOnStart, Date createdOnEnd, int consumerStatus){
        return consumerRepository.countSubscribersByServiceProvider_IdInAndRegistrationDateBetweenAndConsumerStatus(serviceProvidersIds,  createdOnStart, createdOnEnd, consumerStatus);
    }

    @Override
    public long countDistinctConsumerByServiceProvidersBetweenDates(Collection<Long> serviceProvidersIds, Date createdOnStart, Date createdOnEnd) {
        return consumerRepository.countDistinctByServiceProvider_IdInAndCreatedOnBetween(serviceProvidersIds, createdOnStart, createdOnEnd);
    }

    @Override
    public List<DashboardObjectInterface> getAndCountConsumersByServiceProviderBetweenDatesGroupByMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd) {
        return consumerRepository.countByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonth(serviceProviderIds,createdOnStart,createdOnEnd);
    }

    @Override
    public List<DashboardObjectInterface> getAndCountConsumersByServiceProviderBetweenDatesGroupByDateMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd) {
        return consumerRepository.countByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonthDate(serviceProviderIds,createdOnStart,createdOnEnd);
    }

    @Override
    public List<DashboardObjectInterface> getAndCountDistinctConsumersByServiceProviderBetweenDatesGroupByMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd, int consumerStatus) {
        return consumerRepository.countDistinctByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonth(serviceProviderIds, createdOnStart, createdOnEnd, consumerStatus);
    }

    @Override
    public List<DashboardObjectInterface> getAndCountDistinctConsumersByServiceProviderBetweenDatesGroupByDateMonthYear(Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd, int consumerStatus) {
        return consumerRepository.countDistinctByServiceProvider_IdInAndCreatedOnBetweenGroupByYearMonthDate(serviceProviderIds, createdOnStart, createdOnEnd, consumerStatus);
    }

    @Override
    public long getTotalConsumers (Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd){ 
        return consumerRepository.getTotalConsumers(serviceProviderIds, createdOnStart, createdOnEnd);
    }

    @Override
    public List<Object[]> getConsumersPerOperator (Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd){
        return consumerRepository.getConsumersPerOperator(serviceProviderIds, createdOnStart, createdOnEnd);
    }

    @Override
    public List<DashboardObjectInterface> buildAnomalyTypes(List<Long> serviceProviderIds, int threshold) {
        final boolean all =
                (serviceProviderIds == null || serviceProviderIds.isEmpty() ||
                        (serviceProviderIds.size() == 1 && serviceProviderIds.get(0) == 0L));

        final List<String> providerNames = all
                ? List.of()
                : serviceProviderRepository.findNamesByIds(serviceProviderIds); // add this query if missing

        return consumerRepository.getMsisdnAnomalyTypesRollup(
                providerNames,
                all || providerNames.isEmpty(),
                threshold
        );
    }
    
    @Override
    public List<DashboardObjectInterface> buildAnomalyTypes(List<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd){
    	 final boolean all =
                 (serviceProviderIds == null || serviceProviderIds.isEmpty() ||
                         (serviceProviderIds.size() == 1 && serviceProviderIds.get(0) == 0L));

         final List<String> providerNames = all
                 ? List.of()
                 : serviceProviderRepository.findNamesByIds(serviceProviderIds); // add this query if missing
         
        

         List<DashboardObjectInterface> dashboardObjectInterfaces = consumerRepository.getAnomalyCountsByAnomalyTypes(
                 providerNames,
                 all || providerNames.isEmpty(),
                 createdOnStart,
                 createdOnEnd
         );
         return dashboardObjectInterfaces;
    }

  /*  @Override
    public long getConsumersPerOperator(){
        return consumerRepository.getConsumersPerOperator();
    }

    public List<Object[]> getConsumersPerOperatorBreakdown(){
        return consumerRepository.getConsumersPerOperatorBreakdown();
    }
*/

   /* @Override
    public Map<String, Object> getAllFlaggedConsumers2(String params) throws JsonMappingException, JsonProcessingException {
        //List<AnomlyDto> pageAnomaly = anomalyRepository.findAll(PaginationUtil.getPageable(params)).stream().map(a -> new AnomlyDto(a)).collect(Collectors.toList());

System.out.println("Get all flagged ");
        Pagination pagination = PaginationUtil.getFilterObject(params);
        List<Integer> consumerStatus = new ArrayList<>();
       // consumerStatus.add(0);
        List<AnomlyDto> pageAnomaly = null;
        List<AnomalyStatus> anomalyStatus = new ArrayList<>();
        long totalAnomaliesCount = 0L;

        if (Objects.nonNull(pagination.getFilter()) && (Objects.isNull(pagination.getFilter().getServiceProviderID()) || pagination.getFilter().getServiceProviderID() == -1)) {
            if(pagination.getFilter().getIsResolved()){
                System.out.println("Consumer status is "+consumerStatus);
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumerStatus(PaginationUtil.getPageable(params), consumerStatus, anomalyStatus, pagination.getFilter().getAnomalyType());
                pageAnomaly = anomalyData.stream()
                        .map(a -> new AnomlyDto(a , 0)).collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();


            }
            else{
                System.out.println("Consumer status is one "+consumerStatus);
                anomalyStatus = this.setStatusList(pagination.getFilter().getAnomalyStatus());
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumersAll(PaginationUtil.getPageable(params), anomalyStatus, pagination.getFilter().getAnomalyType());
                pageAnomaly = anomalyData.stream().map(a -> new AnomlyDto(a)).collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();

            }
        }
        else {
            if(pagination.getFilter().getIsResolved()){
                System.out.println("Consumer status is two"+consumerStatus);
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumerStatusAndServiceProviderId(PaginationUtil.getPageable(params), consumerStatus, pagination.getFilter().getServiceProviderID(), anomalyStatus,pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData
                .stream()
                .map(c -> new AnomlyDto(c,0)).collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();
            }
            else{
                consumerStatus.add(0);
                System.out.println("Consumer status is three"+consumerStatus);
                anomalyStatus = this.setStatusList(pagination.getFilter().getAnomalyStatus());
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumerStatusAndServiceProviderId(PaginationUtil.getPageable(params), consumerStatus, pagination.getFilter().getServiceProviderID(), anomalyStatus,pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData
                .stream()
                .map(c -> new AnomlyDto(c)).collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();


            }

        }

        pageAnomaly.forEach(anomaliesDto -> {
            anomaliesDto.getConsumers().forEach(c -> {
                if(Objects.isNull(c.getFirstName()))
                c.setFirstName("");
                if(Objects.isNull(c.getLastName()))
                c.setLastName("");
                List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByConsumer_IdAndAnomaly_Id(c.getId(), anomaliesDto.getId());
                temp.forEach(t -> {
                    if (Objects.nonNull(t.getNotes())) {
                        c.setNotes(t.getNotes());
                    }
                });
            });
        });

        Map<String, Object> anomaliesWithCount = new HashMap<String, Object>();
        anomaliesWithCount.put("data", pageAnomaly);
        anomaliesWithCount.put("count", totalAnomaliesCount);
        pageAnomaly.forEach(System.out::println);
        return anomaliesWithCount;
    }*/

    /*@Override
    public Map<String, Object> getAllFlaggedConsumers2(String params)
            throws JsonMappingException, JsonProcessingException {

        System.out.println("Get all flagged");

        final Pagination pagination = PaginationUtil.getFilterObject(params);
        final Pageable pageable     = PaginationUtil.getPageable(params);

        final List<Integer> consumerStatus = new ArrayList<>();
        final List<AnomalyStatus> anomalyStatus = new ArrayList<>();
        List<AnomlyDto> pageAnomaly;
        long totalAnomaliesCount;

        final boolean noSpFilter = pagination != null
                && pagination.getFilter() != null
                && (pagination.getFilter().getServiceProviderID() == null
                || pagination.getFilter().getServiceProviderID() == -1);

        final boolean isResolved = pagination != null
                && pagination.getFilter() != null
                && Boolean.TRUE.equals(pagination.getFilter().getIsResolved());

        if (noSpFilter) {
            if (isResolved) {
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumerStatus(pageable, consumerStatus, anomalyStatus,pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData.stream()
                        .map(a -> new AnomlyDto(a, 0))
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();

            } else {
            	anomalyStatus.addAll(this.setStatusList(pagination.getFilter().getAnomalyStatus()));
                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumersAll(pageable, anomalyStatus,pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData.stream()
                        .map(AnomlyDto::new)
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();
            }
        } else {
            final Long spId = pagination.getFilter().getServiceProviderID();

            if (isResolved) {
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumerStatusAndServiceProviderId(pageable, consumerStatus, spId, anomalyStatus, pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData.stream()
                        .map(a -> new AnomlyDto(a, 0))
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();

            } else {
                consumerStatus.add(0);
                
                anomalyStatus.addAll(this.setStatusList(pagination.getFilter().getAnomalyStatus()));

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumerStatusAndServiceProviderId(pageable, consumerStatus, spId, anomalyStatus, pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData.stream()
                        .map(AnomlyDto::new)
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();
            }
        }

        if (pageAnomaly.isEmpty()) {
            Map<String, Object> empty = new HashMap<>();
            empty.put("data", Collections.emptyList());
            empty.put("count", 0L);
            return empty;
        }

        // --------- HYDRATE CONSUMERS VIA ConsumerAnomaly (fix for Duplicate Records showing []) ---------

        // 1) Collect anomaly ids from the page
        List<Long> anomalyIds = pageAnomaly.stream()
                .map(AnomlyDto::getId)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // 2) Bulk load all ConsumerAnomaly links (with Consumer fetched)
        List<ConsumerAnomaly> links = consumerAnomalyRepository.findAllByAnomalyIdInFetchConsumer(anomalyIds);

        // 3) Build maps for quick patching
        //    a) anomalyId -> list of ConsumerDto (built from CA.consumer)
        Map<Long, List<ConsumerDto>> consumersByAnomalyId = links.stream()
                .collect(Collectors.groupingBy(
                        ca -> ca.getAnomaly().getId(),
                        Collectors.mapping(ca -> {
                            ConsumerDto cd = new ConsumerDto(ca.getConsumer(), Collections.emptyList());
                            if (cd.getFirstName() == null) cd.setFirstName("");
                            if (cd.getLastName()  == null) cd.setLastName("");
                            if (ca.getNotes() != null) cd.setNotes(ca.getNotes());
                            return cd;
                        }, Collectors.toList())
                ));

        //    b) anomalyId -> (consumerId -> notes)
        Map<Long, Map<Long, String>> notesByAnomalyThenConsumer = new HashMap<>();
        for (ConsumerAnomaly ca : links) {
            final Long aId = ca.getAnomaly().getId();
            final Long cId = ca.getConsumer().getId();
            if (aId == null || cId == null) continue;
            notesByAnomalyThenConsumer
                    .computeIfAbsent(aId, k -> new HashMap<>())
                    .put(cId, ca.getNotes());
        }

        // 4) Patch each AnomlyDto
        for (AnomlyDto dto : pageAnomaly) {
            List<ConsumerDto> fromLinks = consumersByAnomalyId.get(dto.getId());

            if (dto.getConsumers() == null || dto.getConsumers().isEmpty()) {
                // If entity-side collection was empty, hydrate entirely from links
                if (fromLinks != null) dto.setConsumers(fromLinks);
            } else {
                // Normalize names and apply notes from CA map (avoid per-row queries)
                Map<Long, String> notesForAnomaly = notesByAnomalyThenConsumer.get(dto.getId());
                for (ConsumerDto c : dto.getConsumers()) {
                    if (c.getFirstName() == null) c.setFirstName("");
                    if (c.getLastName()  == null) c.setLastName("");
                    if (notesForAnomaly != null && c.getId() != null) {
                        String note = notesForAnomaly.get(c.getId());
                        if (note != null) c.setNotes(note);
                    }
                }
            }
        }

        // (Optional) debug
        pageAnomaly.forEach(System.out::println);

        Map<String, Object> anomaliesWithCount = new HashMap<>();
        anomaliesWithCount.put("data", pageAnomaly);
        anomaliesWithCount.put("count", totalAnomaliesCount);
        return anomaliesWithCount;
    }*/

    @Override
    public Map<String, Object> getAllFlaggedConsumers2(String params)
            throws JsonMappingException, JsonProcessingException {

        System.out.println("Get all flagged");

        final Pagination pagination = PaginationUtil.getFilterObject(params);
        final Pageable pageable     = PaginationUtil.getPageable(params);

        final List<Integer> consumerStatus = new ArrayList<>();
        final List<AnomalyStatus> anomalyStatus = new ArrayList<>();
        final List<AnomalyStatus> resolutionStatus = new ArrayList<AnomalyStatus>();
        List<AnomlyDto> pageAnomaly;
        long totalAnomaliesCount;

        final boolean noSpFilter = pagination != null
                && pagination.getFilter() != null
                && (pagination.getFilter().getServiceProviderID() == null
                || pagination.getFilter().getServiceProviderID() == -1);
        
        final boolean isResolved = pagination != null
                && pagination.getFilter() != null
                && Boolean.TRUE.equals(pagination.getFilter().getIsResolved());
        
        final String searchText = Optional.ofNullable(pagination)              
                .map(Pagination::getFilter)                            
                .map(f -> f.getSearchText())                          
                .map(String::trim)                                     
                .map(String::toLowerCase)                              
                .orElse(null);
        
        if (noSpFilter) {
            if (isResolved) {
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumerStatus(pageable, consumerStatus, anomalyStatus, pagination.getFilter().getAnomalyType(),searchText);

                pageAnomaly = anomalyData.stream()
                        .map(a -> new AnomlyDto(a, 0))
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();

            } else {
                anomalyStatus.addAll(this.setStatusList(pagination.getFilter().getAnomalyStatus()));
                resolutionStatus.addAll(this.setResolution(pagination.getFilter().getResolution()));

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumersAll(pageable, anomalyStatus, pagination.getFilter().getAnomalyType(),resolutionStatus,searchText);

                pageAnomaly = anomalyData.stream()
                        .map(AnomlyDto::new)
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();
            }
        } else {
            final Long spId = pagination.getFilter().getServiceProviderID();
            List<Long> spIds;
            
            if(spId != 0) {
            	spIds = Arrays.asList(spId);
            }else {
            	spIds = serviceProviderRepository.findAll()
                        .stream()
                        .map(ServiceProvider::getId)
                        .collect(Collectors.toList());
            }
            if (isResolved) {
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumerStatusAndServiceProviderId(pageable, consumerStatus, spIds, anomalyStatus, pagination.getFilter().getAnomalyType(),resolutionStatus,searchText);

                pageAnomaly = anomalyData.stream()
                        .map(a -> new AnomlyDto(a, 0))
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();

            } else {
                consumerStatus.add(0);
                consumerStatus.add(1);
                anomalyStatus.addAll(this.setStatusList(pagination.getFilter().getAnomalyStatus()));
                resolutionStatus.addAll(this.setResolution(pagination.getFilter().getResolution()));
                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumerStatusAndServiceProviderId(pageable, consumerStatus, spIds, anomalyStatus, pagination.getFilter().getAnomalyType(),resolutionStatus,searchText);
                pageAnomaly = anomalyData.stream()
                        .map(AnomlyDto::new)
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();
            }
        }

        if (pageAnomaly.isEmpty()) {
            Map<String, Object> empty = new HashMap<>();
            empty.put("data", Collections.emptyList());
            empty.put("count", 0L);
            return empty;
        }

        // --------- HYDRATE CONSUMERS VIA ConsumerAnomaly ---------
        System.out.println("Test is Resolved 5"+isResolved);
        // 1) Collect anomaly ids from the page
        List<Long> anomalyIds = pageAnomaly.stream()
                .map(AnomlyDto::getId)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // 2) Bulk load all ConsumerAnomaly links (with Consumer fetched)
        List<ConsumerAnomaly> links = consumerAnomalyRepository.findAllByAnomalyIdInFetchConsumer(anomalyIds);

        // 3) Build maps
        Map<Long, List<ConsumerDto>> consumersByAnomalyId = links.stream()
                .collect(Collectors.groupingBy(
                        ca -> ca.getAnomaly().getId(),
                        Collectors.collectingAndThen(
                                Collectors.mapping(ca -> {
                                    ConsumerDto cd = new ConsumerDto(ca.getConsumer(), Collections.emptyList());
                                    if (cd.getFirstName() == null) cd.setFirstName("");
                                    if (cd.getLastName()  == null) cd.setLastName("");
                                    if (ca.getNotes() != null) cd.setNotes(ca.getNotes());
                                    return cd;
                                }, Collectors.toSet()),   // first deduplicate
                                set -> new ArrayList<>(set)  // then convert back to List
                        )
                ));


        /*// NEW: counts per anomalyId -> effectedRecords
        Map<Long, Long> effectedCountByAnomalyId = links.stream()
                .collect(Collectors.groupingBy(
                        ca -> ca.getAnomaly().getId(),
                        Collectors.counting()
                ));*/

        Map<Long, Long> effectedCountByAnomalyId = links.stream()
                .collect(Collectors.groupingBy(
                        ca -> ca.getAnomaly().getId(),
                        Collectors.mapping(
                                ca -> ca.getConsumer().getId(), // extract consumerId
                                Collectors.collectingAndThen(
                                        Collectors.toSet(),
                                        set -> (long) set.size()
                                )
                        )
                ));


        Map<Long, Map<Long, String>> notesByAnomalyThenConsumer = new HashMap<>();
        for (ConsumerAnomaly ca : links) {
            final Long aId = ca.getAnomaly().getId();
            final Long cId = ca.getConsumer().getId();
            if (aId == null || cId == null) continue;
            notesByAnomalyThenConsumer
                    .computeIfAbsent(aId, k -> new HashMap<>())
                    .put(cId, ca.getNotes());
        }

        // 4) Patch each AnomlyDto (+ set effectedRecords)
        for (AnomlyDto dto : pageAnomaly) {
            // set effectedRecords (default 0)
            int effected = effectedCountByAnomalyId.getOrDefault(dto.getId(), 0L).intValue();
            dto.setEffectedRecords(effected);    // NEW

            List<ConsumerDto> fromLinks = consumersByAnomalyId.get(dto.getId());

            if (dto.getConsumers() == null || dto.getConsumers().isEmpty()) {
                if (fromLinks != null) dto.setConsumers(fromLinks);
            } else {
                Map<Long, String> notesForAnomaly = notesByAnomalyThenConsumer.get(dto.getId());
                for (ConsumerDto c : dto.getConsumers()) {
                    if (c.getFirstName() == null) c.setFirstName("");
                    if (c.getLastName()  == null) c.setLastName("");
                    if (notesForAnomaly != null && c.getId() != null) {
                        String note = notesForAnomaly.get(c.getId());
                        if (note != null) c.setNotes(note);
                    }
                }
            }
        }

        pageAnomaly.forEach(System.out::println);

        Map<String, Object> anomaliesWithCount = new HashMap<>();
        anomaliesWithCount.put("data", pageAnomaly);
        anomaliesWithCount.put("count", totalAnomaliesCount);
        return anomaliesWithCount;
    }


    public List<List<String>> loadConsumers(Long serviceProviderId, User user) {
        ServiceProvider serviceProvider = serviceProviderRepository.findById(serviceProviderId).get();
        List<List<String>> read = null;
        switch (serviceProvider.getName()) {
            case "Orange":
            // get orange xlsx
            log.warn("Orange");
            loadOrangeConsumers(serviceProvider, user);

            break;
            case "Airtel":
            // get Airtel csv
            log.warn("Airtel");
            loadAirtelConsumers(serviceProvider, user);

            break;
            case "Africell":
            // get Africell xlsx
            log.warn("Africell");
            loadAfricellConsumers(serviceProvider, user);
            break;

            case "Vodacom":
            // get Vodacom xlsx
            log.warn("Vodacom");
            loadVodacomConsumers(serviceProvider, user);
            break;

            case "Standard Bank":
                // get Bank xlsx
                log.warn("Standard Bank");
                loadBankConsumers(serviceProvider, user);
        }
        return read;
    }

    private List<List<String>> loadOrangeConsumers(ServiceProvider serviceProvider, User user) {
        this.consumers.clear();
        String fileLocation = "files/orange.xlsx";
        FileInputStream fileInputStream;
        Date date = null;
        Date registrationDate = null;
        try {
            fileInputStream = new FileInputStream(new File(fileLocation));
            try (Workbook workbook = new XSSFWorkbook(fileInputStream)) {
                workbook.setMissingCellPolicy(MissingCellPolicy.RETURN_BLANK_AS_NULL);
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                while (rowIterator.hasNext()) {
                    List<String> line = new ArrayList<>();
                    Row row = rowIterator.next();
                    //For each row, iterate through all the columns
                    if(row.getRowNum() > DEFAULT_FIRST_ROW){
                        for (int cn = 0; cn < row.getLastCellNum(); cn++) {
                            Cell cell = row.getCell(cn);

                            if (!Objects.isNull(cell)) {
                                if(cn == 7){

                                    if(cell.getCellType().equals(CellType.NUMERIC)) {
                                        line.add(cell.getDateCellValue().toString());
                                        DateFormat sourseFormat = new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy");
                                        date = sourseFormat.parse(line.get(7));
                                    } else {
                                        line.add("");
                                    }

                                } else if(cn == 17){
                                    if(cell.getCellType().equals(CellType.NUMERIC)) {
                                        line.add(cell.getDateCellValue().toString());
                                        DateFormat sourseFormat = new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy");
                                        registrationDate = sourseFormat.parse(line.get(17));
                                    } else {
                                        line.add("");
                                    }
                                }
                                else{
                                    cell.setCellType(org.apache.poi.ss.usermodel.CellType.STRING);
                                    line.add(cell.getStringCellValue());
                                }
                            }
                            else{
                                line.add("");
                            }
                        }
                    }
                    if (line.size() == 0) {
                        continue;
                    }
                    Boolean emptyCheck = false;
                    for(int i=0;i<line.size();i++){
                        if(!Objects.equals(line.get(i), "")){
                            emptyCheck = true;
                        }
                        if(line.get(i).equals("\\N")){
                            line.set(i,"");
                        }
                    }
                    if(!emptyCheck){
                        continue;
                    }
                    Consumer tempConsumer = new Consumer();
                    tempConsumer.setMsisdn(line.get(0));
                    //   tempConsumer.setFirstName(line.get(1));
                    if (!line.get(1).equals("") && !line.get(1).equals("\\N")) {
                        tempConsumer.setFirstName(line.get(1));
                    }
                    //   tempConsumer.setLastName(line.get(2) + " " + line.get(3));
                    List<String> lastNameList = new ArrayList<>();
                    if(!line.get(2).equals("") && !line.get(2).equals("\\N")){
                        lastNameList.add(line.get(2));
                    }
                    if(!line.get(3).equals("") && !line.get(3).equals("\\N")){
                        lastNameList.add(line.get(3));
                    }
                    String lastName = "";
                    if(lastNameList.size()>0){
                        lastName = String.join(" ",lastNameList);
                    }
                    tempConsumer.setLastName(lastName);

                    if (line.get(4).toLowerCase().trim().equals("male") || line.get(4).toLowerCase().trim().equals("masculin") || line.get(4).toLowerCase().trim().equals("m")) {
                        tempConsumer.setGender("MALE");
                    } else if (line.get(4).toLowerCase().trim().equals("feminin") || line.get(4).toLowerCase().trim().equals("female") || line.get(4).toLowerCase().trim().equals("f") ) {
                        tempConsumer.setGender("FEMALE");
                    }
                    if (!line.get(5).equals("") && !line.get(5).equals("\\N")) {
                        tempConsumer.setNationality(line.get(5));
                    }
                    if(!line.get(6).equals("") && !line.get(6).equals("\\N")){
                        tempConsumer.setBirthPlace(line.get(6));
                    }
                    if(!line.get(7).equals("") && !line.get(7).equals("\\N")){
                        tempConsumer.setBirthDate(String.valueOf(date));
                    }
                    else{
                        tempConsumer.setBirthDate(null);
                    }
                    if (!line.get(10).equals("\\N") && !line.get(10).equals("")) {
                        tempConsumer.setIdentificationNumber(line.get(10));
                    }
                    //tempConsumer.setIdentificationNumber(line.get(10));
                    if (!line.get(11).equals("") && !line.get(11).equals("\\N")) {
                        tempConsumer.setIdentificationType(line.get(11));
                    }
                    //                    tempConsumer.setIdentificationType(line.get(11));
                    String address = "";
                    List<String> addressList = new ArrayList<>();
                    if (!line.get(12).equals("")) {
                        addressList.add(line.get(12));
                    }
                    if (!line.get(13).equals("")) {
                        addressList.add(line.get(13));
                    }
                    if (!line.get(14).equals("")) {
                        addressList.add(line.get(14));
                    }
                    if (!line.get(15).equals("")) {
                        addressList.add(line.get(15));
                    }
                    if (!line.get(16).equals("")) {
                        addressList.add(line.get(16));
                    }
                    if(addressList.size()>0){
                        address = String.join(" ",addressList);
                    }
                    tempConsumer.setAddress(address);
                    // tempConsumer.setIdentityCapturePath(line.get(18));
                    tempConsumer.setServiceProvider(serviceProvider);
                    java.util.Date currentDate = new java.util.Date();
                    tempConsumer.setCreatedOn(String.valueOf(currentDate));
                    tempConsumer.setIsConsistent(true);
                    if (!line.get(17).equals("") && !line.get(17).equals("\\N")) {
                        tempConsumer.setRegistrationDate(String.valueOf(registrationDate));
                    }
                    if (!line.get(18).equals("") && !line.get(18).equals("\\N")) {
                        tempConsumer.setIdentityCapturePath(line.get(18));
                    }
                    consumers.add(tempConsumer);
                    //                    checkNullAttributesForFile(line);
                }
                this.checkConsumer(consumers, user, serviceProvider);
                this.consumers.clear();
            }
        } catch (IOException e) {
            log.warn("io error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
            e.printStackTrace();
        } catch (Exception e) {
            log.warn("Ex error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
        }
        // log.warn(file.toString());

        return null;
    }

    private List<List<String>> loadAfricellConsumers(ServiceProvider serviceProvider, User user) {
        this.consumers.clear();
        String fileLocation = "files/Africell.xlsx";
        FileInputStream fileInputStream;
        Date date = null;
        int headerSize = 0;
        try {
            fileInputStream = new FileInputStream(new File(fileLocation));
            String password = "Africell123";
            Workbook workbook = WorkbookFactory.create(fileInputStream, password);
            workbook.setMissingCellPolicy(MissingCellPolicy.RETURN_BLANK_AS_NULL);

            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();
            while (rowIterator.hasNext()) {
                List<String> line = new ArrayList<>();
                Row row = rowIterator.next();
                //For each row, iterate through all the columns
                // Iterator<Cell> cellIterator = row.cellIterator();
                if(row.getRowNum() == 0){
                    headerSize = row.getLastCellNum();
                }
                if(row.getRowNum() > DEFAULT_FIRST_ROW){
                    for (int cn = 0; cn < headerSize; cn++) {
                        Cell cell = row.getCell(cn);
                        if (!Objects.isNull(cell)){
                            if (cn == 5) {
                                if (row.getCell(5).getCellType().equals(CellType.NUMERIC)) {
                                    line.add(cell.getDateCellValue().toString());
                                    DateFormat sourseFormat = new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy");
                                    date = sourseFormat.parse(line.get(5));
                                } else {
                                    line.add("");
                                }
                            }
                            else if(cn == 18){
                                if (row.getCell(18).getCellType().equals(CellType.NUMERIC)) {
                                    line.add(cell.getDateCellValue().toString());
                                    DateFormat sourseFormat = new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy");
                                    date = sourseFormat.parse(line.get(18));
                                } else {
                                    line.add("");
                                }
                            }
                            else {
                                cell.setCellType(org.apache.poi.ss.usermodel.CellType.STRING);
                                line.add(cell.getStringCellValue());
                            }
                        } else {
                            line.add("");
                        }

                    }
                }
                if (line.size() == 0) {
                    continue;
                }

                boolean emptyCheck = false;
                for(int i=0;i<line.size();i++){
                    if(!Objects.equals(line.get(i), "")){
                        emptyCheck = true;
                    }
                }
                if(!emptyCheck){
                    break;
                }

                Consumer tempConsumer = new Consumer();
                tempConsumer.setMsisdn(line.get(0));
                tempConsumer.setFirstName(line.get(1));
                tempConsumer.setLastName(line.get(2) + " " + line.get(3));
                if (line.get(4).toLowerCase().trim().equals("male") || line.get(4).toLowerCase().trim().equals("masculin") || line.get(4).toLowerCase().trim().equals("m")) {
                    tempConsumer.setGender("MALE");
                } else if (line.get(4).toLowerCase().trim().equals("feminin") || line.get(4).toLowerCase().trim().equals("female") || line.get(4).toLowerCase().trim().equals("f") ) {
                    tempConsumer.setGender("FEMALE");
                }

                if(!line.get(5).equals("") && !line.get(5).equals("\\N")){
                    tempConsumer.setBirthDate(String.valueOf(date));
                }
                else{
                    tempConsumer.setBirthDate(null);
                }
                if (!line.get(6).equals("") && !line.get(6).equals("\\N")) {
                    tempConsumer.setNationality(line.get(6));
                }
                if (!line.get(7).equals("") && !line.get(7).equals("\\N")) {
                    tempConsumer.setIdentificationType(line.get(7));
                }
                if (!line.get(8).equals("") && !line.get(8).equals("\\N")) {
                    tempConsumer.setIdentificationNumber(line.get(8));
                }
                if (!line.get(9).equals("") && !line.get(9).equals("\\N")) {
                    tempConsumer.setBirthPlace(line.get(9));
                }
                String address = "";
                List<String> addressList = new ArrayList<>();
                if (!line.get(10).equals("") && !line.get(10).equals("\\N")) {
                    addressList.add(line.get(10));
                }
                if (!line.get(11).equals("") && !line.get(11).equals("\\N")) {
                    addressList.add(line.get(11));
                }
                if (!line.get(12).equals("") && !line.get(12).equals("\\N")) {
                    addressList.add(line.get(12));
                }
                if (!line.get(13).equals("") && !line.get(13).equals("\\N")) {
                    addressList.add(line.get(13));
                }
                if (!line.get(14).equals("") && !line.get(14).equals("\\N")) {
                    addressList.add(line.get(14));
                }
                if(addressList.size()>0){
                    address = String.join(" ",addressList);
                }
                tempConsumer.setAddress(address);

                tempConsumer.setIdentityCapturePath(line.get(17));
                try{
                    if (!line.get(18).equals("") && !line.get(18).equals("")) {
                        DateFormat sourseFormat = new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy");
                        date = sourseFormat.parse(line.get(18));
                        tempConsumer.setRegistrationDate(String.valueOf(date));
                    }
                }
                catch(Exception e){

                }

                // tempConsumer.setIdentityCapturePath(line.get(18));
                tempConsumer.setServiceProvider(serviceProvider);
                java.util.Date currentDate = new java.util.Date();
                tempConsumer.setCreatedOn(String.valueOf(currentDate));
                consumers.add(tempConsumer);

            }
            this.checkConsumer(consumers, user, serviceProvider);
            this.consumers.clear();
        } catch (IOException e) {
            log.warn("io error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
            e.printStackTrace();
        } catch (Exception e) {
            log.warn("Ex error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
        }
        // log.warn(file.toString());

        return null;
    }

    private List<List<String>> loadVodacomConsumers(ServiceProvider serviceProvider, User user) {
        this.consumers.clear();
        String fileLocation = "files/Vodacom.xlsx";
        FileInputStream fileInputStream;
        int headerSize = 0;
        try {
            fileInputStream = new FileInputStream(new File(fileLocation));
            try (Workbook workbook = new XSSFWorkbook(fileInputStream)) {
                workbook.setMissingCellPolicy(MissingCellPolicy.RETURN_BLANK_AS_NULL);
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                while (rowIterator.hasNext()) {
                    List<String> line = new ArrayList<>();
                    Row row = rowIterator.next();
                    //For each row, iterate through all the columns

                    Date registrationDate = null;
                    if(row.getRowNum() == 0){
                        headerSize = row.getLastCellNum();
                    }
                    if(row.getRowNum() > DEFAULT_FIRST_ROW) {
                        for (int cn = 0; cn < headerSize; cn++) {
                            Cell cell = row.getCell(cn);
                            if (!Objects.isNull(cell)) {
                                if (cn == 12) {
                                    if (row.getCell(cn).getStringCellValue().equals("\\N")) {
                                        line.add("\\N");
                                    } else
                                        line.add(cell.getStringCellValue());

                                } else if (cn == 3) {
                                    if (cell.getCellType().equals(CellType.NUMERIC)) {
                                        line.add(cell.getDateCellValue().toString());
                                    } else {
                                        line.add("\\N");
                                    }
                                } else {
                                    cell.setCellType(org.apache.poi.ss.usermodel.CellType.STRING);
                                    line.add(cell.getStringCellValue());
                                }
                            } else {
                                line.add("");
                            }
                        }

                    }
                    if (line.size() == 0) {
                        continue;
                    }
                    boolean emptyCheck = false;
                    for(int i=0;i<line.size();i++){
                        if(!Objects.equals(line.get(i), "")){
                            emptyCheck = true;
                        }
                    }
                    if(!emptyCheck){
                        break;
                    }
                    Consumer tempConsumer = new Consumer();
                    tempConsumer.setMsisdn(line.get(0));
                    tempConsumer.setRegistrationDate(String.valueOf(registrationDate));
                    tempConsumer.setFirstName(line.get(5));
                    tempConsumer.setLastName(line.get(6));

                    List<String> address = new ArrayList<>();
                    if (!line.get(7).equals("")) {
                        address.add(line.get(7));
                    }
                    if (!line.get(8).equals("")) {
                        address.add(line.get(8));
                    }

                    if (!line.get(9).equals("")) {
                        address.add(line.get(9));
                    }
                    if (!line.get(10).equals("")) {
                        address.add(line.get(10));
                    }

                    tempConsumer.setAddress(String.join(" ", address));

                    if (line.get(11).toLowerCase().trim().equals("male") || line.get(11).toLowerCase().trim().equals("masculin") || line.get(11).toLowerCase().trim().equals("m")) {
                        tempConsumer.setGender("MALE");
                    } else if (line.get(11).toLowerCase().trim().equals("feminin") || line.get(11).toLowerCase().trim().equals("female") || line.get(11).toLowerCase().trim().equals("f") ) {
                        tempConsumer.setGender("FEMALE");
                    }
                    if(!line.get(12).equals("") && !line.get(12).equals("\\N")){
                        DateFormat sourseFormat = new SimpleDateFormat("yyyyMMdd");
                        Date date = sourseFormat.parse(line.get(12));
                        tempConsumer.setBirthDate(String.valueOf(date));
                    }
                    else{
                        tempConsumer.setBirthDate(null);
                    }
                    if(!line.get(3).equals("START_DATE") && !line.get(3).equals("") && !line.get(12).equals("\\N")){
                        DateFormat sourseFormat = new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy");
                        Date date = sourseFormat.parse(line.get(3));
                        tempConsumer.setRegistrationDate(String.valueOf(date));
                    }
                    else{
                        tempConsumer.setRegistrationDate(null);
                    }
                    // tempConsumer.setEmailAddress(line.get(8))
                    tempConsumer.setIdentificationType(line.get(13));
                    tempConsumer.setIdentificationNumber(line.get(14));

                    if(!line.get(15).equals("") && !line.get(15).equals("\\N")){
                        tempConsumer.setNationality(line.get(15));
                    }
                    if(!line.get(16).equals("") && !line.get(16).equals("\\N")){
                        tempConsumer.setBirthPlace(line.get(16));
                    }
                    if(!line.get(17).equals("") && !line.get(17).equals("\\N")){
                        tempConsumer.setIdentityCapturePath(line.get(17));
                    }

                    tempConsumer.setServiceProvider(serviceProvider);
                    java.util.Date date = new java.util.Date();
                    tempConsumer.setCreatedOn(String.valueOf(date));
                    consumers.add(tempConsumer);
                }
                this.checkConsumer(consumers, user, serviceProvider);
                this.consumers.clear();
            }
        } catch (IOException e) {
            log.warn("io error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
            e.printStackTrace();
        } catch (Exception e) {
            log.warn("Ex error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
        }
        // log.warn(file.toString());

        return null;
    }

    public List<List<String>> loadAirtelConsumers(ServiceProvider serviceProvider, User user) {
        this.consumers.clear();
        String fileName = "files/Airtel.csv";
        try (FileReader filereader = new FileReader(fileName)) {

            CSVParser parser = new CSVParserBuilder().withSeparator('|').build();

            CSVReader csvReader = new CSVReaderBuilder(filereader)
            .withSkipLines(1)
            .withCSVParser(parser)
            .build();

            List<List<String>> rows = new ArrayList<>();
            for (String[] nextLine : csvReader) {
                // log.warn(Arrays.asList(nextLine).toString());
                List<String> line = Arrays.asList(nextLine);
                Consumer tempConsumer = new Consumer();
                tempConsumer.setMsisdn(line.get(0));
                if(!line.get(1).equals("") && !line.get(1).equals("\\N")){
                    tempConsumer.setFirstName(line.get(1));
                }

                if(!line.get(2).equals("") && !line.get(2).equals("\\N")){
                    tempConsumer.setLastName(line.get(2));
                }

                if(!line.get(3).equals("") && !line.get(3).equals("\\N")){
                    tempConsumer.setNationality(line.get(3));
                }

                if(!line.get(4).equals("") && !line.get(4).equals("\\N")){
                    tempConsumer.setIdentificationType(line.get(4));
                }

                if(!line.get(5).equals("") && !line.get(5).equals("\\N")){
                    tempConsumer.setIdentificationNumber(line.get(5));
                }
                if(!line.get(6).equals("") && !line.get(6).equals("\\N")){
                    tempConsumer.setGender(line.get(6));
                }
                String address = "";
                List<String> addressList = new ArrayList<>();
                if (!line.get(7).equals("") && !line.get(7).equals("\\N")) {
                    addressList.add(line.get(7));
                }
                if (!line.get(8).equals("") && !line.get(8).equals("\\N")) {
                    addressList.add(line.get(8));
                }
                if (!line.get(9).equals("") && !line.get(9).equals("\\N")) {
                    addressList.add(line.get(9));
                }
                if (!line.get(10).equals("") && !line.get(10).equals("\\N")) {
                    addressList.add(line.get(10));
                }
                if (!line.get(11).equals("") && !line.get(11).equals("\\N")) {
                    addressList.add(line.get(11));
                }
                if(addressList.size() > 0){
                    address = String.join(" ",addressList);
                }
                tempConsumer.setAddress(address);

                if(!line.get(12).equals("") && !line.get(12).equals("\\N")){
                    tempConsumer.setBirthPlace(line.get(12));
                }
                try {
                    if (line.get(13).equals("\\N") || line.get(13).equals("")){
                        tempConsumer.setBirthDate(null);
                    }else {
                        Date birthDate = new SimpleDateFormat("dd-MMM-yy HH.mm.ss.SSSSSS a").parse(line.get(13));
                        tempConsumer.setBirthDate(String.valueOf(birthDate));
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                if(!line.get(16).equals("") && !line.get(16).equals("\\N")){
                    tempConsumer.setIdentityCapturePath(line.get(16));
                }
                Date registrationDate = null;
                try {
                    if (!line.get(17).equals("\\N") && !line.get(17).equals("")){
                        registrationDate = new SimpleDateFormat("dd-MMM-yy HH.mm.ss.SSSSSS a").parse(line.get(17));
                        tempConsumer.setRegistrationDate(String.valueOf(registrationDate));

                    }
                } catch (Exception e) {
                }
                tempConsumer.setServiceProvider(serviceProvider);
                java.util.Date date = new java.util.Date();
                tempConsumer.setCreatedOn(String.valueOf(date));
                consumers.add(tempConsumer);
            }
            this.checkConsumer(consumers, user, serviceProvider);
            this.consumers.clear();
            csvReader.close();
            return rows;
        } catch (IOException e) {
            log.warn("file not found");
        }
        return null;
    }

    private List<List<String>> loadBankConsumers(ServiceProvider serviceProvider, User user) {
        this.consumers.clear();
        String fileLocation = "files/StandardBank.xlsx";
        FileInputStream fileInputStream;
        Date date = null;
        try {
            fileInputStream = new FileInputStream(new File(fileLocation));
            try (Workbook workbook = new XSSFWorkbook(fileInputStream)) {
                workbook.setMissingCellPolicy(MissingCellPolicy.RETURN_BLANK_AS_NULL);
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                while (rowIterator.hasNext()) {
                    List<String> line = new ArrayList<>();
                    Row row = rowIterator.next();
                    //For each row, iterate through all the columns
                    if(row.getRowNum() > DEFAULT_FIRST_ROW){
                        for (int cn = 0; cn < row.getLastCellNum(); cn++) {
                            Cell cell = row.getCell(cn);

                            if (!Objects.isNull(cell)) {
                                if(cn == 7){

                                    if(cell.getCellType().equals(CellType.NUMERIC)) {
                                        line.add(cell.getDateCellValue().toString());
                                        DateFormat sourseFormat = new SimpleDateFormat("E MMM dd HH:mm:ss zzz yyyy");
                                        date = sourseFormat.parse(line.get(7));
                                    } else {
                                        line.add("\\N");
                                    }
                                } else{
                                    cell.setCellType(org.apache.poi.ss.usermodel.CellType.STRING);
                                    line.add(cell.getStringCellValue());
                                }
                            }
                            else{
                                line.add("");
                            }
                        }
                    }
                    if (line.size() == 0) {
                        continue;
                    }
                    Boolean emptyCheck = false;
                    for(int i=0;i<line.size();i++){
                        if(!Objects.equals(line.get(i), "")){
                            emptyCheck = true;
                        }
                        if(line.get(i).equals("\\N")){
                            line.set(i,"");
                        }
                    }
                    if(!emptyCheck){
                        continue;
                    }
                    Consumer tempConsumer = new Consumer();
                    tempConsumer.setMsisdn(line.get(0));
                    //   tempConsumer.setFirstName(line.get(1));
                    if (!line.get(1).equals("") && !line.get(1).equals("\\N")) {
                        tempConsumer.setFirstName(line.get(1));
                    }
                    //   tempConsumer.setLastName(line.get(2) + " " + line.get(3));
                    List<String> lastNameList = new ArrayList<>();
                    if(!line.get(2).equals("") && !line.get(2).equals("\\N")){
                        lastNameList.add(line.get(2));
                    }
                    if(!line.get(3).equals("") && !line.get(3).equals("\\N")){
                        lastNameList.add(line.get(3));
                    }
                    String lastName = "";
                    if(lastNameList.size()>0){
                        lastName = String.join(" ",lastNameList);
                    }
                    tempConsumer.setLastName(lastName);

                    if (line.get(4).toLowerCase().trim().equals("male") || line.get(4).toLowerCase().trim().equals("masculin") || line.get(4).toLowerCase().trim().equals("m")) {
                        tempConsumer.setGender("MALE");
                    } else if (line.get(4).toLowerCase().trim().equals("feminin") || line.get(4).toLowerCase().trim().equals("female") || line.get(4).toLowerCase().trim().equals("f") ) {
                        tempConsumer.setGender("FEMALE");
                    }
                    if (!line.get(5).equals("") && !line.get(5).equals("\\N")) {
                        tempConsumer.setNationality(line.get(5));
                    }
                    if(!line.get(6).equals("") && !line.get(6).equals("\\N")){
                        tempConsumer.setBirthPlace(line.get(6));
                    }
                    if(!line.get(7).equals("") && !line.get(7).equals("\\N")){
                        tempConsumer.setBirthDate(String.valueOf(date));
                    }
                    else{
                        tempConsumer.setBirthDate(null);
                    }
                    if (!line.get(10).equals("\\N") && !line.get(10).equals("")) {
                        tempConsumer.setIdentificationNumber(line.get(10));
                    }
                    //tempConsumer.setIdentificationNumber(line.get(10));
                    if (!line.get(11).equals("") && !line.get(11).equals("\\N")) {
                        tempConsumer.setIdentificationType(line.get(11));
                    }
                    //                    tempConsumer.setIdentificationType(line.get(11));
                    String address = "";
                    List<String> addressList = new ArrayList<>();
                    if (!line.get(12).equals("")) {
                        addressList.add(line.get(12));
                    }
                    if (!line.get(13).equals("")) {
                        addressList.add(line.get(13));
                    }
                    if (!line.get(14).equals("")) {
                        addressList.add(line.get(14));
                    }
                    if (!line.get(15).equals("")) {
                        addressList.add(line.get(15));
                    }
                    if (!line.get(16).equals("")) {
                        addressList.add(line.get(16));
                    }
                    if(addressList.size()>0){
                        address = String.join(" ",addressList);
                    }
                    tempConsumer.setAddress(address);
                    // tempConsumer.setIdentityCapturePath(line.get(18));
                    tempConsumer.setServiceProvider(serviceProvider);
                    java.util.Date currentDate = new java.util.Date();
                    tempConsumer.setCreatedOn(String.valueOf(currentDate));
                    tempConsumer.setIsConsistent(true);
                    if (!line.get(18).equals("") && !line.get(18).equals("\\N")) {
                        tempConsumer.setIdentityCapturePath(line.get(18));
                    }
                    consumers.add(tempConsumer);
                    //                    checkNullAttributesForFile(line);
                }
                this.checkConsumerForBank(consumers, user, serviceProvider);
                this.consumers.clear();
            }
        } catch (IOException e) {
            log.warn("io error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
            e.printStackTrace();
        } catch (Exception e) {
            log.warn("Ex error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
        }
        // log.warn(file.toString());

        return null;
    }







        @Transactional(propagation = Propagation.REQUIRES_NEW)
        public void checkConsumer(List<Consumer> consumers, User user, ServiceProvider serviceProvider) {
            if (consumers == null || consumers.isEmpty()) {
                log.info("checkConsumer start | operator={} consumers=0", serviceProvider.getName());
                return;
            }

            long t0 = System.nanoTime();
            log.info("checkConsumer start | operator={} consumers={}", serviceProvider.getName(), consumers.size());

            // Prevent Hibernate from auto-flushing before every read query inside this tx.
            try { em.setFlushMode(FlushModeType.COMMIT); } catch (Exception ignore) {}

            // Reduce time spent waiting on locks (session scope).
            try { em.createNativeQuery("SET SESSION innodb_lock_wait_timeout = 5").executeUpdate(); } catch (Exception ignore) {}

            // Precompute once (was cheap but do it outside the loop anyway)
            int existingCountForSp = consumerRepository.countByServiceProvider_Id(serviceProvider.getId());

            // Duplicate detection on msisdn (your equals/hashCode are msisdn-based, but make it explicit + null-safe)
            Set<String> seenMsisdn = new HashSet<>(consumers.size() * 2);

            // For “Exceeding” anomaly counting key
            Map<ExceedingConsumers, Integer> exceedMap = new HashMap<>();

            // Buffer writes in batches to avoid per-row I/O
            final int batchSize = 500; // keep <= hibernate.jdbc.batch_size
            List<Consumer> pendingSaves = new ArrayList<>(batchSize);

            // Optional helper your code uses
            AnomalyCollection anomalyCollection = new AnomalyCollection();

            int processed = 0;
            for (Consumer consumer : consumers) {
                long startOne = System.nanoTime();
                try {
                    // -------- Incomplete anomaly check --------
                    consumer.setIsConsistent(Boolean.TRUE);
                    List<String> errors = checkNullAttributesForFile(consumer); // your existing helper

                    if (!errors.isEmpty()) {
                        try {
                            checkConsumerIncompleteAnomaly(consumer, errors, user, existingCountForSp != 0, anomalyCollection);
                        } catch (Exception e) {
                            log.warn("incomplete-anomaly failed msisdn={}", safeMsisdn(consumer), e);
                        }
                    } else {
                    	log.info("complete consumer msisdn={}",consumer.getMsisdn());
                        if (existingCountForSp != 0) {
                            try { resolveIncompleteAnomaly(consumer, user); } catch (Exception e) { log.warn("resolveIncomplete failed msisdn={}", safeMsisdn(consumer), e); }
                            try { softDeleteConsistentUsers(consumer); }   catch (Exception e) { log.warn("softDeleteConsistent failed msisdn={}", safeMsisdn(consumer), e); }
                        }
                        pendingSaves.add(consumer);
                    }

                    // -------- Duplicate anomaly check (by msisdn) --------
                    String keyMsisdn = normalize(consumer.getMsisdn());
                    if (!seenMsisdn.add(keyMsisdn)) {
                        try { tagDuplicateAnomalies(consumer, user); } catch (Exception e) { log.warn("duplicate-anomaly failed msisdn={}", keyMsisdn, e); }
                    } else {
                        try {
                            Consumer updated = resolvedAndSoftDeleteConsumers(consumer, existingCountForSp != 0, user);
                            pendingSaves.add(updated);
                        } catch (Exception e) {
                            log.warn("resolve-old-anomalies failed msisdn={}", keyMsisdn, e);
                        }
                    }

                    // -------- Exceeding anomaly check --------
                    ExceedingConsumers exKey = new ExceedingConsumers();
                    exKey.setServiceProviderName(serviceProvider.getName());
                    exKey.setIdentificationType(Optional.ofNullable(consumer.getIdentificationType()).orElse(""));
                    exKey.setIdentificationNumber(Optional.ofNullable(consumer.getIdentificationNumber()).orElse(""));
                    int newCnt = exceedMap.merge(exKey, 1, Integer::sum);
                    if (newCnt < 3) {
                        try {
                            Consumer updated = resolvedAndDeleteExceedingConsumers(consumer, existingCountForSp != 0, user);
                            pendingSaves.add(updated);
                        } catch (Exception e) {
                            log.warn("resolve-exceeding failed msisdn={}", keyMsisdn, e);
                        }
                    } else if (newCnt == 3) { // first time crossing the threshold
                        try { tagExceedingAnomalies(consumer, user); } catch (Exception e) { log.warn("exceeding-anomaly failed msisdn={}", keyMsisdn, e); }
                    }

                    // -------- Batch flush/clear --------
                    processed++;
                    if (pendingSaves.size() >= batchSize) {
                        consumerRepository.saveAllAndFlush(pendingSaves);
                        pendingSaves.clear();
                        em.clear();
                    }

                    if (processed % 200 == 0) {
                        long ms = (System.nanoTime() - t0) / 1_000_000;
                        log.info("checkConsumer progress | processed={} elapsedMs={}", processed, ms);
                    }


                } catch (Exception ex) {
                    log.error("checkConsumer error msisdn={} sp={} ", safeMsisdn(consumer), serviceProvider.getName(), ex);
                } finally {
                    long durMs = (System.nanoTime() - startOne) / 1_000_000;
                    if (durMs > 5000) {
                        log.warn("slow record msisdn={} took {} ms", safeMsisdn(consumer), durMs);
                    }
                }
            }

            // Final flush
            if (!pendingSaves.isEmpty()) {
                consumerRepository.saveAllAndFlush(pendingSaves);
                pendingSaves.clear();
                em.clear();
            }

            extractedCon();

            anomalyCollection.getParentAnomalyNoteSet().clear();


            //extracted(user);


            long totalMs = (System.nanoTime() - t0) / 1_000_000;
            log.info("checkConsumer done | operator={} processed={} in {} ms", serviceProvider.getName(), processed, totalMs);


        }




    // ======== helpers (same assumptions as your code) ========

        private String safeMsisdn(Consumer c) {
            return c == null ? "null" : String.valueOf(c.getMsisdn());
        }
        private String normalize(String s) { return s == null ? "" : s.trim(); }


    private void checkConsumerIncompleteAnomaly(
            Consumer consumer,
            List<String> errors,
            User user,
            Boolean flag,
            AnomalyCollection collection
    ) {
        Set<String> setForDefaultErrors = new HashSet<>(errors);
        Set<String> setForFileErrors = new HashSet<>(checkNullAttributesForFile(consumer));

        Set<String> combinedErrors = Stream.concat(setForDefaultErrors.stream(), setForFileErrors.stream())
                .collect(Collectors.toSet());
        collection.setParentAnomalyNoteSet(Stream.concat(combinedErrors.stream(), collection.getParentAnomalyNoteSet().stream())
                .collect(Collectors.toSet()));

        String distinctErrors = String.join(", ", combinedErrors);

        Anomaly tempAnomaly = new Anomaly();
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Incomplete Data");

        List<Consumer> tempConsumer = consumerRepository.findConsumerIdsByMsisdnAndIdNumberAndIdTypeAndServiceProviderID(
                consumer.getMsisdn(),
                consumer.getIdentificationType(),
                consumer.getIdentificationNumber(),
                consumer.getServiceProvider().getId()
        );
        List<Long> consumerIds = tempConsumer.stream().map(Consumer::getId).collect(Collectors.toList());
        List<Long> consumerAnomalies = consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(consumerIds, anomalyType.getId());

        // 🔹 Mark inconsistent & set consistentOn
        consumer.setIsConsistent(false);
        String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        if (!combinedErrors.isEmpty()) {
            consumer.setConsistentOn(today);  // anomaly triggered
        } else {
            consumer.setConsistentOn("N/A");
        }
        consumer = consumerRepository.save(consumer);

        tempAnomaly.setNote("Missing Mandatory Fields: " + distinctErrors);
        ConsumerAnomaly tempCA = new ConsumerAnomaly();

        if (!consumerAnomalies.isEmpty()) {
            Anomaly anomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());
            if (anomaly != null) {
                if (anomaly.getStatus().getCode() == 4) {
                    anomaly.setStatus(AnomalyStatus.RESOLVED_FULLY);
                    anomalyRepository.save(anomaly);

                    AnomalyTracking anomalyTracking = new AnomalyTracking(
                            anomaly, new Date(), AnomalyStatus.RESOLVED_FULLY, "",
                            user.getFirstName() + " " + user.getLastName(),
                            anomaly.getUpdatedOn()
                    );
                    anomalyTrackingRepository.save(anomalyTracking);
                    
                    consumerTrackingRepository.save(new ConsumerTracking(consumer.getId(),consumer.getServiceProvider(),LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),true,new Date()));

                    tempAnomaly.setStatus(AnomalyStatus.REPORTED);
                    tempAnomaly.addConsumer(consumer);
                    tempAnomaly.setReportedOn(new Date());
                    tempAnomaly.setReportedBy(user);
                    tempAnomaly.setAnomalyType(anomalyType);
                    tempAnomaly.setUpdatedOn(new Date());
                    tempAnomaly.setUpdateBy(user.getFirstName() + " " + user.getLastName());

                    anomalyRepository.save(tempAnomaly);

                    anomalyTracking = new AnomalyTracking(
                            anomaly, new Date(), AnomalyStatus.REPORTED, "",
                            user.getFirstName() + " " + user.getLastName(),
                            anomaly.getUpdatedOn()
                    );
                    anomalyTrackingRepository.save(anomalyTracking);
                    
                    
                }

                if (anomaly.getStatus().getCode() == 0 || anomaly.getStatus().getCode() == 1 ||
                        anomaly.getStatus().getCode() == 2 || anomaly.getStatus().getCode() == 3) {

                    tempAnomaly.setId(anomaly.getId());
                    tempAnomaly.setStatus(anomaly.getStatus());
                    tempAnomaly.addConsumer(consumer);
                    tempAnomaly.setReportedOn(anomaly.getReportedOn());
                    tempAnomaly.setReportedBy(user);
                    tempAnomaly.setAnomalyType(anomalyType);
                    tempAnomaly.setUpdatedOn(anomaly.getUpdatedOn());
                    tempAnomaly.setUpdateBy(user.getFirstName() + " " + user.getLastName());

                    tempCA.setAnomaly(tempAnomaly);
                    tempCA.setConsumer(consumer);

                    if (!anomaly.getNote().equals(collection.getParentAnomalyNoteSet().toString())) {
                        anomaly.setNote("Missing Mandatory Fields are: " + collection.getParentAnomalyNoteSet());
                        anomalyRepository.save(anomaly);
                    }

                    tempCA.setNotes("Missing Mandatory Fields are: " + distinctErrors);
                    consumerAnomalyRepository.save(tempCA);
                    
                    List<ConsumerTracking> consumerTracking = consumerTrackingRepository.findByConsumerId(consumer.getId());
                    if(consumerTracking.size() <= 0) {
                    	consumerTrackingRepository.save(new ConsumerTracking(consumer.getId(),consumer.getServiceProvider(),"N/A",false,new Date()));
                    }
                }
            }
        } else {
            tempAnomaly.setStatus(AnomalyStatus.REPORTED);
            tempAnomaly.setReportedOn(new Date());
            tempAnomaly.setReportedBy(user);
            tempAnomaly.setAnomalyType(anomalyType);
            tempAnomaly.setUpdatedOn(new Date());
            tempAnomaly.setUpdateBy(user.getFirstName() + " " + user.getLastName());

            Anomaly savedAnomaly = anomalyRepository.save(tempAnomaly);

            AnomalyTracking anomalyTracking = new AnomalyTracking(
                    savedAnomaly, new Date(), AnomalyStatus.REPORTED, "",
                    user.getFirstName() + " " + user.getLastName(),
                    savedAnomaly.getUpdatedOn()
            );
            anomalyTrackingRepository.save(anomalyTracking);

            ConsumerAnomaly consumerAnomaly = new ConsumerAnomaly();
            consumerAnomaly.setNotes("Missing Mandatory Fields are: " + distinctErrors);
            consumerAnomaly.setAnomaly(savedAnomaly);
            consumerAnomaly.setConsumer(consumer);

            consumerAnomalyRepository.save(consumerAnomaly);
            List<ConsumerTracking> consumerTracking = consumerTrackingRepository.findByConsumerId(consumer.getId());
            if(consumerTracking.size() <= 0) {
            	consumerTrackingRepository.save(new ConsumerTracking(consumer.getId(),consumer.getServiceProvider(),"N/A",false,new Date()));
            }
        }

        // 🔹 soft deleted old consumers
        if (flag) {
            if ((consumerAnomalies.size() == 0 || consumerAnomalies.size() == 1) && consumerIds.size() == 1) {
                for (Long id : consumerIds) {
                    consumerRepository.updatePreviousConsumersStatus(1, id);
                }
            }
        }
    }

    private void resolveIncompleteAnomaly(Consumer consumer,User user){
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Incomplete Data");

        List<Consumer> tempConsumer = consumerRepository.findConsumerIdsByMsisdnAndIdNumberAndIdTypeAndServiceProviderID(
                consumer.getMsisdn(), consumer.getIdentificationType(),
                consumer.getIdentificationNumber(), consumer.getServiceProvider().getId());
        List<Long> consumerIds = tempConsumer.stream().map(Consumer::getId).collect(Collectors.toList());

        Anomaly tempAnomaly = new Anomaly();

        //previously tagged anomalies
        List<Long> consumerAnomalies = consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(consumerIds, anomalyType.getId());
        if (consumerAnomalies.size() > 0) {
            //get anomaly for duplicate that is tagged previously
            Anomaly anomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());
            if (!Objects.isNull(anomaly)) {
                //if status is resolution submitted
                if (anomaly.getStatus().getCode() == 4) {
                    //resolved old anomalies
                    anomaly.setStatus(AnomalyStatus.RESOLVED_FULLY);
                    anomalyRepository.save(anomaly);
                    AnomalyTracking anomalyTracking = new AnomalyTracking(anomaly, new Date(), AnomalyStatus.RESOLVED_FULLY, "", user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
                    anomalyTrackingRepository.save(anomalyTracking);
                    
                    consumerTrackingRepository.save(new ConsumerTracking(consumer.getId(),consumer.getServiceProvider(),LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),true,new Date()));
                }
                if (anomaly.getStatus().getCode() == 0 || anomaly.getStatus().getCode() == 1 ||
                        anomaly.getStatus().getCode() == 2 || anomaly.getStatus().getCode() == 3) {
                    ConsumerAnomaly tempConsumerAnomaly = new ConsumerAnomaly();
                    tempAnomaly.setId(anomaly.getId());
                    tempAnomaly.setNote(anomaly.getNote());
                    tempAnomaly.setStatus(anomaly.getStatus());
                    tempAnomaly.setReportedOn(anomaly.getReportedOn());
                    tempAnomaly.setReportedBy(anomaly.getReportedBy());
                    tempAnomaly.getConsumers().remove(consumer);
                    tempAnomaly.addConsumer(consumer);
                    tempAnomaly.setUpdatedOn(anomaly.getUpdatedOn());
                    tempAnomaly.setAnomalyType(anomalyType);
                    tempConsumerAnomaly.setAnomaly(tempAnomaly);

                    consumer = consumerRepository.save(consumer);

                    tempConsumerAnomaly.setConsumer(consumer);
                    tempConsumerAnomaly.setNotes(anomaly.getNote());

                    consumerAnomalyRepository.save(tempConsumerAnomaly);
                    
                    consumerTrackingRepository.save(new ConsumerTracking(consumer.getId(),consumer.getServiceProvider(),"N/A",false,new Date()));
                }
            }
        }
        if ((/*consumerAnomalies.size() == 0 || */consumerAnomalies.size() == 1) && consumerIds.size() == 1) {
            for (int i = 0; i < consumerIds.size(); i++) {
                consumerRepository.updatePreviousConsumersStatus(1, consumerIds.get(i));
            }
        }
    }





    @Transactional
    private Consumer resolvedAndSoftDeleteConsumers(Consumer consumer, Boolean flag, User user) {
        if (consumer == null) return null;

        System.out.println("resolvedAndSoftDeleteConsumers values are: " + consumer.getMsisdn());

        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Duplicate Records");

        List<Long> consumerIds = new ArrayList<>();
        consumerIds.addAll(consumerRepository.findConsumerIdsByMsisdnAndConsumerStatus(consumer.getMsisdn(), 0));
        consumerIds.addAll(consumerRepository.findConsumerIdsByMsisdnAndConsumerStatus(consumer.getMsisdn(), 1));

        List<Long> consumerAnomalies =
                consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(consumerIds, anomalyType.getId());

        if (!consumerAnomalies.isEmpty()) {
            Anomaly anomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());

            if (anomaly != null) {
                consumer = consumerRepository.save(consumer);

                String note = anomaly.getNote() != null ? anomaly.getNote()
                        : "Duplicate Anomaly: You can't have more than one active record per MSISDN: " + consumer.getMsisdn();

                safeLinkConsumerToAnomaly(consumer, anomaly, note);

                // ✅ Only check resolution if status == 4 (submitted) or 5 (partially resolved)
                if (anomaly.getStatus().getCode() == 4 || anomaly.getStatus().getCode() == 5) {
                    AnomalyStatus newStatus = resolveAnomalyStatus(anomaly);

                    if (newStatus != anomaly.getStatus()) {
                        anomaly.setStatus(newStatus);
                        anomaly.setUpdatedOn(new Date());
                        anomalyRepository.save(anomaly);

                        anomalyTrackingRepository.save(
                                new AnomalyTracking(anomaly, new Date(), newStatus, "",
                                        user.getFirstName() + " " + user.getLastName(),
                                        anomaly.getUpdatedOn())
                        );
                        
                        consumerTrackingRepository.save(new ConsumerTracking(consumer.getId(),consumer.getServiceProvider(),LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),true,new Date()));
                    }
                }
            }
        }

        consumerIds.remove(consumer.getId());

        if (Boolean.TRUE.equals(flag) && consumerIds.size() <= 2) {
            if (consumerAnomalies.size() > 1 && consumerIds.size() > 1) {
                for (Long id : consumerIds) {
                    if (!Objects.equals(id, consumer.getId())) {
                        consumerRepository.updatePreviousConsumersStatus(1, id);
                    }
                }
            }
        }

        return consumer;
    }






    private AnomalyStatus resolveAnomalyStatus(Anomaly anomaly) {
        List<ConsumerAnomaly> links = consumerAnomalyRepository.findByAnomaly_Id(anomaly.getId());
        if (links == null || links.isEmpty()) {
            return anomaly.getStatus();
        }

        boolean allTrue = links.stream()
                .map(link -> link.getConsumer().getIsConsistent())
                .filter(Objects::nonNull)
                .allMatch(Boolean::booleanValue);

        boolean allFalse = links.stream()
                .map(link -> link.getConsumer().getIsConsistent())
                .filter(Objects::nonNull)
                .noneMatch(Boolean::booleanValue);

        if (allTrue) {
            return AnomalyStatus.RESOLVED_FULLY; // code 6
        } else if (!allTrue && !allFalse) {
            return AnomalyStatus.RESOLVED_PARTIALLY; // code 5
        } else {
            return AnomalyStatus.RESOLVED_PARTIALLY; // code 4, all false
        }
    }



    private AnomalyStatus resolveAnomalyStatusV1(Anomaly anomaly) {
        List<ConsumerAnomaly> links = consumerAnomalyRepository.findByAnomaly_Id(anomaly.getId());
        if (links == null || links.isEmpty()) {
            return anomaly.getStatus();
        }

        boolean allTrue = links.stream()
                .map(link -> link.getConsumer().getIsConsistent())
                .filter(Objects::nonNull)
                .allMatch(Boolean::booleanValue);

        boolean allFalse = links.stream()
                .map(link -> link.getConsumer().getIsConsistent())
                .filter(Objects::nonNull)
                .noneMatch(Boolean::booleanValue);

        if (allTrue) {
            // ✅ all consistent → set date
            links.forEach(link -> {
                Consumer c = link.getConsumer();
                if (Boolean.TRUE.equals(c.getIsConsistent())) {
                    c.setConsistentOn(LocalDate.now().toString());
                    consumerRepository.save(c);
                }
            });
            return AnomalyStatus.RESOLVED_FULLY; // code 6

        } else if (!allTrue && !allFalse) {
            // ✅ mix → stamp date for consistent, N/A for inconsistent
            links.forEach(link -> {
                Consumer c = link.getConsumer();
                if (Boolean.TRUE.equals(c.getIsConsistent())) {
                    c.setConsistentOn(LocalDate.now().toString());
                } else {
                    c.setConsistentOn("N/A");
                }
                consumerRepository.save(c);
            });
            return AnomalyStatus.RESOLVED_PARTIALLY; // code 5

        } else {
            // ✅ all inconsistent → set N/A
            links.forEach(link -> {
                Consumer c = link.getConsumer();
                c.setConsistentOn("N/A");
                consumerRepository.save(c);
            });
            return AnomalyStatus.RESOLVED_PARTIALLY; // code 4
        }
    }



    private static boolean hasText(String s) { return s != null && !s.trim().isEmpty(); }
    private String norm(String s) { return s == null ? null : s.trim(); }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }





    private List<String> checkErrors(Consumer consumer) {
        List<String> errors = new ArrayList<>();
        if (consumer.getMsisdn() == null) {
            errors.add("MSISDN");
        }
        if (consumer.getSubscriberType() == null) {
            errors.add("Subscriber Type");
        }
        if (consumer.getFirstName() == null) {
            errors.add("firstName");
        }
        if (consumer.getLastName() == null) {
            errors.add("lastName");
        }
        if (consumer.getGender() == null) {
            errors.add("gender");
        }
        if (consumer.getAddress() == null) {
            errors.add("address");
        }
        if (consumer.getNationality() == null) {
            errors.add("nationality");
        }
        if (consumer.getIdentificationNumber() == null) {
            errors.add("code");
        }
        if (consumer.getIdentificationType() == null) {
            errors.add("typePiece");
        }
        if (consumer.getIdentityCapturePath() == null) {
            errors.add("fullPath");
        }
        if (consumer.getIdentityValitidyDate() == null) {
            errors.add("Identification Validity Date");
        }
        if (consumer.getRegistrationDate() == null) {
            errors.add("Registration Date");
        }
        return errors;
    }


    private List<String> checkNullAttributesForFile(Consumer consumer){
        List <String> nullAttributesOfFile = new ArrayList<>();
        if (consumer.getMsisdn() == null || consumer.getMsisdn().equals("")) {
            nullAttributesOfFile.add("MSISDN");
        }

        if (consumer.getRegistrationDate() == null || consumer.getRegistrationDate().equals("")) {
            nullAttributesOfFile.add("Registration Date");
        }

        if (consumer.getFirstName() == null || consumer.getFirstName().equals("")) {
            nullAttributesOfFile.add("firstName");
        }

        if (consumer.getMiddleName() == null || consumer.getMiddleName().equals("")) {
            nullAttributesOfFile.add("middleName");
        }
        
        if (consumer.getLastName() == null || consumer.getLastName().equals("")) {
            nullAttributesOfFile.add("lastName");
        }
        if (consumer.getGender() == null || consumer.getGender().equals("")) {
            nullAttributesOfFile.add("gender");
        }

        if (consumer.getBirthPlace() == null || consumer.getBirthPlace().equals("")) {
            nullAttributesOfFile.add("birthPlace");
        }
        if (consumer.getBirthDate() == null || consumer.getBirthDate().equals("")) {
            nullAttributesOfFile.add("birthday");
        }

        if (consumer.getAddress() == null || consumer.getAddress().equals("")) {
            nullAttributesOfFile.add("address");
        }

        if (consumer.getAlternateMsisdn1() == null || consumer.getAlternateMsisdn1().equals("")) {
            nullAttributesOfFile.add("AlternateMsisdn1");
        }

        if (consumer.getAlternateMsisdn2() == null || consumer.getAlternateMsisdn2().equals("")) {
            nullAttributesOfFile.add("AlternateMsisdn2");
        }
        if (consumer.getIdentificationNumber() == null || consumer.getIdentificationNumber().equals("")) {
            nullAttributesOfFile.add("IdentificationNumber");
        }
        if (consumer.getIdentificationType() == null || consumer.getIdentificationType().equals("")) {
            nullAttributesOfFile.add("identificationType");
        }

        if (consumer.getAddress() == null || consumer.getAddress().equals("")) {
            nullAttributesOfFile.add("address");
        }

        if (consumer.getCreatedOn() == null || consumer.getCreatedOn().equals("")) {
            nullAttributesOfFile.add("createdOn");
        }
        return nullAttributesOfFile;
        
        
    }
    
    private void softDeleteConsistentUsers(Consumer consumer){
        
        List<Consumer> tempConsumer = consumerRepository.findConsumerIdsByMsisdnAndIdNumberAndIdTypeAndServiceProviderID(
            consumer.getMsisdn(), consumer.getIdentificationType(),
            consumer.getIdentificationNumber(), consumer.getServiceProvider().getId());
        List<Long> consumerIds = tempConsumer.stream().map(Consumer::getId).collect(Collectors.toList());
        List<Long> consumerAnomalies = consumerAnomalyRepository.findAnomaliesIdByConsumer(consumerIds);

        if (consumerAnomalies.size() ==0 && consumerIds.size() == 1) {
            for (int i = 0; i < consumerIds.size(); i++) {
                consumerRepository.updatePreviousConsumersStatus(1, consumerIds.get(i));
            }
        }
    }

    private void checkConsumerForBank(List<Consumer> consumers, User user, ServiceProvider serviceProvider) {
        Set<Consumer> consumerSet = new HashSet<>();

        AnomalyCollection anomalyCollection=new AnomalyCollection();

        int count = consumerRepository.countByServiceProvider_Id(serviceProvider.getId());
        if (consumers != null) {
            // this loop for duplicate consumers
            consumers.forEach((consumer) -> {
                consumer.setIsConsistent(true);

                // duplicate anomaly
                if (!consumerSet.add(consumer)) {
                    this.tagDuplicateAnomalies(consumer, user);
                } else {
                    consumer = this.resolvedAndSoftDeleteConsumers(consumer, count != 0,user);
                    consumerRepository.save(consumer);
                }
            });
        }
        anomalyCollection.getParentAnomalyNoteSet().clear();
    }
    
    @Override
    public List<Object[]> getConsumersbyServiceProvider (Collection<Long> serviceProviderIds, Date createdOnStart, Date createdOnEnd){
       return consumerRepository.getConsumersByServiceProvider(serviceProviderIds, createdOnStart, createdOnEnd);
    }
    
    public List<AnomalyStatus> setStatusList(AnomalyStatus status){
    	List<AnomalyStatus>  anomalyStatus = new ArrayList<AnomalyStatus>();
    	if (status != null) {
			switch (status) {
			case REPORTED:
				anomalyStatus.add(AnomalyStatus.REPORTED);
				break;

			case QUESTION_SUBMITTED:
				anomalyStatus.add(AnomalyStatus.QUESTION_SUBMITTED);
				break;

			case UNDER_INVESTIGATION:
				anomalyStatus.add(AnomalyStatus.UNDER_INVESTIGATION);
				break;

			case QUESTION_ANSWERED:
				anomalyStatus.add(AnomalyStatus.QUESTION_ANSWERED);
				break;

			case RESOLUTION_SUBMITTED:
				anomalyStatus.add(AnomalyStatus.RESOLUTION_SUBMITTED);
				break;
				
			case RESOLVED_PARTIALLY:
				anomalyStatus.add(AnomalyStatus.RESOLVED_PARTIALLY);
				break;
				
			case RESOLVED_FULLY:
				anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);
				break;
				
			case WITHDRAWN:
				anomalyStatus.add(AnomalyStatus.WITHDRAWN);
				break;

			default:
				anomalyStatus.addAll(Arrays.asList(AnomalyStatus.REPORTED, AnomalyStatus.QUESTION_SUBMITTED,
						AnomalyStatus.UNDER_INVESTIGATION, AnomalyStatus.QUESTION_ANSWERED,
						AnomalyStatus.RESOLUTION_SUBMITTED,AnomalyStatus.RESOLVED_PARTIALLY,AnomalyStatus.RESOLVED_FULLY,AnomalyStatus.WITHDRAWN));
				break;
			}
		} 

    	return anomalyStatus;
    }
    
    
    public List<AnomalyStatus> setResolution(String resolution){
    	//unResolve - {Reported , Inprogress, Question Submitted, Question answered, Resolution submitted}
        //resolve - {Resolve successfully}
        //withdraw - {Withdrawn}
    	List<AnomalyStatus>  anomalyStatus = new ArrayList<AnomalyStatus>();
    	if (resolution != null) {
			switch (resolution) {
			case "resolve":
				anomalyStatus.add(AnomalyStatus.RESOLVED_FULLY);
				break;

			case "unResolve":
				anomalyStatus.addAll(Arrays.asList(AnomalyStatus.REPORTED, 
						AnomalyStatus.UNDER_INVESTIGATION,
						AnomalyStatus.QUESTION_SUBMITTED,
						AnomalyStatus.QUESTION_ANSWERED,
						AnomalyStatus.RESOLUTION_SUBMITTED,
						AnomalyStatus.RESOLVED_PARTIALLY));
				break;

			case "withdrawn":
				anomalyStatus.add(AnomalyStatus.WITHDRAWN);
				break;
			}
		} 
    	return anomalyStatus;
    }


    @Transactional
    public void extractedCon() {
        List<Consumer> consumerList = consumerRepository.findAll();

        if (consumerList.isEmpty()) {
            log.info("No consumers found for consistency check");
            return;
        }

        log.info("Running consistency check for {} consumers", consumerList.size());

        List<Consumer> toUpdate = new ArrayList<>();

        for (Consumer consumer : consumerList) {
            try {
                // ✅ Use centralized logic from ConsumerServiceImpl
                updateConsistencyFlag(consumer);
                toUpdate.add(consumer);
            } catch (Exception e) {
                log.warn("Failed consistency check for consumer id={} msisdn={}",
                        consumer.getId(), consumer.getMsisdn(), e);
            }
        }

        if (!toUpdate.isEmpty()) {
            consumerRepository.saveAllAndFlush(toUpdate);
            log.info("Consistency flags updated for {} consumers", toUpdate.size());
        }
    }


    /**
     * Marks consumer consistent or inconsistent based on:
     *  - missing mandatory fields (msisdn, registrationDate, firstName, lastName, address, alt msisdns, etc.)
     *  - duplicate msisdn
     *  - duplicate ID number + type
     */
    public void updateConsistencyFlag(Consumer consumer) {
        System.out.println("updateConsistencyFlag values :  "+consumer.getMsisdn() +" inconsigtent value "+consumer.getIsConsistent());
        System.out.println("updateConsistencyFlag getConsistentOn :  "+consumer.getConsistentOn());

        boolean consistent = true;
        List<String> reasons = new ArrayList<>();

        // 🔹 Rule 1: Mandatory fields
        if (isNullOrEmpty(consumer.getMsisdn())) reasons.add("MSISDN is null/empty");
        if (isNullOrEmpty(consumer.getRegistrationDate())) reasons.add("RegistrationDate is null/empty");
        if (isNullOrEmpty(consumer.getFirstName())) reasons.add("FirstName is null/empty");
        if (isNullOrEmpty(consumer.getLastName())) reasons.add("LastName is null/empty");
        if (isNullOrEmpty(consumer.getMiddleName())) reasons.add("MiddleName is null/empty");
        if (isNullOrEmpty(consumer.getGender())) reasons.add("Gender is null/empty");
        if (isNullOrEmpty(consumer.getBirthDate())) reasons.add("BirthDate is null/empty");
        if (isNullOrEmpty(consumer.getBirthPlace())) reasons.add("BirthPlace is null/empty");
        if (isNullOrEmpty(consumer.getAddress())) reasons.add("Address is null/empty");
        if (isNullOrEmpty(consumer.getIdentificationType())) reasons.add("IdentificationType is null/empty");
        if (isNullOrEmpty(consumer.getIdentificationNumber())) reasons.add("IdentificationNumber is null/empty");
        if (isNullOrEmpty(consumer.getAlternateMsisdn1())) reasons.add("AlternateMsisdn1 is null/empty");
        if (isNullOrEmpty(consumer.getAlternateMsisdn2())) reasons.add("AlternateMsisdn2 is null/empty");

        // If any mandatory field missing
        if (!reasons.isEmpty()) {
            consistent = false;
        }

        // 🔹 Rule 2: Duplicate MSISDN
        if (consistent && consumer.getMsisdn() != null) {
            long count = consumerRepository.countByMsisdn(consumer.getMsisdn());
            if (count > 1) {
                consistent = false;
                reasons.add("Duplicate MSISDN found: " + consumer.getMsisdn());
            }
        }

        // 🔹 Rule 3: Duplicate ID (idNumber + idType)
        if (consistent && consumer.getIdentificationNumber() != null && consumer.getIdentificationType() != null) {
            long count = consumerRepository.countByIdentificationNumberAndIdentificationType(
                    consumer.getIdentificationNumber(),
                    consumer.getIdentificationType()
            );
            if (count > 2) {
                consistent = false;
                reasons.add("Duplicate ID+Type found: " +
                        consumer.getIdentificationNumber() + " / " + consumer.getIdentificationType());
            }
        }

        consumer.setIsConsistent(consistent);

        if (!consistent) {
            consumer.setConsistentOn("N/A");
            log.warn("Consumer id={} marked INCONSISTENT → reasons={}",
                    consumer.getId(), String.join("; ", reasons));
        } else {
            // Only set date if not already stamped
            if (consumer.getConsistentOn() == null || "N/A".equalsIgnoreCase(consumer.getConsistentOn())) {
                consumer.setConsistentOn(LocalDate.now().toString());
            }
            log.info("Consumer id={} is CONSISTENT", consumer.getId());
        }


        System.out.println("updateConsistencyFlag values finishing  :  "+consumer.getMsisdn() +" inconsigtent value "+consumer.getIsConsistent());
        System.out.println("updateConsistencyFlag getConsistentOn finishing :  "+consumer.getConsistentOn());

    }


    private boolean isNullOrEmpty(String s) {
        return (s == null || s.trim().isEmpty());
    }




    private boolean isEmpty(String s) {
        return (s == null || s.trim().isEmpty());
    }


    private void safeLinkConsumerToAnomaly(Consumer consumer, Anomaly anomaly, String fullNote) {
        if (consumer == null || anomaly == null) return;

        try {
            // Fetch all existing links (should be max 1, but safe in case old duplicates exist)
            List<ConsumerAnomaly> links =
                    consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(anomaly.getId(), consumer.getId());

            if (links != null && !links.isEmpty()) {
                ConsumerAnomaly existing = links.get(0);

                // Update note only if changed
                if (!Objects.equals(existing.getNotes(), fullNote)) {
                    existing.setNotes(fullNote);
                    consumerAnomalyRepository.save(existing);
                }

                // If multiple duplicates exist from old runs, clean them
                if (links.size() > 1) {
                    for (int i = 1; i < links.size(); i++) {
                        consumerAnomalyRepository.delete(links.get(i));
                    }
                }
            } else {
                ConsumerAnomaly link = new ConsumerAnomaly();
                link.setAnomaly(anomaly);
                link.setConsumer(consumer);
                link.setNotes(fullNote);
                consumerAnomalyRepository.save(link);
                
                consumerTrackingRepository.save(new ConsumerTracking(consumer.getId(),consumer.getServiceProvider(),"N/A",false,new Date()));
            }
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate anomaly link already exists for consumer={} anomaly={}. Ignoring insert.",
                    consumer.getId(), anomaly.getId());
            em.clear(); // reset session so Hibernate won’t blow up later
        } catch (Exception e) {
            log.error("safeLinkConsumerToAnomaly failed consumer={} anomaly={} note={}",
                    consumer.getId(), anomaly.getId(), fullNote, e);
            em.clear();
        }
    }



    @Transactional
    private Anomaly tagExceedingAnomalies(Consumer consumer, User user) {
        if (consumer == null) return null;

        final String idType   = norm(consumer.getIdentificationType());
        final String idNumber = norm(consumer.getIdentificationNumber());
        final ServiceProvider sp = consumer.getServiceProvider();
        if (!hasText(idType) || !hasText(idNumber) || sp == null) return null;

        List<Consumer> candidates =
                consumerRepository.findByIdKeyNormalizedAnyStatus(sp.getId(), idType, idNumber);

        if (candidates.stream().noneMatch(c -> Objects.equals(c.getId(), consumer.getId()))) {
            candidates.add(consumer);
        }

        // --- Not exceeding ---
        if (candidates.size() <= 2) {
            return null;
        }

        // --- Exceeding detected ---

        candidates.forEach(c -> {
            c.setIsConsistent(false);
            //c.setConsistentOn(today);
        });
        consumerRepository.saveAll(candidates); // persist all updated

        List<Long> ids = candidates.stream().map(Consumer::getId).collect(Collectors.toList());

        final String spName = sp.getName() == null ? "" : sp.getName();
        final String note = "Exceeding Anomaly: You can't have more than two active records per operator "
                + "for a given combination of (ID Card Type + ID Number + ServiceProviderName): ("
                + idType + " + " + idNumber + " + " + spName + ")";

        final AnomalyType type = anomalyTypeRepository.findFirstByName("Exceeding Threshold");
        final Date now = new Date();

        List<Long> existing = consumerAnomalyRepository
                .findAnomaliesIdByConsumerAndAnomalyTypeId(ids, type.getId());

        Anomaly anomaly;
        if (existing == null || existing.isEmpty()) {
            anomaly = new Anomaly();
            anomaly.setNote(note);
            anomaly.setStatus(AnomalyStatus.REPORTED);
            anomaly.setReportedOn(now);
            anomaly.setReportedBy(user);
            anomaly.setUpdatedOn(now);
            anomaly.setAnomalyType(type);
            anomaly.setUpdateBy(user.getFirstName() + " " + user.getLastName());
            anomaly = anomalyRepository.save(anomaly);

            anomalyTrackingRepository.save(
                    new AnomalyTracking(anomaly, now, AnomalyStatus.REPORTED, "",
                            user.getFirstName() + " " + user.getLastName(), now)
            );
        } else {
            anomaly = anomalyRepository.findFirstByIdInAndAnomalyType_Id(existing, type.getId())
                    .orElse(null);
            if (anomaly == null) return null;
        }

        for (Consumer c : candidates) {
            safeLinkConsumerToAnomaly(c, anomaly, note);
        }

        consumerRepository.markConsumersConsistent(0, ids);
        return anomaly;
    }



    @Transactional
    private Anomaly tagDuplicateAnomalies(Consumer consumer, User user) {
        if (consumer == null) return null;

        String msisdn = normalize(consumer.getMsisdn());
        if (!hasText(msisdn)) return null;

        List<Consumer> candidates =
                consumerRepository.findByMsisdnAndServiceProvider_Id(msisdn, consumer.getServiceProvider().getId());

        if (candidates.stream().noneMatch(c -> Objects.equals(c.getId(), consumer.getId()))) {
            candidates.add(consumer);
        }

        if (candidates.size() <= 1) return null;

        candidates.forEach(c -> c.setIsConsistent(false));
        consumerRepository.save(consumer);

        List<Long> ids = candidates.stream().map(Consumer::getId).collect(Collectors.toList());

        final String note = "Duplicate Anomaly: You can't have more than one active record per MSISDN: " + msisdn;
        final AnomalyType type = anomalyTypeRepository.findFirstByName("Duplicate Records");
        final Date now = new Date();

        List<Long> existing = consumerAnomalyRepository
                .findAnomaliesIdByConsumerAndAnomalyTypeId(ids, type.getId());

        Anomaly anomaly;
        if (existing == null || existing.isEmpty()) {
            anomaly = new Anomaly();
            anomaly.setNote(note);
            anomaly.setStatus(AnomalyStatus.REPORTED);
            anomaly.setReportedOn(now);
            anomaly.setReportedBy(user);
            anomaly.setUpdatedOn(now);
            anomaly.setAnomalyType(type);
            anomaly.setUpdateBy(user.getFirstName() + " " + user.getLastName());
            anomaly = anomalyRepository.save(anomaly);

            anomalyTrackingRepository.save(
                    new AnomalyTracking(anomaly, now, AnomalyStatus.REPORTED, "",
                            user.getFirstName() + " " + user.getLastName(), now)
            );
        } else {
            anomaly = anomalyRepository.findFirstByIdInAndAnomalyType_Id(existing, type.getId())
                    .orElse(null);
            if (anomaly == null) return null;
        }

        for (Consumer c : candidates) {
            safeLinkConsumerToAnomaly(c, anomaly, note);
        }

        consumerRepository.markConsumersConsistent(0, ids);
        return anomaly;
    }


    private Consumer resolvedAndDeleteExceedingConsumers(Consumer consumer, Boolean flag, User user) {
        // Exceeding records
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Exceeding Threshold");

        List<Consumer> duplicateConsumers = consumerRepository
                .findByIdentificationTypeAndIdentificationNumberAndServiceProviderAndConsumerStatusIn(
                        consumer.getIdentificationType(),
                        consumer.getIdentificationNumber(),
                        consumer.getServiceProvider(),
                        Arrays.asList(0, 1)
                );

        List<Long> consumerIds = duplicateConsumers.stream()
                .map(Consumer::getId)
                .collect(Collectors.toList());

        // Previously tagged anomalies
        List<Long> consumerAnomalies =
                consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(consumerIds, anomalyType.getId());

        if (!consumerAnomalies.isEmpty()) {
            Anomaly anomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());

            if (anomaly != null) {
                // ✅ Already resolved anomalies
                if (anomaly.getStatus().getCode() == 4 || anomaly.getStatus().getCode() == 5) {
                    AnomalyStatus newStatus = resolveAnomalyStatus(anomaly);

                    if (newStatus != anomaly.getStatus()) {
                        anomaly.setStatus(newStatus);
                        anomaly.setUpdatedOn(new Date());
                        anomalyRepository.save(anomaly);

                        anomalyTrackingRepository.save(
                                new AnomalyTracking(
                                        anomaly,
                                        new Date(),
                                        newStatus,
                                        "",
                                        user.getFirstName() + " " + user.getLastName(),
                                        anomaly.getUpdatedOn()
                                )
                        );
                    }
                    LocalDateTime now = LocalDateTime.now();
                    String formattedDate = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    consumer.setConsistentOn(formattedDate);
                    consumer = consumerRepository.save(consumer);
                }

                // ✅ Active anomalies (0–3 codes)
                if (anomaly.getStatus().getCode() == 0 || anomaly.getStatus().getCode() == 1 ||
                        anomaly.getStatus().getCode() == 2 || anomaly.getStatus().getCode() == 3) {

                    ConsumerAnomaly tempConsumerAnomaly = new ConsumerAnomaly();
                    Anomaly tempAnomaly = new Anomaly();

                    tempAnomaly.setId(anomaly.getId());
                    tempAnomaly.setNote(anomaly.getNote());
                    tempAnomaly.setStatus(anomaly.getStatus());
                    tempAnomaly.setReportedOn(anomaly.getReportedOn());
                    tempAnomaly.setReportedBy(anomaly.getReportedBy());
                    tempAnomaly.setAnomalyType(anomalyType);
                    tempAnomaly.setUpdatedOn(anomaly.getUpdatedOn());

                    tempConsumerAnomaly.setAnomaly(tempAnomaly);

                    // ✅ mark consumer inconsistent & consistentOn = "N/A"
                    consumer.setIsConsistent(false);
                    consumer.setConsistentOn("N/A");
                    consumer = consumerRepository.save(consumer);

                    tempConsumerAnomaly.setConsumer(consumer);
                    tempConsumerAnomaly.setNotes(anomaly.getNote());

                    consumerAnomalyRepository.save(tempConsumerAnomaly);
                }


            }
        }

        // ✅ Soft delete older consumers
        if (flag && consumerIds.size() > 2) {
            for (Long cId : consumerIds) {
                if (!Objects.equals(cId, consumer.getId())) {
                    consumerRepository.updatePreviousConsumersStatus(1, cId);
                }
            }
        }

        return consumer;
    }


}
