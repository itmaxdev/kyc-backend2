package com.app.kyc.service;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.model.*;
import com.app.kyc.util.AnomalyCollection;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.AnomalyTracking;
import com.app.kyc.entity.AnomalyType;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ConsumerAnomaly;
import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.entity.User;
import com.app.kyc.repository.AnomalyRepository;
import com.app.kyc.repository.AnomalyTrackingRepository;
import com.app.kyc.repository.AnomalyTypeRepository;
import com.app.kyc.repository.ConsumerAnomalyRepository;
import com.app.kyc.repository.ConsumerRepository;
import com.app.kyc.repository.ServiceProviderRepository;
import com.app.kyc.response.ConsumersDetailsResponseDTO;
import com.app.kyc.response.ConsumersHasSubscriptionsResponseDTO;
import com.app.kyc.response.FlaggedConsumersListDTO;
import com.app.kyc.util.PaginationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.PersistenceContext;

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

        return consumer.map(c -> new ConsumerDto(c, c.getAnomalies())).orElse(null);
    }


    /*public Map<String, Object> getAllConsumers(String params) throws JsonMappingException, JsonProcessingException {
        List<ConsumerDto> pageConsumers = null;
        Long totalInConsistentCustomer;
        List<ConsumersHasSubscriptionsResponseDTO> consumersHasSubscriptionsResponseDTO = null;
        Pagination pagination = PaginationUtil.getFilterObject(params);
        //3 checks, 1 is for whole filter object, 2nd is for filter consistent and 3rd check is for filter consistent value.
        // TODO enchance logic for consumerAnomaly
        if (!Objects.isNull(pagination.getFilter()) && !Objects.isNull(pagination.getFilter().getConsistent()) && !pagination.getFilter().getConsistent()) {
           System.out.println("Into not consistent");
            Page<Consumer> consumerData =  consumerRepository.findByIsConsistentFalse(PaginationUtil.getPageable(params));
            
            pageConsumers = consumerData
            .stream()
            .map(c -> new ConsumerDto(c, c.getAnomalies())).collect(Collectors.toList());
            totalInConsistentCustomer = consumerData.getTotalElements();
            
        } else {
            System.out.println("Into  consistent");
            Page<Consumer> consumerData = consumerRepository.findByIsConsistentTrue(PaginationUtil.getPageable(params));
            
            pageConsumers = consumerData.stream().map(c -> new ConsumerDto(c, c.getAnomalies())).collect(Collectors.toList());
            totalInConsistentCustomer = consumerData.getTotalElements();
        }
        pageConsumers.forEach(consumerDto -> {
            if(Objects.isNull(consumerDto.getLastName()))
            consumerDto.setLastName("");
            if(Objects.isNull(consumerDto.getFirstName()))
            consumerDto.setFirstName("");
            consumerDto.getAnomlies().forEach(anomaly -> {
                if (anomaly.getAnomalyType().getId() == 1) {
                    List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByConsumer_IdAndAnomaly_Id(consumerDto.getId(), anomaly.getId());
                    temp.forEach(t -> {
                        if (Objects.nonNull(t.getNotes())) {
                            consumerDto.setNotes(t.getNotes());
                        }
                    });
                }
            });
        });
        
        consumersHasSubscriptionsResponseDTO = new ArrayList<>();
        for (ConsumerDto c : pageConsumers) {
            int countSubscriptions = consumerServiceService.countConsumersByConsumerId(c.getId());
            consumersHasSubscriptionsResponseDTO.add(new ConsumersHasSubscriptionsResponseDTO(c, countSubscriptions > 0));
        }
        Map<String, Object> consumersWithCount = new HashMap<String, Object>();
        consumersWithCount.put("data", consumersHasSubscriptionsResponseDTO);
        //        consumersWithCount.put("count", new PageImpl<>(pageConsumers).getTotalElements());
        consumersWithCount.put("count", totalInConsistentCustomer);
        return consumersWithCount;
    }*/

    // @Transactional(readOnly = true) // optional, recommended
    /*@Transactional(readOnly = true)
    public Map<String, Object> getAllConsumers(String params) throws JsonMappingException, JsonProcessingException {

        // 1) Resolve pageable + filter (null-safe) and cap page size
        final Pagination pagination = PaginationUtil.getFilterObject(params);
        final Pageable requested    = PaginationUtil.getPageable(params);
        final int MAX_PAGE_SIZE     = 5000;
        final Pageable pageable     = PageRequest.of(
                requested.getPageNumber(),
                Math.min(requested.getPageSize(), MAX_PAGE_SIZE),
                requested.getSort()
        );

        final Boolean consistent = (pagination != null && pagination.getFilter() != null)
                ? pagination.getFilter().getConsistent()
                : null;

        // 2) Page base entities (no hard-coded status)
        final Page<Consumer> consumerPage =
                Boolean.FALSE.equals(consistent) ? consumerRepository.findByIsConsistentFalse(pageable)
                        : Boolean.TRUE.equals(consistent)  ? consumerRepository.findByIsConsistentTrue(pageable)
                        : consumerRepository.findAll(pageable);

        final List<Consumer> consumers = consumerPage.getContent();
        final long total               = consumerPage.getTotalElements();

        if (consumers.isEmpty()) {
            Map<String, Object> resp = new HashMap<>();
            resp.put("data",  Collections.emptyList());
            resp.put("count", total);
            return resp;
        }

        // 3) BULK anomalies for this page (avoid touching c.getAnomalies())
        final List<ConsumerAnomaly> pageAnomalies = consumerAnomalyRepository.findAllByConsumerIn(consumers);

        // Build notes map for anomalyTypeId = 1 (adjust if different)
        final long NOTES_ANOMALY_TYPE_ID = 1L;
        final Map<Long, String> notesByConsumerId = new HashMap<>();
        for (ConsumerAnomaly ca : pageAnomalies) {
            if (ca == null || ca.getAnomaly() == null || ca.getAnomaly().getAnomalyType() == null || ca.getConsumer() == null) continue;
            if (ca.getAnomaly().getAnomalyType().getId() != NOTES_ANOMALY_TYPE_ID) continue;
            if (ca.getNotes() == null) continue;
            notesByConsumerId.putIfAbsent(ca.getConsumer().getId(), ca.getNotes()); // first note wins
        }

        // 4) OPTIONAL: bulk has-subscriptions (replace per-row count if you add the repo)
        // final Set<Long> ids = consumers.stream().map(Consumer::getId).collect(Collectors.toSet());
        // final Set<Long> withSubs = consumerSubscriptionRepository.findConsumerIdsWithAnySubscription(ids);

        // 5) Map to DTOs (don’t touch lazy collections)
        final List<ConsumersHasSubscriptionsResponseDTO> data = new ArrayList<>(consumers.size());
        for (Consumer c : consumers) {
            ConsumerDto dto = new ConsumerDto(c, Collections.emptyList()); // avoid lazy-load of c.getAnomalies()
            if (dto.getFirstName() == null) dto.setFirstName("");
            if (dto.getLastName()  == null) dto.setLastName("");
            String notes = notesByConsumerId.get(c.getId());
            if (notes != null) dto.setNotes(notes);

            boolean hasSubs =
                    *//* withSubs != null ? withSubs.contains(c.getId()) : *//*
                    consumerServiceService.countConsumersByConsumerId(c.getId()) > 0;

            data.add(new ConsumersHasSubscriptionsResponseDTO(dto, hasSubs));
        }

        Map<String, Object> resp = new HashMap<>();
        resp.put("data",  data);
        resp.put("count", total);
        return resp;
    }*/

   /* @Transactional(readOnly = true)
    public Map<String, Object> getAllConsumers(String params)
            throws JsonMappingException, JsonProcessingException {

        // 0) Global counters (independent of paging/filter)
        final long allCount          = consumerRepository.count();
        final long consistentCount   = consumerRepository.countByIsConsistentTrue();
        final long inconsistentCount = consumerRepository.countByIsConsistentFalse();

        // 1) Resolve pageable + filter (null-safe) and cap page size
        final Pagination pagination = PaginationUtil.getFilterObject(params);
        final Pageable requested    = PaginationUtil.getPageable(params);
        final int MAX_PAGE_SIZE     = 5000;
        final Pageable pageable     = PageRequest.of(
                requested.getPageNumber(),
                Math.min(requested.getPageSize(), MAX_PAGE_SIZE),
                requested.getSort()
        );

        final Boolean consistent = (pagination != null && pagination.getFilter() != null)
                ? pagination.getFilter().getConsistent()
                : null;

        // 2) Page base entities (filter only affects the page content, not the global counters)
        final Page<Consumer> consumerPage =
                Boolean.FALSE.equals(consistent) ? consumerRepository.findByIsConsistentFalse(pageable)
                        : Boolean.TRUE.equals(consistent) ? consumerRepository.findByIsConsistentTrue(pageable)
                        : consumerRepository.findAll(pageable);

        final List<Consumer> consumers = consumerPage.getContent();

        // 3) If page empty, still return global counters
        if (consumers.isEmpty()) {
            Map<String, Object> resp = new HashMap<>();
            resp.put("data", Collections.emptyList());
            resp.put("count", allCount);                 // <- all consumers
            resp.put("consistentCount", consistentCount);
            resp.put("inconsistentCount", inconsistentCount);
            return resp;
        }

        // 4) BULK anomalies for this page (avoid touching c.getAnomalies())
        final List<ConsumerAnomaly> pageAnomalies = consumerAnomalyRepository.findAllByConsumerIn(consumers);

        // Build notes map for anomalyTypeId = 1 (adjust if different)
        final long NOTES_ANOMALY_TYPE_ID = 1L;
        final Map<Long, String> notesByConsumerId = new HashMap<>();
        for (Consumer caConsumer : consumers) { *//* pre-size optional *//* }
        for (ConsumerAnomaly ca : pageAnomalies) {
            if (ca == null || ca.getAnomaly() == null || ca.getAnomaly().getAnomalyType() == null || ca.getConsumer() == null) continue;
            if (ca.getAnomaly().getAnomalyType().getId() != NOTES_ANOMALY_TYPE_ID) continue;
            if (ca.getNotes() == null) continue;
            notesByConsumerId.putIfAbsent(ca.getConsumer().getId(), ca.getNotes()); // first note wins
        }

        // 5) Map to DTOs (don’t touch lazy collections)
        final List<ConsumersHasSubscriptionsResponseDTO> data = new ArrayList<>(consumers.size());
        for (Consumer c : consumers) {
            ConsumerDto dto = new ConsumerDto(c, Collections.emptyList()); // avoid lazy-load of c.getAnomalies()
            if (dto.getFirstName() == null) dto.setFirstName("");
            if (dto.getLastName()  == null) dto.setLastName("");
            String notes = notesByConsumerId.get(c.getId());
            if (notes != null) dto.setNotes(notes);

            boolean hasSubs = consumerServiceService.countConsumersByConsumerId(c.getId()) > 0;
            data.add(new ConsumersHasSubscriptionsResponseDTO(dto, hasSubs));
        }

        // 6) Build response with global counters
        Map<String, Object> resp = new HashMap<>();
        resp.put("data", data);
        resp.put("count", allCount);                 // <- all consumers in DB
        resp.put("consistentCount", consistentCount);
        resp.put("inconsistentCount", inconsistentCount);
        return resp;
    }*/

    /*@Transactional(readOnly = true)
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

        // Pull filter fields (type + serviceProviderID), tolerating missing fields
        final Pagination pagination = PaginationUtil.getFilterObject(params);
        final String type = Optional.ofNullable(pagination)
                .map(Pagination::getFilter).map(f -> f.getType())
                .map(String::trim).map(String::toUpperCase)
                .orElse("ALL");
        final Long spId = Optional.ofNullable(pagination)
                .map(Pagination::getFilter).map(f -> f.getServiceProviderID())
                .orElse(null);

        // Provider-scoped or global counters
        final long allCount, consistentCount, inconsistentCount;
        if (spId != null) {
            allCount          = consumerRepository.countByServiceProviderId(spId);
            consistentCount   = consumerRepository.countConsistentByServiceProviderId(spId);
            inconsistentCount = consumerRepository.countInconsistentByServiceProviderId(spId);
        } else {
            allCount          = consumerRepository.count();
            // robust global counts (include NULLs as inconsistent)
            consistentCount   = consumerRepository.countConsistent();
            inconsistentCount = consumerRepository.countInconsistent();
        }

        // Fetch ONE page (scoped to provider if provided)
        final Page<Consumer> page = (spId != null)
                ? consumerRepository.findByServiceProvider_Id(spId, pageable)
                : consumerRepository.findAll(pageable);

        // De-dup (just in case upstream fetch joins leaked dups)
        final List<Consumer> consumers = page.getContent().stream()
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(Consumer::getId, c -> c, (a,b)->a, LinkedHashMap::new),
                        m -> new ArrayList<>(m.values())
                ));

        // Bulk anomalies for notes (type id = 1)
        final List<ConsumerAnomaly> pageAnomalies = consumers.isEmpty()
                ? Collections.emptyList()
                : consumerAnomalyRepository.findAllByConsumerIn(consumers);

        final long NOTES_ANOMALY_TYPE_ID = 1L;
        final Map<Long, String> notesByConsumerId = new HashMap<>();
        for (ConsumerAnomaly ca : pageAnomalies) {
            if (ca == null || ca.getAnomaly() == null || ca.getAnomaly().getAnomalyType() == null || ca.getConsumer() == null) continue;
            if (!Objects.equals(ca.getAnomaly().getAnomalyType().getId(), NOTES_ANOMALY_TYPE_ID)) continue;
            if (ca.getNotes() == null) continue;
            notesByConsumerId.putIfAbsent(ca.getConsumer().getId(), ca.getNotes()); // first note wins
        }

        // Map ONCE
        final List<ConsumersHasSubscriptionsResponseDTO> allData = new ArrayList<>(consumers.size());
        for (Consumer c : consumers) {
            ConsumerDto dto = new ConsumerDto(c, Collections.emptyList());
            dto.setIsConsistent(c.getIsConsistent());            // needed for bucketing
            if (dto.getFirstName() == null) dto.setFirstName("");
            if (dto.getLastName()  == null) dto.setLastName("");
            String notes = notesByConsumerId.get(c.getId());
            if (notes != null) dto.setNotes(notes);

            boolean hasSubs = consumerServiceService.countConsumersByConsumerId(c.getId()) > 0;
            allData.add(new ConsumersHasSubscriptionsResponseDTO(dto, hasSubs));
        }

        // Partition (null -> inconsistent)
        final List<ConsumersHasSubscriptionsResponseDTO> consistentData   = new ArrayList<>();
        final List<ConsumersHasSubscriptionsResponseDTO> inconsistentData = new ArrayList<>();
        for (ConsumersHasSubscriptionsResponseDTO row : allData) {
            Boolean ic = row.getConsumer().getIsConsistent();
            if (Boolean.TRUE.equals(ic)) consistentData.add(row);
            else                          inconsistentData.add(row);
        }

        // Choose "data" bucket by filter.type
        final List<ConsumersHasSubscriptionsResponseDTO> dataBucket;
        switch (type) {
            case "CONSISTENT":   dataBucket = consistentData;   break;
            case "INCONSISTENT": dataBucket = inconsistentData; break;
            default:             dataBucket = allData;          break; // "ALL"
        }

        Map<String, Object> resp = new HashMap<>();
        resp.put("count", allCount);
        resp.put("consistentCount", consistentCount);
        resp.put("inconsistentCount", inconsistentCount);
        resp.put("data", dataBucket);              // shaped by filter.type
        resp.put("consistentData", consistentData);
        resp.put("inconsistentData", inconsistentData);
        return resp;
    }*/




    /*private List<ConsumersHasSubscriptionsResponseDTO> toDtoPage(List<Consumer> consumers) {
        if (consumers == null || consumers.isEmpty()) return Collections.emptyList();

        // Bulk anomalies for this slice (avoid touching c.getAnomalies())
        final List<ConsumerAnomaly> sliceAnomalies = consumerAnomalyRepository.findAllByConsumerIn(consumers);

        // Build notes map for anomalyTypeId = 1 (adjust if needed)
        final long NOTES_ANOMALY_TYPE_ID = 1L;
        final Map<Long, String> notesByConsumerId = new HashMap<>();
        for (ConsumerAnomaly ca : sliceAnomalies) {
            if (ca == null || ca.getAnomaly() == null || ca.getAnomaly().getAnomalyType() == null || ca.getConsumer() == null) continue;
            if (!Objects.equals(ca.getAnomaly().getAnomalyType().getId(), NOTES_ANOMALY_TYPE_ID)) continue;
            if (ca.getNotes() == null) continue;
            notesByConsumerId.putIfAbsent(ca.getConsumer().getId(), ca.getNotes()); // first note wins
        }

        // Map to DTOs (don’t touch lazy collections)
        final List<ConsumersHasSubscriptionsResponseDTO> data = new ArrayList<>(consumers.size());
        for (Consumer c : consumers) {
            ConsumerDto dto = new ConsumerDto(c, Collections.emptyList());
            if (dto.getFirstName() == null) dto.setFirstName("");
            if (dto.getLastName()  == null) dto.setLastName("");
            String notes = notesByConsumerId.get(c.getId());
            if (notes != null) dto.setNotes(notes);

            boolean hasSubs = consumerServiceService.countConsumersByConsumerId(c.getId()) > 0;
            data.add(new ConsumersHasSubscriptionsResponseDTO(dto, hasSubs));
        }
        return data;
    }*/


   /* @Transactional(readOnly = true)
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

        // Counters (provider-scoped when spId provided)
        final long allCount, consistentCount, inconsistentCount;
        if (spId != null) {
            allCount          = consumerRepository.countByServiceProviderId(spId);
            consistentCount   = consumerRepository.countConsistentByServiceProviderId(spId);
            inconsistentCount = consumerRepository.countInconsistentByServiceProviderId(spId);
        } else {
            allCount          = consumerRepository.count();
            consistentCount   = consumerRepository.countConsistent();
            inconsistentCount = consumerRepository.countInconsistent();
        }

        // ===== Fetch THREE independent pages =====
        // A) ALL
        final Page<Consumer> allPage = (spId != null)
                ? consumerRepository.findByServiceProvider_Id(spId, pageable)
                : consumerRepository.findAll(pageable);

        // B) CONSISTENT
        final Page<Consumer> consistentPage = (spId != null)
                ? consumerRepository.findByIsConsistentTrueAndConsumerStatusAndServiceProvider_Id(pageable, 0, spId)
                : consumerRepository.findByIsConsistentTrue(pageable);

        // C) INCONSISTENT
        final Page<Consumer> inconsistentPage = (spId != null)
                ? consumerRepository.findByIsConsistentFalseAndConsumerStatusAndServiceProvider_Id(pageable, 0, spId)
                : consumerRepository.findByIsConsistentFalse(pageable);

        // Map each slice independently (with de-dup just in case)
        final List<ConsumersHasSubscriptionsResponseDTO> allData          = toDtoPage(dedup(allPage.getContent()));
        final List<ConsumersHasSubscriptionsResponseDTO> consistentData   = toDtoPage(dedup(consistentPage.getContent()));
        final List<ConsumersHasSubscriptionsResponseDTO> inconsistentData = toDtoPage(dedup(inconsistentPage.getContent()));

        // "data" shaped by filter.type
        final List<ConsumersHasSubscriptionsResponseDTO> dataBucket =
                "CONSISTENT".equals(type)   ? consistentData :
                        "INCONSISTENT".equals(type) ? inconsistentData :
                                allData;

        Map<String, Object> resp = new HashMap<>();
        resp.put("count", allCount);
        resp.put("consistentCount", consistentCount);
        resp.put("inconsistentCount", inconsistentCount);
        resp.put("data", dataBucket);              // page of ALL / CONSISTENT / INCONSISTENT based on type
        resp.put("consistentData", consistentData);      // always a consistent page
        resp.put("inconsistentData", inconsistentData);  // always an inconsistent page
        return resp;
    }
*/


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
        final Page<Consumer> allPage = (spId != null)
                ? consumerRepository.findByServiceProvider_Id(spId, pageable)
                : consumerRepository.findAll(pageable);

        // B) CONSISTENT
        final Page<Consumer> consistentPage = (spId != null)
                ? consumerRepository.findByIsConsistentTrueAndConsumerStatusInAndServiceProvider_Id(pageable, allowedStatuses, spId)
                : consumerRepository.findByIsConsistentTrueAndConsumerStatusIn(pageable, allowedStatuses);

        // C) INCONSISTENT
        final Page<Consumer> inconsistentPage = (spId != null)
                ? consumerRepository.findByIsConsistentFalseAndConsumerStatusInAndServiceProvider_Id(pageable, allowedStatuses, spId)
                : consumerRepository.findByIsConsistentFalseAndConsumerStatusIn(pageable, allowedStatuses);

        // Map each slice independently (with de-dup just in case)
        final List<ConsumersHasSubscriptionsResponseDTO> allData          = toDtoPage(dedup(allPage.getContent()));
        final List<ConsumersHasSubscriptionsResponseDTO> consistentData   = toDtoPage(dedup(consistentPage.getContent()));
        final List<ConsumersHasSubscriptionsResponseDTO> inconsistentData = toDtoPage(dedup(inconsistentPage.getContent()));

        // "data" shaped by filter.type
        final List<ConsumersHasSubscriptionsResponseDTO> dataBucket =
                "CONSISTENT".equals(type)   ? consistentData :
                        "INCONSISTENT".equals(type) ? inconsistentData :
                                allData;

        Map<String, Object> resp = new HashMap<>();
        resp.put("count", allCount);
        resp.put("consistentCount", consistentCount);
        resp.put("inconsistentCount", inconsistentCount);
        resp.put("data", dataBucket);               // page of ALL / CONSISTENT / INCONSISTENT based on type
        resp.put("consistentData", consistentData); // always a consistent page
        resp.put("inconsistentData", inconsistentData); // always an inconsistent page
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
        return consumerRepository.countConsumersByServiceProvider_IdInAndRegistrationDateBetweenAndIsConsistentAndConsumerStatus(serviceProvidersIds,  createdOnStart, createdOnEnd, isConsistent, consumerStatus);
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
    public long getTotalConsumers (){
        return consumerRepository.getTotalConsumers();
    }

    @Override
    public List<Object[]> getConsumersPerOperator (){
        return consumerRepository.getConsumersPerOperator();
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
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);
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
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);
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
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);

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
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);

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
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumerStatus(pageable, consumerStatus, anomalyStatus, pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData.stream()
                        .map(a -> new AnomlyDto(a, 0))
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();

            } else {
                anomalyStatus.addAll(this.setStatusList(pagination.getFilter().getAnomalyStatus()));

                Page<Anomaly> anomalyData =
                        anomalyRepository.findAllByConsumersAll(pageable, anomalyStatus, pagination.getFilter().getAnomalyType());

                pageAnomaly = anomalyData.stream()
                        .map(AnomlyDto::new)
                        .collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();
            }
        } else {
            final Long spId = pagination.getFilter().getServiceProviderID();

            if (isResolved) {
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);

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

        // --------- HYDRATE CONSUMERS VIA ConsumerAnomaly ---------

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
                        Collectors.mapping(ca -> {
                            ConsumerDto cd = new ConsumerDto(ca.getConsumer(), Collections.emptyList());
                            if (cd.getFirstName() == null) cd.setFirstName("");
                            if (cd.getLastName()  == null) cd.setLastName("");
                            if (ca.getNotes() != null) cd.setNotes(ca.getNotes());
                            return cd;
                        }, Collectors.toList())
                ));

        // NEW: counts per anomalyId -> effectedRecords
        Map<Long, Long> effectedCountByAnomalyId = links.stream()
                .collect(Collectors.groupingBy(
                        ca -> ca.getAnomaly().getId(),
                        Collectors.counting()
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

    /*public void checkConsumer(List<Consumer> consumers, User user, ServiceProvider serviceProvider) {
       System.out.println("Inside check consumer");
        Set<Consumer> consumerSet = new HashSet<>();
        
        AnomalyCollection anomalyCollection=new AnomalyCollection();
        
        HashMap<ExceedingConsumers, Integer> consumerMap = new HashMap<>();
        int count = consumerRepository.countByServiceProvider_Id(serviceProvider.getId());
        if (consumers != null) {
            // this loop for duplicate consumers
            consumers.forEach((consumer) -> {
                consumer.setIsConsistent(true);
                List<String> errors = checkNullAttributesForFile(consumer); // check the missing fields for Incomplete anomaly
                
                // incomplete anomaly
                if (errors.size() > 0) {
                    // tag incomplete data anomaly
                    this.checkConsumerIncompleteAnomaly(consumer, errors, user, count != 0,anomalyCollection); // tag incomplete data anomaly
                } else {
                    if(count != 0){
                        this.resolveIncompleteAnomaly(consumer,user);
                        this.softDeleteConsistentUsers(consumer);
                    }
                    consumerRepository.save(consumer);
                }

                // duplicate anomaly
                if (!consumerSet.add(consumer)) {
                    this.tagDuplicateAnomalies(consumer, user);
                } else {
                    //resolved all old anomalies
                    consumer = this.resolvedAndSoftDeleteConsumers(consumer, count != 0,user);
                    consumerRepository.save(consumer);
                }
                
                //Exceeding Anomaly
                Boolean flag = true;
                ExceedingConsumers exceedingConsumers = new ExceedingConsumers();
                if(!Objects.isNull(consumer.getIdentificationType())){
                    exceedingConsumers.setIdentificationType(consumer.getIdentificationType());
                }else{
                    exceedingConsumers.setIdentificationType("");
                }
                if(!Objects.isNull(consumer.getIdentificationNumber())){
                    exceedingConsumers.setIdentificationNumber(consumer.getIdentificationNumber());
                }else{
                    exceedingConsumers.setIdentificationNumber("");
                }
                
                exceedingConsumers.setServiceProviderName(consumer.getServiceProvider().getName());
                
                consumerMap.put(exceedingConsumers, consumerMap.containsKey(exceedingConsumers) ? consumerMap.get(exceedingConsumers) + 1 : 1);
                
                //consumer.setConsistent(true);
                if (consumerMap.containsKey(exceedingConsumers)) { // check the consumer existence in hashMap
                    if (consumerMap.get(exceedingConsumers) < 3) { // check the consumer count
                        if (consumerMap.get(exceedingConsumers) == 2) {
                            flag = false;
                        }
                        consumer = this.resolvedAndDeleteExceedingConsumers(consumer, count != 0,user);
                        consumerRepository.save(consumer);
                    } else {
                        this.tagExceedingAnomalies(consumer, user);
                    }
                }
            });
        }
        anomalyCollection.getParentAnomalyNoteSet().clear();
    }*/






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

            anomalyCollection.getParentAnomalyNoteSet().clear();

            long totalMs = (System.nanoTime() - t0) / 1_000_000;
            log.info("checkConsumer done | operator={} processed={} in {} ms", serviceProvider.getName(), processed, totalMs);
        }

        // ======== helpers (same assumptions as your code) ========

        private String safeMsisdn(Consumer c) {
            return c == null ? "null" : String.valueOf(c.getMsisdn());
        }
        private String normalize(String s) { return s == null ? "" : s.trim(); }

        // Your existing methods referenced here must remain:
        // - List<String> checkNullAttributesForFile(Consumer c)
        // - void checkConsumerIncompleteAnomaly(Consumer c, List<String> errors, User user, boolean hasExisting, AnomalyCollection ac)
        // - void resolveIncompleteAnomaly(Consumer c, User user)
        // - void softDeleteConsistentUsers(Consumer c)
        // - Consumer resolvedAndSoftDeleteConsumers(Consumer c, boolean hasExisting, User user)
        // - void tagDuplicateAnomalies(Consumer c, User user)
        // - Consumer resolvedAndDeleteExceedingConsumers(Consumer c, boolean hasExisting, User user)
        // - void tagExceedingAnomalies(Consumer c, User user)





    private void checkConsumerIncompleteAnomaly(Consumer consumer, List<String> errors, User user, Boolean flag,AnomalyCollection collection) {

        Set<String> setForDefaultErrors = new HashSet<>();
        Set<String> setForFileErrors = new HashSet<>();
        for (String s : errors)
        setForDefaultErrors.add(s);
        for (String s : checkNullAttributesForFile(consumer))
        setForFileErrors.add(s);

        Set<String> combinedErrors = Stream.concat(setForDefaultErrors.stream(), setForFileErrors.stream())
        .collect(Collectors.toSet());
        collection.setParentAnomalyNoteSet(Stream.concat(combinedErrors.stream(), collection.getParentAnomalyNoteSet().stream())
        .collect(Collectors.toSet()));

        String distinctErrors = String.join(", ", combinedErrors);
        String err_str = String.join(", ", errors);
        String fileErrors = String.join(", ", checkNullAttributesForFile(consumer));

        Anomaly tempAnomaly = new Anomaly();

        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Incomplete Data");

        List<Consumer> tempConsumer = consumerRepository.findConsumerIdsByMsisdnAndConsumerStatusAndIdNumberAndIdTypeAndServiceProviderID(consumer.getMsisdn(), 0, consumer.getIdentificationType(), consumer.getIdentificationNumber(), consumer.getServiceProvider().getId());
        List<Long> consumerIds = tempConsumer.stream().map(Consumer::getId).collect(Collectors.toList());
        List<Long> consumerAnomalies = consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(consumerIds, anomalyType.getId());

        consumer.setIsConsistent(false);
        consumer = consumerRepository.save(consumer);

        tempAnomaly.setNote("Missing Fields: " + distinctErrors);
        ConsumerAnomaly tempCA = new ConsumerAnomaly();

        if (!consumerAnomalies.isEmpty()) {
            Anomaly anomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());
            if (!Objects.isNull(anomaly)) {
                if (anomaly.getStatus().getCode() == 4) {
                    anomaly.setStatus(AnomalyStatus.RESOLVED_SUCCESSFULLY);
                    anomalyRepository.save(anomaly);

                    AnomalyTracking anomalyTracking = new AnomalyTracking(anomaly, new Date(), AnomalyStatus.RESOLVED_SUCCESSFULLY, "", user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
                    anomalyTrackingRepository.save(anomalyTracking);

                    tempAnomaly.setStatus(AnomalyStatus.REPORTED);
                    tempAnomaly.getConsumers().remove(consumer);
                    tempAnomaly.addConsumer(consumer);
                    tempAnomaly.setReportedOn(new Date());
                    tempAnomaly.setReportedBy(user);
                    tempAnomaly.setAnomalyType(anomalyType);
                    tempAnomaly.setUpdatedOn(new Date());
                    tempAnomaly.setUpdateBy(anomaly.getReportedBy().getFirstName() + " " + anomaly.getReportedBy().getLastName());

                    anomalyRepository.save(tempAnomaly);
                    anomalyTracking = new AnomalyTracking(anomaly, new Date(), AnomalyStatus.REPORTED, "", user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
                    anomalyTrackingRepository.save(anomalyTracking);

                }
                if (anomaly.getStatus().getCode() == 0 || anomaly.getStatus().getCode() == 1 ||
                anomaly.getStatus().getCode() == 2 || anomaly.getStatus().getCode() == 3) {

                    tempAnomaly.setId(anomaly.getId());
                    tempAnomaly.setStatus(anomaly.getStatus());
                    tempAnomaly.getConsumers().remove(consumer);
                    tempAnomaly.addConsumer(consumer);
                    tempAnomaly.setReportedOn(anomaly.getReportedOn());
                    tempAnomaly.setReportedBy(user);
                    tempAnomaly.setAnomalyType(anomalyType);
                    tempAnomaly.setUpdatedOn(anomaly.getUpdatedOn());
                    tempAnomaly.setUpdateBy(anomaly.getReportedBy().getFirstName() + " " + anomaly.getReportedBy().getLastName());

                    tempCA.setAnomaly(tempAnomaly);
                    tempCA.setConsumer(consumer);
                    if (!anomaly.getNote().equals(collection.getParentAnomalyNoteSet().toString())) {

                        anomaly.setNote("Missing Fields are: "+collection.getParentAnomalyNoteSet().toString());
                        anomalyRepository.save(anomaly);
                    }
                    tempCA.setNotes("Missing Fields are: " + distinctErrors);
                    consumerAnomalyRepository.save(tempCA);
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
            AnomalyTracking anomalyTracking = new AnomalyTracking(tempAnomaly, new Date(), AnomalyStatus.REPORTED, "", user.getFirstName()+" "+user.getLastName(), tempAnomaly.getUpdatedOn());
            anomalyTrackingRepository.save(anomalyTracking);

            ConsumerAnomaly consumerAnomaly = new ConsumerAnomaly();
            consumerAnomaly.setNotes("Missing Fields are: " + distinctErrors);
            consumerAnomaly.setAnomaly(savedAnomaly);
            consumerAnomaly.setConsumer(consumer);

            consumerAnomalyRepository.save(consumerAnomaly);
        }

        // soft deleted old consumers
        if (flag) {
            if ((consumerAnomalies.size() == 0 || consumerAnomalies.size() == 1) && consumerIds.size() == 1) {
                for (int i = 0; i < consumerIds.size(); i++) {
                    consumerRepository.updatePreviousConsumersStatus(1, consumerIds.get(i));
                }
            }
        }

    }

    private void resolveIncompleteAnomaly(Consumer consumer,User user){
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Incomplete Data");

        List<Consumer> tempConsumer = consumerRepository.findConsumerIdsByMsisdnAndConsumerStatusAndIdNumberAndIdTypeAndServiceProviderID(
                consumer.getMsisdn(), 0, consumer.getIdentificationType(),
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
                    anomaly.setStatus(AnomalyStatus.RESOLVED_SUCCESSFULLY);
                    anomalyRepository.save(anomaly);
                    AnomalyTracking anomalyTracking = new AnomalyTracking(anomaly, new Date(), AnomalyStatus.RESOLVED_SUCCESSFULLY, "", user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
                    anomalyTrackingRepository.save(anomalyTracking);
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
                }
            }
        }
        if ((/*consumerAnomalies.size() == 0 || */consumerAnomalies.size() == 1) && consumerIds.size() == 1) {
            for (int i = 0; i < consumerIds.size(); i++) {
                consumerRepository.updatePreviousConsumersStatus(1, consumerIds.get(i));
            }
        }
    }

    private Consumer resolvedAndSoftDeleteConsumers(Consumer consumer, Boolean flag,User user) {
        // duplicate records
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Duplicate Records");

        //previously inserted consumer
        List<Long> consumerIds = consumerRepository.findConsumerIdsByMsisdnAndConsumerStatus(consumer.getMsisdn(), 0);

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
                    anomaly.setStatus(AnomalyStatus.RESOLVED_SUCCESSFULLY);
                    anomalyRepository.save(anomaly);
                    AnomalyTracking anomalyTracking = new AnomalyTracking(anomaly, new Date(), AnomalyStatus.RESOLVED_SUCCESSFULLY, "", user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
                    anomalyTrackingRepository.save(anomalyTracking);
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
                }
            }
        }

        consumerIds.remove(consumer.getId());

        //soft deleted old consumers
        if (flag && !(consumerIds.size() > 2)) {
            if (consumerAnomalies.size() > 1 && consumerIds.size() > 1) {
                for (int i = 0; i < consumerIds.size(); i++) {
                    if (!Objects.equals(consumerIds.get(i), consumer.getId())) {
                        consumerRepository.updatePreviousConsumersStatus(1, consumerIds.get(i));
                    }
                }
            }
        }
        return consumer;
    }

    /*private void tagDuplicateAnomalies(Consumer consumer, User user) {
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Duplicate Records");

        Anomaly tempAnomaly = new Anomaly();
        Anomaly anomaly = new Anomaly();
        String note = "You can't have more than one active record per MSISDN: " + consumer.getMsisdn();
        tempAnomaly.setNote("Duplicate Anomaly: " + note);

        // get previous consumers
        List<Consumer> duplicateConsumers = consumerRepository.findByMsisdnAndConsumerStatus(consumer.getMsisdn(), 0);
        List<Long> duplicateConsumerIds = duplicateConsumers.stream().map(Consumer::getId).collect(Collectors.toList());

        consumer.setIsConsistent(false);
        consumer = consumerRepository.save(consumer);

        // check anomaly of previous consumers
        List<Long> consumerAnomalies = consumerAnomalyRepository
                .findAnomaliesIdByConsumerAndAnomalyTypeId(duplicateConsumerIds, anomalyType.getId());

        if (consumerAnomalies.isEmpty()) {
            // make new anomaly
            tempAnomaly.setStatus(AnomalyStatus.REPORTED);
            tempAnomaly.setReportedOn(new Date());
            tempAnomaly.setReportedBy(user);
            tempAnomaly.getConsumers().remove(consumer);
            tempAnomaly.addConsumer(consumer);
            tempAnomaly.setUpdatedOn(new Date());
            tempAnomaly.setAnomalyType(anomalyType);
            tempAnomaly = anomalyRepository.save(tempAnomaly);

            AnomalyTracking anomalyTracking = new AnomalyTracking(
                    tempAnomaly, new Date(), AnomalyStatus.REPORTED, "",
                    user.getFirstName() + " " + user.getLastName(), tempAnomaly.getUpdatedOn()
            );
            anomalyTrackingRepository.save(anomalyTracking);

            // ✅ FIX: check before using get(0)
            List<ConsumerAnomaly> links =
                    consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(tempAnomaly.getId(), consumer.getId());

            if (links == null || links.isEmpty()) {
                ConsumerAnomaly link = new ConsumerAnomaly();
                link.setAnomaly(tempAnomaly);
                link.setConsumer(consumer);
                link.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            } else {
                ConsumerAnomaly link = links.get(0);
                link.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            }
        } else {
            //load anomaly and tag to new consumer
            tempAnomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());
            if (!Objects.isNull(tempAnomaly)) {
                ConsumerAnomaly consumerAnomaly = new ConsumerAnomaly();
                anomaly.setId(tempAnomaly.getId());
                anomaly.setNote(tempAnomaly.getNote());
                anomaly.setStatus(tempAnomaly.getStatus());
                anomaly.setReportedOn(tempAnomaly.getReportedOn());
                anomaly.setReportedBy(tempAnomaly.getReportedBy());
                anomaly.addConsumer(consumer);
                anomaly.setAnomalyType(tempAnomaly.getAnomalyType());
                anomaly.setUpdatedOn(tempAnomaly.getUpdatedOn());

                consumerAnomaly.setAnomaly(anomaly);
                consumerAnomaly.setConsumer(consumer);
                consumerAnomaly.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(consumerAnomaly);
            }
        }

        // tag anomaly to all duplicate consumers
        for (Consumer temp : duplicateConsumers) {
            List<ConsumerAnomaly> consumerAnomaly =
                    consumerAnomalyRepository.findByAnomaly_AnomalyTypeAndConsumer(anomalyType, temp);
            if (consumerAnomaly == null || consumerAnomaly.isEmpty()) {
                ConsumerAnomaly tempConsumerAnomaly = new ConsumerAnomaly();
                tempConsumerAnomaly.setAnomaly(tempAnomaly);
                tempConsumerAnomaly.setConsumer(temp);
                tempConsumerAnomaly.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(tempConsumerAnomaly);
            }
        }

        consumerRepository.markConsumersConsistent(0, duplicateConsumerIds);
    }*/



    /*private void tagDuplicateAnomalies(Consumer consumer, User user) {
        // ----- Guard 1: skip if this consumer MSISDN is blank/null -----
        if (consumer == null || isBlank(consumer.getMsisdn())) {
            return; // No duplicate anomaly for blank MSISDN
        }

        final String msisdn = consumer.getMsisdn().trim();

        // Pull ACTIVE duplicates with the same MSISDN (exclude blanks at DB, and status=0)
        // Prefer this repo method; it already exists in your repo:
        List<Consumer> duplicateConsumers = consumerRepository.findByMsisdnAndConsumerStatus(msisdn, 0);

        // Ensure the current consumer is saved and included in the evaluation
        consumer.setIsConsistent(false);
        consumer = consumerRepository.save(consumer);

        // If current consumer isn’t in the fetched list yet, add it (when just created/updated)
        Consumer finalConsumer = consumer;
        boolean currentIncluded = duplicateConsumers.stream().anyMatch(c -> Objects.equals(c.getId(), finalConsumer.getId()));
        if (!currentIncluded) {
            duplicateConsumers.add(consumer);
        }

        // ----- Only proceed if we truly have duplicates (>= 2 ACTIVE consumers with same MSISDN) -----
        if (duplicateConsumers.size() < 2) {
            return; // do NOT report anomaly for a single active record
        }

        // (Optional extra safety; usually zero since msisdn filter used)
        long blankMsisdnCount = duplicateConsumers.stream().map(Consumer::getMsisdn).filter(this::isBlank).count();
        if (blankMsisdnCount >= 2) {
            return; // don’t create duplicate anomaly for multiple blanks
        }

        // Build IDs for downstream calls
        List<Long> duplicateConsumerIds = duplicateConsumers.stream().map(Consumer::getId).collect(Collectors.toList());

        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Duplicate Records");
        final String note = "You can't have more than one active record per MSISDN: " + msisdn;

        // Check existing anomaly of this type for any of these consumers
        List<Long> consumerAnomalies = consumerAnomalyRepository
                .findAnomaliesIdByConsumerAndAnomalyTypeId(duplicateConsumerIds, anomalyType.getId());

        Anomaly targetAnomaly;
        Date now = new Date();

        if (consumerAnomalies == null || consumerAnomalies.isEmpty()) {
            // Create a new anomaly
            targetAnomaly = new Anomaly();
            targetAnomaly.setNote("Duplicate Anomaly: " + note);
            targetAnomaly.setStatus(AnomalyStatus.REPORTED);
            targetAnomaly.setReportedOn(now);
            targetAnomaly.setReportedBy(user);
            targetAnomaly.setUpdatedOn(now);
            targetAnomaly.setAnomalyType(anomalyType);
            targetAnomaly = anomalyRepository.save(targetAnomaly);

            // Tracking with hard-coded updatedBy
            AnomalyTracking anomalyTracking = new AnomalyTracking(
                    targetAnomaly, now, AnomalyStatus.REPORTED, "",
                    "System for Anomaly", targetAnomaly.getUpdatedOn()
            );
            anomalyTrackingRepository.save(anomalyTracking);
        } else {
            // Load the existing anomaly for this group
            targetAnomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());
            if (targetAnomaly == null) return; // nothing to do
        }

        // ----- Link ALL involved consumers (including current) to the anomaly (idempotent) -----
        for (Consumer cons : duplicateConsumers) {
            List<ConsumerAnomaly> links =
                    consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(targetAnomaly.getId(), cons.getId());

            if (links == null || links.isEmpty()) {
                ConsumerAnomaly link = new ConsumerAnomaly();
                link.setAnomaly(targetAnomaly);
                link.setConsumer(cons);
                link.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            } else {
                ConsumerAnomaly link = links.get(0);
                // Update note to latest phrasing (optional)
                link.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            }
        }

        // Mark all involved consumers as inconsistent (include current)
        consumerRepository.markConsumersConsistent(0, duplicateConsumerIds);
    }*/

    // helper (already in your class)

    private static boolean hasText(String s) { return s != null && !s.trim().isEmpty(); }
    private String norm(String s) { return s == null ? null : s.trim(); }

    @Transactional
    private void tagDuplicateAnomalies(Consumer consumer, User user) {
        if (consumer == null) return;

        final String msisdn = norm(consumer.getMsisdn());
        if (!hasText(msisdn)) return; // (you already handle blank/null case separately)

        // 1) Fetch ALL rows with this MSISDN (ignore status)
        List<Consumer> sameMsisdnAnyStatus = consumerRepository.findByMsisdn(msisdn);

        // Ensure current consumer is included
        if (sameMsisdnAnyStatus.stream().noneMatch(c -> Objects.equals(c.getId(), consumer.getId()))) {
            sameMsisdnAnyStatus.add(consumer);
        }

        // 2) Decide your rule:
        //    a) pure duplicate regardless of status:
        if (sameMsisdnAnyStatus.size() < 2) return;

        //    b) or: duplicate if at least one active AND total >= 2:
        // boolean anyActive = sameMsisdnAnyStatus.stream().anyMatch(c -> c.getConsumerStatus() != null && c.getConsumerStatus() == 0);
        // if (!anyActive || sameMsisdnAnyStatus.size() < 2) return;

        // 3) Proceed to create/reuse anomaly and link (your existing code)...
        sameMsisdnAnyStatus.forEach(c -> c.setIsConsistent(false));
        consumerRepository.save(consumer);

        List<Long> ids = sameMsisdnAnyStatus.stream().map(Consumer::getId).collect(Collectors.toList());

        AnomalyType type = anomalyTypeRepository.findFirstByName("Duplicate Records");
        String note = "You can't have more than one active record per MSISDN: " + msisdn;

        List<Long> existingAnomalyIds =
                consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(ids, type.getId());

        Date now = new Date();
        Anomaly anomaly;
        if (existingAnomalyIds == null || existingAnomalyIds.isEmpty()) {
            anomaly = new Anomaly();
            anomaly.setNote("Duplicate Anomaly: " + note);
            anomaly.setStatus(AnomalyStatus.REPORTED);
            anomaly.setReportedOn(now);
            anomaly.setReportedBy(user);
            anomaly.setUpdatedOn(now);
            anomaly.setAnomalyType(type);
            anomaly = anomalyRepository.save(anomaly);

            anomalyTrackingRepository.save(
                    new AnomalyTracking(anomaly, now, AnomalyStatus.REPORTED, "",
                            "System for Anomaly", anomaly.getUpdatedOn())
            );
        } else {
            anomaly = anomalyRepository.findByIdAndAnomalyType_Id(existingAnomalyIds, type.getId());
            if (anomaly == null) return;
        }

        for (Consumer c : sameMsisdnAnyStatus) {

            List<ConsumerAnomaly> links =
                    consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(anomaly.getId(), c.getId());
            if (links == null || links.isEmpty()) {
                ConsumerAnomaly link = new ConsumerAnomaly();
                link.setAnomaly(anomaly);
                link.setConsumer(c);
                link.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            } else {
                ConsumerAnomaly link = links.get(0);
                link.setNotes("Duplicate Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            }
        }

        long activeCnt = consumerRepository.findByMsisdn(msisdn)
                .stream().filter(c -> Integer.valueOf(0).equals(c.getConsumerStatus())).count();
        long totalCnt = consumerRepository.findByMsisdn(msisdn).size();
        log.info("MSISDN={} activeCnt={} totalCnt={}", msisdn, activeCnt, totalCnt);

        consumerRepository.markConsumersConsistent(0, ids);
    }



    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }


    private Consumer resolvedAndDeleteExceedingConsumers(Consumer consumer, Boolean flag,User user) {
        // Exceeding records
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Exceeding Threshold");

        //previously inserted consumer
        List<Consumer> duplicateConsumers = consumerRepository.findByIdentificationTypeAndIdentificationNumberAndServiceProviderAndConsumerStatus(consumer.getIdentificationType(), consumer.getIdentificationNumber(), consumer.getServiceProvider(), 0);
        List<Long> consumerIds = duplicateConsumers.stream().map(Consumer::getId).collect(Collectors.toList());

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
                    anomaly.setStatus(AnomalyStatus.RESOLVED_SUCCESSFULLY);
                    anomalyRepository.save(anomaly);
                    AnomalyTracking anomalyTracking = new AnomalyTracking(anomaly, new Date(), AnomalyStatus.RESOLVED_SUCCESSFULLY, "", user.getFirstName()+" "+user.getLastName(), anomaly.getUpdatedOn());
                    anomalyTrackingRepository.save(anomalyTracking);
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
                    tempAnomaly.setAnomalyType(anomalyType);
                    tempAnomaly.setUpdatedOn(anomaly.getUpdatedOn());
                    tempConsumerAnomaly.setAnomaly(tempAnomaly);
                    consumer.setIsConsistent(false);
                    consumer = consumerRepository.save(consumer);

                    tempConsumerAnomaly.setConsumer(consumer);
                    tempConsumerAnomaly.setNotes(anomaly.getNote());

                    consumerAnomalyRepository.save(tempConsumerAnomaly);
                }
            }
        }
        // soft deleted old consumers
        if (flag) {
            if (consumerIds.size() > 2) {
                for (int i = 0; i < consumerIds.size(); i++) {
                    if (!Objects.equals(consumerIds.get(i), consumer.getId())) {
                        consumerRepository.updatePreviousConsumersStatus(1, consumerIds.get(i));
                    }
                }
            }
        }
        return consumer;
    }




  /*  private void tagExceedingAnomalies(Consumer consumer, User user) {
        // --------- Hard guards: incomplete key -> no anomaly ----------
        if (consumer == null) return;
        if (isBlank(consumer.getIdentificationType())) return;   // ID Type required
        if (isBlank(consumer.getIdentificationNumber())) return; // ID Number required
        if (consumer.getServiceProvider() == null) return;       // SP required

        final String idType   = consumer.getIdentificationType().trim();
        final String idNumber = consumer.getIdentificationNumber().trim();
        final String spName   = consumer.getServiceProvider().getName();

        final String note = "You can't have more than two active records per operator for a given combination of "
                + "(ID Card Type + ID Number + ServiceProviderName): (" + idType + " + " + idNumber + " + " + spName + ")";

        final AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Exceeding Threshold");

        // --------- Pull ACTIVE consumers for same (idType, idNumber, SP) ----------
        // IMPORTANT: use the consumerStatus=0 version
        List<Consumer> sameKeyActive =
                consumerRepository.findByIdentificationTypeAndIdentificationNumberAndServiceProviderAndConsumerStatus(
                        idType, idNumber, consumer.getServiceProvider(), 0);

        // Ensure current consumer is included in evaluation set
        boolean included = sameKeyActive.stream().anyMatch(c -> Objects.equals(c.getId(), consumer.getId()));
        if (!included) {
            // If current is not active you can decide to return; otherwise include it explicitly.
            sameKeyActive.add(consumer);
        }

        // --------- Optional guard: skip if ≥ 2 blank MSISDN across the set ----------
        long blankMsisdnCount = sameKeyActive.stream().map(Consumer::getMsisdn).filter(this::isBlank).count();
        if (blankMsisdnCount >= 2) return;

        // --------- Only exceed when > 2 (i.e., 3 or more) ----------
        if (sameKeyActive.size() <= 2) return;

        // Mark all as inconsistent (including current)
        sameKeyActive.forEach(c -> c.setIsConsistent(false));
        // persist current at least
        consumerRepository.save(consumer);

        List<Long> ids = sameKeyActive.stream().map(Consumer::getId).collect(Collectors.toList());

        // Check if an anomaly of this type already exists for any of these consumers
        List<Long> existingAnomalyIds =
                consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(ids, anomalyType.getId());

        Anomaly targetAnomaly;
        Date now = new Date();

        if (existingAnomalyIds == null || existingAnomalyIds.isEmpty()) {
            // Create new anomaly
            targetAnomaly = new Anomaly();
            targetAnomaly.setNote("Exceeding Anomaly: " + note);
            targetAnomaly.setStatus(AnomalyStatus.REPORTED);
            targetAnomaly.setReportedOn(now);
            targetAnomaly.setReportedBy(user);
            targetAnomaly.setUpdatedOn(now);
            targetAnomaly.setAnomalyType(anomalyType);
            targetAnomaly = anomalyRepository.save(targetAnomaly);

            // Tracking (force updatedBy)
            AnomalyTracking at = new AnomalyTracking(
                    targetAnomaly, now, AnomalyStatus.REPORTED, "",
                    "System for Anomaly", targetAnomaly.getUpdatedOn()
            );
            anomalyTrackingRepository.save(at);
        } else {
            // Load existing anomaly
            targetAnomaly = anomalyRepository.findByIdAndAnomalyType_Id(existingAnomalyIds, anomalyType.getId());
            if (targetAnomaly == null) return;
        }

        // --------- Link ALL involved consumers idempotently ----------
        for (Consumer c : sameKeyActive) {
            List<ConsumerAnomaly> links =
                    consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(targetAnomaly.getId(), c.getId());
            if (links == null || links.isEmpty()) {
                ConsumerAnomaly link = new ConsumerAnomaly();
                link.setAnomaly(targetAnomaly);
                link.setConsumer(c);
                link.setNotes("Exceeding Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            } else {
                ConsumerAnomaly link = links.get(0);
                link.setNotes("Exceeding Anomaly: " + note); // keep message fresh
                consumerAnomalyRepository.save(link);
            }
        }

        // Mark DB flag for all involved consumers
        consumerRepository.markConsumersConsistent(0, ids);
    }

*/


   /* @Transactional // NOT readOnly
    private void tagExceedingAnomalies(Consumer consumer, User user) {
        if (consumer == null) return;

        // 1) Hard guards for key parts
        String idType   = norm(consumer.getIdentificationType());
        String idNumber = norm(consumer.getIdentificationNumber());
        if (!hasText(idType) || !hasText(idNumber) || consumer.getServiceProvider() == null) return;

        String spName = consumer.getServiceProvider().getName();
        String note = "You can't have more than two active records per operator for a given combination of "
                + "(ID Card Type + ID Number + ServiceProviderName): (" + idType + " + " + idNumber + " + " + spName + ")";

        AnomalyType type = anomalyTypeRepository.findFirstByName("Exceeding Threshold");

        // 2) ACTIVE set for same (idType, idNumber, SP)
        List<Consumer> sameKeyActive =
                consumerRepository.findByIdentificationTypeAndIdentificationNumberAndServiceProviderAndConsumerStatus(
                        idType, idNumber, consumer.getServiceProvider(), 0);

        // Include current
        boolean included = sameKeyActive.stream().anyMatch(c -> Objects.equals(c.getId(), consumer.getId()));
        if (!included) sameKeyActive.add(consumer);

        // 3) Optional: skip if ≥2 blank MSISDNs (your earlier requirement)
        long blankCnt = sameKeyActive.stream().map(Consumer::getMsisdn).map(this::norm).filter(s -> !hasText(s)).count();
        if (blankCnt >= 2) return;

        // 4) Threshold: more than two -> i.e., size >= 3
        if (sameKeyActive.size() < 3) return;

        // 5) Mark inconsistent & persist current
        sameKeyActive.forEach(c -> c.setIsConsistent(false));
        consumerRepository.save(consumer);
        List<Long> ids = sameKeyActive.stream().map(Consumer::getId).collect(Collectors.toList());

        // 6) Find/create anomaly
        List<Long> existingAnomalyIds =
                consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(ids, type.getId());

        Anomaly anomaly;
        Date now = new Date();

        if (existingAnomalyIds == null || existingAnomalyIds.isEmpty()) {
            anomaly = new Anomaly();
            anomaly.setNote("Exceeding Anomaly: " + note);
            anomaly.setStatus(AnomalyStatus.REPORTED);
            anomaly.setReportedOn(now);
            anomaly.setReportedBy(user);
            anomaly.setUpdatedOn(now);
            anomaly.setAnomalyType(type);
            anomaly = anomalyRepository.save(anomaly);

            anomalyTrackingRepository.save(
                    new AnomalyTracking(anomaly, now, AnomalyStatus.REPORTED, "",
                            "System for Anomaly", anomaly.getUpdatedOn())
            );
        } else {
            anomaly = anomalyRepository.findByIdAndAnomalyType_Id(existingAnomalyIds, type.getId());
            if (anomaly == null) return;
        }

        // 7) Idempotent linking (create if missing)
        for (Consumer c : sameKeyActive) {
            List<ConsumerAnomaly> links =
                    consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(anomaly.getId(), c.getId());
            if (links == null || links.isEmpty()) {
                ConsumerAnomaly link = new ConsumerAnomaly();
                link.setAnomaly(anomaly);
                link.setConsumer(c);
                link.setNotes("Exceeding Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            } else {
                ConsumerAnomaly link = links.get(0);
                link.setNotes("Exceeding Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            }
        }

        consumerRepository.markConsumersConsistent(0, ids);
    }
*/


    @Transactional
    private void tagExceedingAnomalies(Consumer consumer, User user) {
        if (consumer == null) return;

        // ---- Guard: don't report when ID fields are blank/null (your rule) ----
        final String idType   = norm(consumer.getIdentificationType());
        final String idNumber = norm(consumer.getIdentificationNumber());
        final ServiceProvider sp = consumer.getServiceProvider();
        if (!hasText(idType) || !hasText(idNumber) || sp == null) return;

        // ---- Fetch duplicates for SAME operator (any status) ----
        // To make it "active-only", use a method that also filters consumer_status=0.
        List<Consumer> candidates =
                consumerRepository.findByIdKeyNormalizedAnyStatus(sp.getId(), idType, idNumber);

        // Ensure current consumer is included
        if (candidates.stream().noneMatch(c -> Objects.equals(c.getId(), consumer.getId()))) {
            candidates.add(consumer);
        }

        // ---- Exceeding threshold: more than two (i.e., >=3) ----
        if (candidates.size() <= 2) return;

        // Mark all involved inconsistent (persist at least current)
        candidates.forEach(c -> c.setIsConsistent(false));
        consumerRepository.save(consumer);
        List<Long> ids = candidates.stream().map(Consumer::getId).collect(Collectors.toList());

        // Build note
        final String spName = sp.getName() == null ? "" : sp.getName();
        final String note = "You can't have more than two active records per operator for a given combination of "
                + "(ID Card Type + ID Number + ServiceProviderName): (" + idType + " + " + idNumber + " + " + spName + ")";

        // Find or create anomaly
        final AnomalyType type = anomalyTypeRepository.findFirstByName("Exceeding Threshold");
        final Date now = new Date();
        List<Long> existing = consumerAnomalyRepository
                .findAnomaliesIdByConsumerAndAnomalyTypeId(ids, type.getId());

        Anomaly anomaly;
        if (existing == null || existing.isEmpty()) {
            anomaly = new Anomaly();
            anomaly.setNote("Exceeding Anomaly: " + note);
            anomaly.setStatus(AnomalyStatus.REPORTED);
            anomaly.setReportedOn(now);
            anomaly.setReportedBy(user);
            anomaly.setUpdatedOn(now);
            anomaly.setAnomalyType(type);
            anomaly = anomalyRepository.save(anomaly);

            anomalyTrackingRepository.save(
                    new AnomalyTracking(anomaly, now, AnomalyStatus.REPORTED, "",
                            "System for Anomaly", anomaly.getUpdatedOn())
            );
        } else {
            anomaly = anomalyRepository.findByIdAndAnomalyType_Id(existing, type.getId());
            if (anomaly == null) return;
        }

        // Link ALL involved consumers (create if missing, otherwise update notes)
        for (Consumer c : candidates) {
            List<ConsumerAnomaly> links =
                    consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(anomaly.getId(), c.getId());
            if (links == null || links.isEmpty()) {
                ConsumerAnomaly link = new ConsumerAnomaly();
                link.setAnomaly(anomaly);
                link.setConsumer(c);
                link.setNotes("Exceeding Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            } else {
                ConsumerAnomaly link = links.get(0);
                link.setNotes("Exceeding Anomaly: " + note);
                consumerAnomalyRepository.save(link);
            }
        }

        // Persist DB flag for all involved
        consumerRepository.markConsumersConsistent(0, ids);
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
        
        List<Consumer> tempConsumer = consumerRepository.findConsumerIdsByMsisdnAndConsumerStatusAndIdNumberAndIdTypeAndServiceProviderID(
            consumer.getMsisdn(), 0, consumer.getIdentificationType(),
            consumer.getIdentificationNumber(), consumer.getServiceProvider().getId());
//        List<Consumer> previousConsumers = consumerRepository.findConsumerIdsByMsisdnAndConsumerStatusAndIdNumberAndIdTypeAndServiceProviderID(
//            consumer.getMsisdn(), 1, consumer.getIdentificationType(),
//            consumer.getIdentificationNumber(), consumer.getServiceProvider().getId());
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
    
    public List<AnomalyStatus> setStatusList(AnomalyStatus status){
    	List<AnomalyStatus>  anomalyStatus = new ArrayList<AnomalyStatus>();
    	if (status != null) {
			switch (status) {
			case REPORTED:
				log.info("in case 1");
				anomalyStatus.add(AnomalyStatus.REPORTED);
				break;

			case QUESTION_SUBMITTED:
				log.info("in case 2");
				anomalyStatus.add(AnomalyStatus.QUESTION_SUBMITTED);
				break;

			case UNDER_INVESTIGATION:
				log.info("in case 3");
				anomalyStatus.add(AnomalyStatus.UNDER_INVESTIGATION);
				break;

			case QUESTION_ANSWERED:
				log.info("in case 4");
				anomalyStatus.add(AnomalyStatus.QUESTION_ANSWERED);
				break;

			case RESOLUTION_SUBMITTED:
				log.info("in case 5");
				anomalyStatus.add(AnomalyStatus.RESOLUTION_SUBMITTED);
				break;

			default:
				// If nothing is matched, you can decide:
				// 1. Add all, or
				// 2. Leave empty, or
				// 3. Log an error
				log.info("in case default");
				anomalyStatus.addAll(Arrays.asList(AnomalyStatus.REPORTED, AnomalyStatus.QUESTION_SUBMITTED,
						AnomalyStatus.UNDER_INVESTIGATION, AnomalyStatus.QUESTION_ANSWERED,
						AnomalyStatus.RESOLUTION_SUBMITTED));
				break;
			}
		} else {
			anomalyStatus.add(AnomalyStatus.REPORTED);
			anomalyStatus.add(AnomalyStatus.QUESTION_SUBMITTED);
			anomalyStatus.add(AnomalyStatus.UNDER_INVESTIGATION);
			anomalyStatus.add(AnomalyStatus.QUESTION_ANSWERED);
			anomalyStatus.add(AnomalyStatus.RESOLUTION_SUBMITTED);
		}
    	return anomalyStatus;
    }
}
