package com.app.kyc.service;

import com.app.kyc.entity.AnomalyType;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.model.DashboardAnomalyStatusInterface;
import com.app.kyc.model.DashboardObject;
import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.model.ResolutionMetricDTO;
import com.app.kyc.request.DashboardRequestDTO;
import com.app.kyc.response.DashboardResponseDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class DashboardServiceImpl implements DashboardService {
    @Autowired
    IndustryService industryService;

    @Autowired
    ServiceProviderService serviceProviderService;

    @Autowired
    ServiceTypeService serviceTypeService;

    @Autowired
    ConsumerService consumerService;

    @Autowired
    ConsumerServiceService consumerServiceService;

    @Autowired
    AnomalyService anomalyService;

    @Autowired
    AnomalyTypeService anomalyTypeService;

    @Autowired
    ConsumerAnomalyService consumerAnomalyService;

//   static final long DEFAULT_INDUSTRY_ID = 19L;

    static final long DEFAULT_DAYS_INTERVAL = 30L;

    static final long ALL = 0L;
    static final List<AnomalyStatus> DEFAULT_ANOMALY_STATUS_LIST
            = new ArrayList<>(Arrays.asList(
            AnomalyStatus.REPORTED
    ));

    static final List<AnomalyStatus> DEFAULT_ANOMALY_PENDING_LIST
            = new ArrayList<>(Arrays.asList(
            AnomalyStatus.UNDER_INVESTIGATION,
            AnomalyStatus.QUESTION_SUBMITTED,
            AnomalyStatus.QUESTION_ANSWERED,
            AnomalyStatus.RESOLUTION_SUBMITTED
    ));

    static final List<AnomalyStatus> DEFAULT_ANOMALY_WITHDRAWN_LIST
            = new ArrayList<>(Arrays.asList(
            AnomalyStatus.WITHDRAWN
    ));

    static final List<AnomalyStatus> DEFAULT_ANOMALY_RESOLVED_STATUS
            = new ArrayList<>(List.of(
            AnomalyStatus.RESOLVED_SUCCESSFULLY
    ));


    //TODO:: Handle Service Types in filters

    //TODO:: Make methods Async
    @Override
    public DashboardResponseDTO getDashboardV2(DashboardRequestDTO dashboardRequestDTO) throws NullPointerException, JsonProcessingException {

        //Initialization of Lists
        List<DashboardObjectInterface>
                listIndustry;
        List<DashboardObjectInterface> listServiceProvider = new ArrayList<>(Arrays.asList(new DashboardObject("All", 0)));
        List<DashboardObjectInterface> listServiceType = new ArrayList<>(Arrays.asList(new DashboardObject("All", 0)));
        List<DashboardObjectInterface> anomaliesByStatus;
        List<DashboardObjectInterface> serviceProviderColors = new ArrayList<>();
        List<DashboardObjectInterface> consumers = new ArrayList<>();
        List<DashboardObjectInterface> anomalies = new ArrayList<>();
        List<DashboardObjectInterface> anomalyTypes = new ArrayList<>();


        List<DashboardObjectInterface>
                subscriptions;

        //Initialization of int
        int
                numServiceProviders,
                numConsistentConsumers,
                numNonConsistentConsumers,
                numSubscriptions,
                numAnomaliesReported,
                numAnomaliesInProgress,
                numAnomaliesWithdrawn,
                numAnomaliesResolved;




        //Initialization of DashboardObjects
        DashboardObjectInterface
                reportedDashboardObject = new DashboardObject("Reported", new ArrayList<>()),
                resolvedDashboardObject = new DashboardObject("Resolved", new ArrayList<>()),
                pendingDashboardObject = new DashboardObject("Pending", new ArrayList<>()),
                withdrawnDashboardObject = new DashboardObject("Withdrawn", new ArrayList<>());

        //Get start and end dates from request DTO
        Date
                startDate = dashboardRequestDTO.getStartDate(),
                endDate = dashboardRequestDTO.getEndDate();


        //merge all anomalies grouped by thier respective statuses
        anomaliesByStatus = Arrays.asList(
                reportedDashboardObject,
                resolvedDashboardObject,
                pendingDashboardObject,
                withdrawnDashboardObject
        );

        //get all industries and map them to dashboard object
        listIndustry = industryService.getAllIndustries();

        //get selected industry based on default response
        long selectedIndustry =
                dashboardRequestDTO.isDefaultResponse()
                        ?
                        listIndustry.get(0).getValue()
                        :
                        (long) dashboardRequestDTO.getIndustry().getValue();

        //get service provider ids based on selected industry
        List<Long> serviceProviderIds =
                dashboardRequestDTO.isDefaultResponse() || dashboardRequestDTO.getServiceProvider().getValue() == ALL
                        ?
                        serviceProviderService.getAllServiceProvidersIdByIndustryId(selectedIndustry)
                        :
                        Collections.singletonList((long) dashboardRequestDTO.getServiceProvider().getValue());

        //get all anomalies of service providers from particular dates
        List<DashboardAnomalyStatusInterface> anomalyCount =
                consumerAnomalyService.countAnomaliesByServiceProviderAndAnomalyStatus(startDate, endDate, serviceProviderIds);

        //Populate anomalies by count for Reported, Resolved, Pending and Withdrawn
        anomalyCount.forEach(it -> {
            if (it.getStatus().getStatus().equalsIgnoreCase(AnomalyStatus.REPORTED.getStatus())) {
                reportedDashboardObject.getValues().add(new DashboardObject(it.getName(), it.getAnomalyCount(), it.getServiceProviderId()));
            } else if (it.getStatus().getStatus().equalsIgnoreCase(AnomalyStatus.RESOLVED_SUCCESSFULLY.getStatus())) {
                resolvedDashboardObject.getValues().add(new DashboardObject(it.getName(), it.getAnomalyCount(), it.getServiceProviderId()));
            } else if (it.getStatus().getStatus().equalsIgnoreCase(AnomalyStatus.WITHDRAWN.getStatus())) {
                withdrawnDashboardObject.getValues().add(new DashboardObject(it.getName(), it.getAnomalyCount(), it.getServiceProviderId()));
            } else {
                //Condition to handle the count update for anomalies other than reported, resolved and withdrawn
                if (pendingDashboardObject.getValues().contains(new DashboardObject(it.getName(), it.getAnomalyCount(), it.getServiceProviderId()))) {
                    int index = pendingDashboardObject.getValues().indexOf(new DashboardObject(it.getName(), it.getAnomalyCount(), it.getServiceProviderId()));
                    int count = pendingDashboardObject.getValues().get(index).getValue();
                    pendingDashboardObject.getValues().remove(new DashboardObject(it.getName(), it.getAnomalyCount(), it.getServiceProviderId()));
                    pendingDashboardObject.getValues().add(new DashboardObject(it.getName(), count + it.getAnomalyCount(), it.getServiceProviderId()));
                } else {
                    pendingDashboardObject.getValues().add(new DashboardObject(it.getName(), it.getAnomalyCount(), it.getServiceProviderId()));
                }
            }

        });

        //populate list of service providers based on industry
        listServiceProvider.addAll(getDashboardServiceProviders(selectedIndustry));

        //populate list of service types based on industry
        listServiceType.addAll(getDashboardServiceTypes(selectedIndustry));

        ////count Service Providers based on selected industry for selected dates
        //numServiceProviders = serviceProviderService.countServiceProvidersByIndustryId(selectedIndustry, startDate, endDate);

        //count non-consistent consumers based on selected service provider for selected dates
        numConsistentConsumers = (int) consumerService.countConsumersByServiceProvidersBetweenDates(serviceProviderIds, startDate, endDate, true, 0);

        //count consistent consumer based on selected service provider for selected dates
        numNonConsistentConsumers = (int) consumerService.countConsumersByServiceProvidersBetweenDates(serviceProviderIds, startDate, endDate, false, 0);

        //number of subscriptions is equal to sum of all consumers
        numSubscriptions = (int) consumerService.countSubscribersByServiceProvidersBetweenDates(serviceProviderIds, startDate, endDate, 0);

        //count anomalies other than withdrawn and resolved based on service providers for selected dates
        numAnomaliesReported = anomalyService.getAnomaliesReportedByServiceProvidersAndDates(serviceProviderIds, DEFAULT_ANOMALY_STATUS_LIST, startDate, endDate);

        numAnomaliesInProgress = anomalyService.getAnomaliesReportedByServiceProvidersAndDates(serviceProviderIds, DEFAULT_ANOMALY_PENDING_LIST, startDate, endDate);

        numAnomaliesWithdrawn = anomalyService.getAnomaliesReportedByServiceProvidersAndDates(serviceProviderIds, DEFAULT_ANOMALY_WITHDRAWN_LIST, startDate, endDate);

        //count resolved anomalies based on service providers for selected dates
        numAnomaliesResolved =
                anomalyService.getAnomaliesReportedByServiceProvidersAndDates(serviceProviderIds, DEFAULT_ANOMALY_RESOLVED_STATUS, startDate, endDate);


        //get consumers count grouped by service providers for selected dates
        //consumers = consumerService.getAndCountConsumersGroupedByServiceProviderId(serviceProviderIds, startDate, endDate);
        List<DashboardObjectInterface> consumersList = new ArrayList<>();
        long totalConsumers = consumerService.getTotalConsumers();
        //long consumersPerOperator = consumerService.getConsumersPerOperator();

        //consumers = new ArrayList<>(Arrays.asList(new DashboardObject("Total consumers", (int) totalConsumers),new DashboardObject("Consumers per operator", (int) consumersPerOperator)));

        // 1. Total consumers
        consumersList.add(new DashboardObject("Total consumers", (int) totalConsumers));


        List<DashboardObjectInterface> resolutionMetricsList = new ArrayList<>();
        //TODO:: Validate Logic
        //count Average Resolution Time based for selected dates
        double numAverageResolutionTime = anomalyService.getAverageResolutionTimeInHours(selectedIndustry, serviceProviderIds, startDate, endDate);
        resolutionMetricsList.add(new DashboardObject("Monthly distribution of resolution time", (int) numAverageResolutionTime));
        
        List<Object[]> results = anomalyService.getResolutionMetrics(selectedIndustry, serviceProviderIds, startDate, endDate);
        List<ResolutionMetricDTO> metrics = new ArrayList<>();
        for (Object[] row : results) {
            String label = (String) row[0];
            Long resolved = ((Number) row[1]).longValue();
            Long unresolved = ((Number) row[2]).longValue();

            metrics.add(new ResolutionMetricDTO(label, resolved, unresolved));
        }
        

        // 2. Consumers per operator
        List<Object[]> result = consumerService.getConsumersPerOperator();
        for (Object[] row : result) {
            String operator = (String) row[0];
            Long count = ((Number) row[1]).longValue();
            consumersList.add(new DashboardObject(
                    "Consumers per " + operator + " operator", Math.toIntExact(count)
            ));
        }
        System.out.println("Customers value are "+consumersList.size());

        long activeCount = anomalyService.countByStatusNotIn(Arrays.asList(
                AnomalyStatus.RESOLVED_SUCCESSFULLY,
                AnomalyStatus.WITHDRAWN
        ));



        // Build anomalies list
        List<DashboardObjectInterface> anomaliesList = new ArrayList<>();
        anomaliesList.add(new DashboardObject("Active Anomalies", (int) activeCount));

//         anomalyTypes = consumerService.buildAnomalyTypes(serviceProviderIds, 1000) // pick your threshold
//                .stream()
//                .map(it -> new DashboardObject(it.getName(), it.getValue()))
//                .collect(Collectors.toList());
        
        //Anomaly count based on anomaly types
        anomalyTypes = consumerService.buildAnomalyTypes(serviceProviderIds, startDate, endDate)
              .stream()
              .map(it -> new DashboardObject(it.getName(), it.getValue()))
              .collect(Collectors.toList());

        //get subscription count grouped by service providers for selected dates
        subscriptions = consumerService.getAndCountDistinctConsumersGroupedByServiceProviderId(serviceProviderIds, startDate, endDate);

        //count Service Providers based on selected industry for selected dates
        numServiceProviders = subscriptions.size();


        //get colors of service providers
        serviceProviderColors = getColorsOfServiceProviders(serviceProviderIds);
        DashboardResponseDTO dashboardResponseDTO = new DashboardResponseDTO();

        dashboardResponseDTO.setListIndustry(listIndustry);
        dashboardResponseDTO.setListServiceProvider(listServiceProvider);
        dashboardResponseDTO.setListServiceType(listServiceType);
        dashboardResponseDTO.setNumServiceProviders(numServiceProviders);
        dashboardResponseDTO.setNumConsistentConsumers(numConsistentConsumers);
        dashboardResponseDTO.setNumNonConsistentConsumers(numNonConsistentConsumers);
        dashboardResponseDTO.setNumSubscriptions(numSubscriptions);
        dashboardResponseDTO.setNumAnomaliesReported(numAnomaliesReported);
        dashboardResponseDTO.setNumAnomaliesResolved(numAnomaliesResolved);

       // dashboardResponseDTO.setNumAverageResolutionTime(numAverageResolutionTime);
        dashboardResponseDTO.setSubscriptions(subscriptions);
        dashboardResponseDTO.setAnomaliesByStatus(anomaliesByStatus);

        dashboardResponseDTO.setColor(serviceProviderColors);
        dashboardResponseDTO.setNumAnomaliesInProgress(numAnomaliesInProgress);
        dashboardResponseDTO.setNumAnomaliesWithdrawn(numAnomaliesWithdrawn);

        dashboardResponseDTO.setConsumers(consumersList);
        dashboardResponseDTO.setAnomalies(anomaliesList);
        dashboardResponseDTO.setAnomalyTypes(anomalyTypes);
        dashboardResponseDTO.setResolutionMetrics(resolutionMetricsList);
        dashboardResponseDTO.setAverageResolutionMetrics(metrics);

        return dashboardResponseDTO;
    }

    private List<DashboardObjectInterface> getDashboardServiceProviders(Long id) {
        return serviceProviderService.getAllServiceProvidersByIndustryIdForDashboard(id);
    }

    private List<DashboardObjectInterface> getDashboardServiceTypes(Long id) {
        return serviceTypeService.getAllServiceTypesByIndustryIdForDashboard(id);
    }

    private List<DashboardObjectInterface> getColorsOfServiceProviders(List<Long> serviceProviderIds) {
        return serviceProviderService.findColorsByServiceProviderIds(serviceProviderIds);
    }


    @Override
    public DashboardResponseDTO getAnomalyTimeSeries(DashboardRequestDTO dashboardRequestDTO) throws NullPointerException {

        //Initialization of Lists
        List<DashboardObjectInterface>
                timeAnomalies,
                timeSubscriptions,
                timeConsumers;


        //Get start and end dates from request DTO
        Date
                startDate = dashboardRequestDTO.getStartDate(),
                endDate = dashboardRequestDTO.getEndDate();

        long diff = endDate.getTime() - startDate.getTime();
        long daysBetween = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
        System.out.println(diff);
        System.out.println(daysBetween);
        //get selected industry based on default response
        long selectedIndustry =
                dashboardRequestDTO.isDefaultResponse()
                        ?
                        industryService.getAllIndustries().get(0).getValue()
                        :
                        (long) dashboardRequestDTO.getIndustry().getValue();

        List<Long> serviceProviderIds =
                dashboardRequestDTO.isDefaultResponse() || dashboardRequestDTO.getServiceProvider().getValue() == ALL
                        ?
                        serviceProviderService.getAllServiceProvidersIdByIndustryId(selectedIndustry)
                        :
                        Collections.singletonList((long) dashboardRequestDTO.getServiceProvider().getValue());


        if (daysBetween > DEFAULT_DAYS_INTERVAL) {
            timeAnomalies = anomalyService.getAnomaliesByServiceProviderAndStatusGroupByMonthYear(
                    serviceProviderIds,
                    DEFAULT_ANOMALY_STATUS_LIST,
                    startDate,
                    endDate);

            timeSubscriptions = consumerService.getAndCountDistinctConsumersByServiceProviderBetweenDatesGroupByMonthYear(
                    serviceProviderIds,
                    startDate,
                    endDate, 0);

            timeConsumers = consumerService.getAndCountConsumersByServiceProviderBetweenDatesGroupByMonthYear(
                    serviceProviderIds,
                    startDate,
                    endDate);
        } else {
            timeAnomalies = anomalyService.getAnomaliesByServiceProviderAndStatusGroupByDateMonthYear(
                    serviceProviderIds,
                    DEFAULT_ANOMALY_STATUS_LIST,
                    startDate,
                    endDate);

            timeSubscriptions = consumerService.getAndCountDistinctConsumersByServiceProviderBetweenDatesGroupByDateMonthYear(
                    serviceProviderIds,
                    startDate,
                    endDate, 0);

            timeConsumers = consumerService.getAndCountConsumersByServiceProviderBetweenDatesGroupByDateMonthYear(
                    serviceProviderIds,
                    startDate,
                    endDate);
        }

        DashboardResponseDTO dashboardResponseDTO = new DashboardResponseDTO();
        dashboardResponseDTO.setTimeFixedAnomalies(timeAnomalies);
        dashboardResponseDTO.setTimeConsumers(timeConsumers);
        dashboardResponseDTO.setTimeSubscriptions(timeSubscriptions);
        return dashboardResponseDTO;
    }


}
