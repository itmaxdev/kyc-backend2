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

@Service
@Slf4j
public class ConsumerServiceImpl implements ConsumerService {
    @Autowired
    private ServiceProviderRepository serviceProviderRepository;
    
    @Autowired
    private ConsumerAnomalyRepository consumerAnomalyRepository;
    
    @Autowired
    AnomalyRepository anomalyRepository;
    
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
    
    public ConsumerDto getConsumerById(Long id) {
        Optional<Consumer> consumer = Optional.ofNullable(consumerRepository.findByIdAndConsumerStatus(id, 0));
        ConsumerDto consumerDto = null;
        if (consumer.isPresent()) {
            consumerDto = new ConsumerDto(consumer.get(), consumer.get().getAnomalies());
        }
        
        return consumerDto;
    }

    public Map<String, Object> getAllConsumers(String params) throws JsonMappingException, JsonProcessingException {
        List<ConsumerDto> pageConsumers = null;
        Long totalInConsistentCustomer;
        List<ConsumersHasSubscriptionsResponseDTO> consumersHasSubscriptionsResponseDTO = null;
        Pagination pagination = PaginationUtil.getFilterObject(params);
        //3 checks, 1 is for whole filter object, 2nd is for filter consistent and 3rd check is for filter consistent value.
        // TODO enchance logic for consumerAnomaly
        if (!Objects.isNull(pagination.getFilter()) && !Objects.isNull(pagination.getFilter().getConsistent()) && !pagination.getFilter().getConsistent()) {
            Page<Consumer> consumerData =  consumerRepository.findByIsConsistentFalseAndConsumerStatus(PaginationUtil.getPageable(params), 0);
            
            pageConsumers = consumerData
            .stream()
            .map(c -> new ConsumerDto(c, c.getAnomalies())).collect(Collectors.toList());
            totalInConsistentCustomer = consumerData.getTotalElements();
            
        } else {
            Page<Consumer> consumerData = consumerRepository.findByIsConsistentTrueAndConsumerStatus(PaginationUtil.getPageable(params), 0);
            
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




  /*  @Override
    public long getConsumersPerOperator(){
        return consumerRepository.getConsumersPerOperator();
    }

    public List<Object[]> getConsumersPerOperatorBreakdown(){
        return consumerRepository.getConsumersPerOperatorBreakdown();
    }
*/
    
    @Override
    public Map<String, Object> getAllFlaggedConsumers2(String params) throws JsonMappingException, JsonProcessingException {
        //List<AnomlyDto> pageAnomaly = anomalyRepository.findAll(PaginationUtil.getPageable(params)).stream().map(a -> new AnomlyDto(a)).collect(Collectors.toList());
        Pagination pagination = PaginationUtil.getFilterObject(params);
        List<Integer> consumerStatus = new ArrayList<>();
        consumerStatus.add(0);
        List<AnomlyDto> pageAnomaly = null;
        List<AnomalyStatus> anomalyStatus = new ArrayList<>();
        long totalAnomaliesCount = 0L;

        if (Objects.nonNull(pagination.getFilter()) && (Objects.isNull(pagination.getFilter().getServiceProviderID()) || pagination.getFilter().getServiceProviderID() == -1)) {
            if(pagination.getFilter().getIsResolved()){
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumerStatus(PaginationUtil.getPageable(params), consumerStatus, anomalyStatus);
                pageAnomaly = anomalyData.stream()
                        .map(a -> new AnomlyDto(a , 0)).collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();


            }
            else{
                anomalyStatus.add(AnomalyStatus.REPORTED);
                anomalyStatus.add(AnomalyStatus.QUESTION_SUBMITTED);
                anomalyStatus.add(AnomalyStatus.UNDER_INVESTIGATION);
                anomalyStatus.add(AnomalyStatus.QUESTION_ANSWERED);
                anomalyStatus.add(AnomalyStatus.RESOLUTION_SUBMITTED);
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumerStatus(PaginationUtil.getPageable(params), consumerStatus, anomalyStatus);
                pageAnomaly = anomalyData.stream().map(a -> new AnomlyDto(a)).collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();

            }
        }
        else {
            if(pagination.getFilter().getIsResolved()){
                consumerStatus.add(1);
                anomalyStatus.add(AnomalyStatus.RESOLVED_SUCCESSFULLY);
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumerStatusAndServiceProviderId(PaginationUtil.getPageable(params), consumerStatus, pagination.getFilter().getServiceProviderID(), anomalyStatus);

                pageAnomaly = anomalyData
                .stream()
                .map(c -> new AnomlyDto(c,0)).collect(Collectors.toList());
                totalAnomaliesCount = anomalyData.getTotalElements();
            }
            else{
                anomalyStatus.add(AnomalyStatus.REPORTED);
                anomalyStatus.add(AnomalyStatus.QUESTION_SUBMITTED);
                anomalyStatus.add(AnomalyStatus.UNDER_INVESTIGATION);
                anomalyStatus.add(AnomalyStatus.QUESTION_ANSWERED);
                anomalyStatus.add(AnomalyStatus.RESOLUTION_SUBMITTED);
                Page<Anomaly> anomalyData = anomalyRepository.findAllByConsumerStatusAndServiceProviderId(PaginationUtil.getPageable(params), consumerStatus, pagination.getFilter().getServiceProviderID(), anomalyStatus);

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
                    tempConsumer.setCreatedOn(currentDate);
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
                tempConsumer.setCreatedOn(currentDate);
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
                    tempConsumer.setCreatedOn(date);
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
                tempConsumer.setCreatedOn(date);
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
                    tempConsumer.setCreatedOn(currentDate);
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

    private void checkConsumer(List<Consumer> consumers, User user, ServiceProvider serviceProvider) {
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
    }
    
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
    
    private void tagDuplicateAnomalies(Consumer consumer, User user) {
        
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Duplicate Records");
        
        Anomaly tempAnomaly = new Anomaly();
        Anomaly anomaly = new Anomaly();
        String note = "You can't have more than one active record per MSISDN: " + consumer.getMsisdn();
        tempAnomaly.setNote("Duplicate Anomaly: "+note);
        
        // get previous consumers
        List<Consumer> duplicateConsumers = consumerRepository.findByMsisdnAndConsumerStatus(consumer.getMsisdn(), 0);
        List<Long> duplicateConsumerIds = duplicateConsumers.stream().map(Consumer::getId).collect(Collectors.toList());
        
        consumer.setIsConsistent(false);
        consumer = consumerRepository.save(consumer);
        
        // check anomaly of previous consumers
        List<Long> consumerAnomalies = consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(duplicateConsumerIds, anomalyType.getId());
        
        if (consumerAnomalies.isEmpty()) {
            // make new anomaly
            tempAnomaly.setNote("Duplicate Anomaly: "+note);
            tempAnomaly.setStatus(AnomalyStatus.REPORTED);
            tempAnomaly.setReportedOn(new Date());
            tempAnomaly.setReportedBy(user);
            tempAnomaly.getConsumers().remove(consumer);
            tempAnomaly.addConsumer(consumer);
            tempAnomaly.setUpdatedOn(new Date());
            tempAnomaly.setAnomalyType(anomalyType);
            tempAnomaly = anomalyRepository.save(tempAnomaly);

            AnomalyTracking anomalyTracking = new AnomalyTracking(tempAnomaly, new Date(), AnomalyStatus.REPORTED, "", user.getFirstName()+" "+user.getLastName(), tempAnomaly.getUpdatedOn());
            anomalyTrackingRepository.save(anomalyTracking);

            List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(tempAnomaly.getId(), consumer.getId());
            temp.get(0).setNotes("Duplicate Anomaly: "+note);
            consumerAnomalyRepository.save(temp.get(0));
        } else {
            //load anomaly nad tag to new consumer
            tempAnomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());
            if (!Objects.isNull(tempAnomaly)) {
                ConsumerAnomaly consumerAnomaly = new ConsumerAnomaly();
                anomaly.setId(tempAnomaly.getId());
                anomaly.setNote(tempAnomaly.getNote());
                anomaly.setStatus(tempAnomaly.getStatus());
                anomaly.setReportedOn(tempAnomaly.getReportedOn());
                anomaly.setReportedBy(tempAnomaly.getReportedBy());
                anomaly.getConsumers().remove(consumer);
                anomaly.setUpdatedOn(tempAnomaly.getUpdatedOn());
                anomaly.addConsumer(consumer);
                anomaly.setAnomalyType(tempAnomaly.getAnomalyType());
                
                consumerAnomaly.setAnomaly(anomaly);
                consumerAnomaly.setConsumer(consumer);
                consumerAnomaly.setNotes("Duplicate Anomaly: "+note);
                consumerAnomalyRepository.save(consumerAnomaly);

            } else {
                tempAnomaly = new Anomaly();
                tempAnomaly.setNote("Duplicate Anomaly: "+note);
                tempAnomaly.setStatus(AnomalyStatus.REPORTED);
                tempAnomaly.setReportedOn(new Date());
                tempAnomaly.setReportedBy(user);
                tempAnomaly.setAnomalyType(anomalyType);
                tempAnomaly.setUpdatedOn(new Date());
                tempAnomaly.setUpdateBy(user.getFirstName()+" "+ user.getLastName());
                Anomaly savedAnomaly = anomalyRepository.save(tempAnomaly);
                AnomalyTracking anomalyTracking = new AnomalyTracking(tempAnomaly, new Date(), AnomalyStatus.REPORTED, "", user.getFirstName()+" "+user.getLastName(), tempAnomaly.getUpdatedOn());
                anomalyTrackingRepository.save(anomalyTracking);
                ConsumerAnomaly consumerAnomaly= new ConsumerAnomaly();
                consumerAnomaly.setNotes("Duplicate Anomaly: "+note);
                consumerAnomaly.setAnomaly(savedAnomaly);
                consumerAnomaly.setConsumer(consumer);
                consumerAnomalyRepository.save(consumerAnomaly);
            }
        }
        
        
        // tag anomaly to new duplicate consumers
        for (Consumer temp : duplicateConsumers) {
            List<ConsumerAnomaly> consumerAnomaly = consumerAnomalyRepository.findByAnomaly_AnomalyTypeAndConsumer(anomalyType, temp);
            if (consumerAnomaly.size() < 1) {
                ConsumerAnomaly tempConsumerAnomaly = new ConsumerAnomaly();
                tempConsumerAnomaly.setAnomaly(tempAnomaly);
                tempConsumerAnomaly.setConsumer(temp);
                tempConsumerAnomaly.setNotes("Duplicate Anomaly: "+note);
                consumerAnomalyRepository.save(tempConsumerAnomaly);
            }
        }
        
        // mark inConsistent consumers
        consumerRepository.markConsumersConsistent(0, duplicateConsumerIds);
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
    
    private void tagExceedingAnomalies(Consumer consumer, User user) {
        
        AnomalyType anomalyType = anomalyTypeRepository.findFirstByName("Exceeding Threshold");
        
        Anomaly tempAnomaly = new Anomaly();
        Anomaly anomaly = new Anomaly();
        String note = "You can't have more than two active records per operator for a given combination of (ID Card Type + ID Number + ServiceProviderName): (" + consumer.getIdentificationType() + " + " + consumer.getIdentificationNumber() + consumer.getServiceProvider().getName() +  ") ";
        tempAnomaly.setNote("Exceeding Anomaly: "+note);
        // get previous consumers
        List<Consumer> duplicateConsumers = consumerRepository.findByIdentificationTypeAndIdentificationNumberAndServiceProviderAndConsumerStatus(consumer.getIdentificationType(), consumer.getIdentificationNumber(), consumer.getServiceProvider(), 0);
        List<Long> duplicateConsumerIds = duplicateConsumers.stream().map(Consumer::getId).collect(Collectors.toList());
        
        consumer.setIsConsistent(false);
        consumer = consumerRepository.save(consumer);
        
        // check anomaly of previous consumers
        List<Long> consumerAnomalies = consumerAnomalyRepository.findAnomaliesIdByConsumerAndAnomalyTypeId(duplicateConsumerIds, anomalyType.getId());
        
        if (consumerAnomalies.isEmpty()) {
            // make new anomaly
            tempAnomaly.setNote("Exceeding Anomaly: "+note);
            tempAnomaly.setStatus(AnomalyStatus.REPORTED);
            tempAnomaly.setReportedOn(new Date());
            tempAnomaly.setReportedBy(user);
            tempAnomaly.setUpdatedOn(new Date());
            tempAnomaly.getConsumers().remove(consumer);
            tempAnomaly.addConsumer(consumer);
            tempAnomaly.setAnomalyType(anomalyType);
            tempAnomaly = anomalyRepository.save(tempAnomaly);

            AnomalyTracking anomalyTracking = new AnomalyTracking(tempAnomaly, new Date(), AnomalyStatus.REPORTED, "", user.getFirstName()+" "+user.getLastName(), tempAnomaly.getUpdatedOn());
            anomalyTrackingRepository.save(anomalyTracking);

            List<ConsumerAnomaly> temp = consumerAnomalyRepository.findByAnomaly_IdAndConsumer_Id(tempAnomaly.getId(), consumer.getId());
            temp.get(0).setNotes("Exceeding Anomaly: "+note);
            consumerAnomalyRepository.save(temp.get(0));
        } else {
            //load anomaly nad tag to new consumer
            tempAnomaly = anomalyRepository.findByIdAndAnomalyType_Id(consumerAnomalies, anomalyType.getId());
            if (!Objects.isNull(tempAnomaly)) {
                ConsumerAnomaly consumerAnomaly = new ConsumerAnomaly();
                anomaly.setId(tempAnomaly.getId());
                anomaly.setNote(tempAnomaly.getNote());
                anomaly.setStatus(tempAnomaly.getStatus());
                anomaly.setReportedOn(tempAnomaly.getReportedOn());
                anomaly.setReportedBy(tempAnomaly.getReportedBy());
                anomaly.getConsumers().remove(consumer);
                anomaly.addConsumer(consumer);
                anomaly.setAnomalyType(tempAnomaly.getAnomalyType());
                anomaly.setUpdatedOn(tempAnomaly.getUpdatedOn());
                
                consumerAnomaly.setAnomaly(anomaly);
                consumerAnomaly.setConsumer(consumer);
                consumerAnomaly.setNotes("Exceeding Anomaly: "+note);
                consumerAnomalyRepository.save(consumerAnomaly);
            } else {
                
                tempAnomaly = new Anomaly();
                tempAnomaly.setStatus(AnomalyStatus.REPORTED);
                tempAnomaly.setNote("Exceeding Anomaly: "+note);
                tempAnomaly.setReportedOn(new Date());
                tempAnomaly.setReportedBy(user);
                tempAnomaly.setAnomalyType(anomalyType);
                tempAnomaly.setUpdateBy(user.getFirstName()+" "+user.getLastName());
                tempAnomaly.setUpdatedOn(new Date());
                Anomaly savedAnomaly = anomalyRepository.save(tempAnomaly);

                AnomalyTracking anomalyTracking = new AnomalyTracking(tempAnomaly, new Date(), AnomalyStatus.REPORTED, "", user.getFirstName()+" "+user.getLastName(), tempAnomaly.getUpdatedOn());
                anomalyTrackingRepository.save(anomalyTracking);
                
                ConsumerAnomaly consumerAnomaly = new ConsumerAnomaly();
                consumerAnomaly.setNotes("Exceeding Anomaly: "+note);
                consumerAnomaly.setAnomaly(savedAnomaly);
                consumerAnomaly.setConsumer(consumer);
                consumerAnomalyRepository.save(consumerAnomaly);
            }
        }
        
        
        // tag anomaly to new duplicate consumers
        for (Consumer temp : duplicateConsumers) {
            List<ConsumerAnomaly> consumerAnomaly = consumerAnomalyRepository.findByAnomaly_AnomalyTypeAndConsumer(anomalyType, temp);
            if (consumerAnomaly.size() < 1) {
                ConsumerAnomaly tempConsumerAnomaly = new ConsumerAnomaly();
                tempConsumerAnomaly.setAnomaly(anomaly.getId() == null ? tempAnomaly : anomaly);
                tempConsumerAnomaly.setConsumer(temp);
                tempConsumerAnomaly.setNotes("Exceeding Anomaly: "+note);
                consumerAnomalyRepository.save(tempConsumerAnomaly);
            }
        }
        
        consumerRepository.markConsumersConsistent(0, duplicateConsumerIds);
        
    }
    
    private List<String> checkErrors(Consumer consumer) {
        List<String> errors = new ArrayList<>();
        if (consumer.getMsisdn() == null) {
            errors.add("MSSIDN");
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
            nullAttributesOfFile.add("MSSIDN");
        }
        if (consumer.getFirstName() == null || consumer.getFirstName().equals("")) {
            nullAttributesOfFile.add("firstName");
        }
        
        if (consumer.getLastName() == null || consumer.getLastName().equals("")) {
            nullAttributesOfFile.add("lastName");
        }
        if (consumer.getGender() == null || consumer.getGender().equals("")) {
            nullAttributesOfFile.add("gender");
        }
        if (consumer.getNationality() == null || consumer.getNationality().equals("")) {
            nullAttributesOfFile.add("nationality");
        }
        if (consumer.getBirthPlace() == null || consumer.getBirthPlace().equals("")) {
            nullAttributesOfFile.add("birthPlace");
        }
        if (consumer.getBirthDate() == null || consumer.getBirthDate().equals("")) {
            nullAttributesOfFile.add("birthday");
        }
        if (consumer.getRegistrationDate() == null || consumer.getRegistrationDate().equals("")) {
            nullAttributesOfFile.add("Registration Date");
        }
        
        
        if (consumer.getIdentificationNumber() == null || consumer.getIdentificationNumber().equals("")) {
            nullAttributesOfFile.add("code");
        }
        if (consumer.getIdentificationType() == null || consumer.getIdentificationType().equals("")) {
            nullAttributesOfFile.add("typePiece");
        }
        if (consumer.getAddress() == null || consumer.getAddress().equals("")) {
            nullAttributesOfFile.add("address");
        }
        if (consumer.getAddress() == null || consumer.getAddress().equals("")) {
            nullAttributesOfFile.add("district");
        }
        if (consumer.getAddress() == null || consumer.getAddress().equals("")) {
            nullAttributesOfFile.add("houseNumber");
        }
        if (consumer.getAddress() == null || consumer.getAddress().equals("")) {
            nullAttributesOfFile.add("commune");
        }
        if (consumer.getAddress() == null || consumer.getAddress().equals("")) {
            nullAttributesOfFile.add("province");
        }
        // if (consumer.getCreatedOn() == null || consumer.getCreatedOn().equals("")) {
        //     nullAttributesOfFile.add("recievedDate");
        // }
        if (consumer.getIdentityCapturePath() == null || consumer.getIdentityCapturePath().equals("")) {
            nullAttributesOfFile.add("fullPath");
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
}
