package com.app.kyc.service;

import java.util.*;
import java.util.stream.Collectors;

import com.app.kyc.entity.*;
import com.app.kyc.model.*;
import com.app.kyc.repository.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.crossstore.ChangeSetPersister.NotFoundException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import com.app.kyc.response.ServiceProviderAllInfoResponseDTO;
import com.app.kyc.response.ServiceProviderUserResponseDTO;
import com.app.kyc.response.ServiceUserResponseDTO;
import com.app.kyc.util.PaginationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ServiceProviderServiceImpl implements ServiceProviderService
{

   @Autowired
   private ServiceProviderRepository serviceProviderRepository;

   @Autowired
   private ServiceRepository serviceRepository;

   @Autowired
   private ConsumerRepository consumerRepository;

   @Autowired
   private AnomalyRepository anomalyRepository;

   @Autowired
   private ServiceService serviceService;

   @Autowired
   private UserService userService;

   @Autowired
   private AnomalyService anomalyService;

   @Autowired
   private ConsumerServiceService consumerServiceService;

   @Autowired
   private ConsumerService consumerService;

   @Autowired
   private ConsumerAnomalyRepository consumerAnomalyRepository;

   public ServiceProvider getServiceProviderById(Long id)
   {
      return serviceProviderRepository.findById(id).get();
   }

   public ServiceProvider getServiceProviderByName(String name)
   {
      return serviceProviderRepository.findByName(name);
   }

   /*public Map<String, Object> getAllServiceProviders(String params) throws JsonMappingException, JsonProcessingException
   {
      Page<ServiceProvider> pageServiceProvider = serviceProviderRepository.findAll(PaginationUtil.getPageable(params));
      List<ServiceProvider> serviceProviders = pageServiceProvider.toList();
      List<ServiceProviderUserResponseDTO> response = new ArrayList<ServiceProviderUserResponseDTO>();
      for(ServiceProvider s : serviceProviders)
      {
         List<com.app.kyc.entity.Service> services = serviceRepository.findByServiceProvider(s.getId());
         ServiceProviderUserResponseDTO obj = new ServiceProviderUserResponseDTO(s.getId(), s.getName(), s.getCreatedOn(), s.getIndustry().getName(),
                 userService.getUserById(s.getCreatedBy()), services.isEmpty() ? false : true, s.getColor());
         response.add(obj);
      }
      Map<String, Object> serviceProvidersWithCount = new HashMap<String, Object>();
      serviceProvidersWithCount.put("data", response);
      serviceProvidersWithCount.put("count", pageServiceProvider.getTotalElements());
      return serviceProvidersWithCount;
   }*/

   public Map<String, Object> getAllServiceProviders(String params) throws JsonMappingException, JsonProcessingException {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> paramMap = mapper.readValue(params, new TypeReference<Map<String, Object>>() {});

      // 🔹 Extract pagination safely
      Map<String, Object> pagination = (Map<String, Object>) paramMap.get("pagination");
      if (pagination == null) {
         pagination = new HashMap<>();
         paramMap.put("pagination", pagination);
      }

      // Default page
      int page = (int) pagination.getOrDefault("page", 0);

      // Fix perPage → if <= 1, fetch all
      int perPage = 1;
      if (pagination.containsKey("perPage")) {
         Object perPageObj = pagination.get("perPage");
         if (perPageObj instanceof Number) {
            perPage = ((Number) perPageObj).intValue();
         }
      }

      if (perPage <= 1) {
         perPage = Integer.MAX_VALUE; // fetch all rows
      }

      // 🔹 Build Pageable manually (respect sort if given)
      Map<String, Object> sortMap = (Map<String, Object>) paramMap.get("sort");
      Sort sort = Sort.by("id").ascending(); // default
      if (sortMap != null && sortMap.containsKey("field") && sortMap.containsKey("order")) {
         String field = sortMap.get("field").toString();
         String order = sortMap.get("order").toString();
         sort = order.equalsIgnoreCase("DESC") ? Sort.by(field).descending() : Sort.by(field).ascending();
      }

      Pageable pageable = PageRequest.of(page, perPage, sort);

      // 🔹 Fetch data
      //Page<ServiceProvider> pageServiceProvider = serviceProviderRepository.findAll(pageable);
      //Page<ServiceProvider> pageServiceProvider = serviceProviderRepository.findAllByStatus(,pageable);
      Page<ServiceProvider> pageServiceProvider =
              serviceProviderRepository.findAllByStatus(ServiceProviderStatus.Active, pageable);

      List<ServiceProviderUserResponseDTO> response = new ArrayList<>();
      for (ServiceProvider s : pageServiceProvider) {
         List<com.app.kyc.entity.Service> services = serviceRepository.findByServiceProvider(s.getId());
         ServiceProviderUserResponseDTO obj = new ServiceProviderUserResponseDTO(
                 s.getId(),
                 s.getName(),
                 s.getCreatedOn(),
                 s.getIndustry().getName(),
                 userService.getUserById(s.getCreatedBy()),
                 !services.isEmpty(),
                 s.getColor()
         );
         response.add(obj);
      }

      // 🔹 Wrap response
      Map<String, Object> serviceProvidersWithCount = new HashMap<>();
      serviceProvidersWithCount.put("data", response);
      serviceProvidersWithCount.put("count", pageServiceProvider.getTotalElements());

      return serviceProvidersWithCount;
   }


   @Override
   public List<ServiceProvider> getAllServiceProviders()
   {
      return serviceProviderRepository.findAll();
   }

   @Override
   public List<DashboardObjectInterface> getAllServiceProvidersByIndustryIdForDashboard(Long industryId)
   {
      return serviceProviderRepository.findAllByIndustryIdForDashboard(industryId);
   }

   @Override
   public List<Long> getAllServiceProvidersIdByIndustryId(Long industryId)
   {
      return serviceProviderRepository.findServiceProvidersIdByIndustryId(industryId);
   }

   public void addServiceProvider(ServiceProvider serviceProvider)
   {
      serviceProvider.setCreatedOn(new Date());
      serviceProviderRepository.save(serviceProvider);
   }

   @Override
   public List<DashboardObjectInterface> findColorsByServiceProviderIds(List<Long> serviceProviderIds)
   {
      return serviceProviderRepository.findColorsByServiceProviderIds(serviceProviderIds);
   }

   public ServiceProvider updateServiceProvider(ServiceProvider serviceProvider) throws NotFoundException
   {
      ServiceProvider originalServiceProvider = getServiceProviderById(serviceProvider.getId());
      if(originalServiceProvider == null) throw new NotFoundException();
      originalServiceProvider.setAddress(serviceProvider.getAddress());
      originalServiceProvider.setCompanyPhoneNumber(serviceProvider.getCompanyPhoneNumber());
      originalServiceProvider.setName(serviceProvider.getName());
      originalServiceProvider.setColor(serviceProvider.getColor());
      return serviceProviderRepository.save(originalServiceProvider);
   }

   public void deleteServiceProvider(Long id)
   {
      serviceProviderRepository.deleteById(id);
   }

   public void deleteData(Long serviceProviderId)
   {
      Optional<ServiceProvider> serviceProvider = serviceProviderRepository.findById(serviceProviderId);
      List<Consumer> consumers = this.getAllConsumersByServiceProvider(serviceProvider.get().getId());
      List<ConsumerAnomaly> consumerAnomalies = consumerAnomalyRepository.deleteAllByConsumerIn(consumers);
      consumerRepository.deleteAllByIdIn(consumers.stream().map(Consumer::getId).collect(Collectors.toList()));
      anomalyRepository.deleteAllByIdIn(consumerAnomalies.stream().map(c-> c.getAnomaly().getId()).collect(Collectors.toList()));
   }

   private List<Consumer> getAllConsumersByServiceProvider(Long id){
      List<Consumer> consumerDtos  = consumerRepository.findAllByServiceProvider_Id(id);
      return consumerDtos;
   }

   @Override
   public int countServiceProvidersByIndustryId(Long industryId, Date start, Date end)
   {
      return (int) serviceProviderRepository.countByIndustryIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(industryId, start, end);
   }

   @Override
   public ServiceProviderAllInfoResponseDTO getServiceProviderAllInfoById(Long id)
   {
      ServiceProviderAllInfoResponseDTO response = new ServiceProviderAllInfoResponseDTO();
      ServiceProvider serviceProvider = serviceProviderRepository.findById(id).get();

      response.setId(id);
      response.setName(serviceProvider.getName());
      response.setIndustry(serviceProvider.getIndustry().getName());
      response.setCreatedOn(serviceProvider.getIndustry().getCreatedOn());

      List<com.app.kyc.entity.Service> services = serviceService.getListServicesByServiceProviderId(id);
      List<ServiceUserResponseDTO> servicesResponse = new ArrayList<ServiceUserResponseDTO>();
      int consumersByService = 0;
      for(com.app.kyc.entity.Service s : services)
      {
         consumersByService = consumerServiceService.countConsumersByServiceId(s.getId());
         ServiceUserResponseDTO obj = new ServiceUserResponseDTO(s.getId(), s.getName(), s.getServiceProvider().getName(), s.getCreatedOn(), s.getServiceType(),
                 (s.getCreatedBy() == null ? null : userService.getUserById(s.getCreatedBy())), (s.getApprovedBy() == null ? null : userService.getUserById(s.getApprovedBy())),
                 consumersByService > 0 ? true : false);
         servicesResponse.add(obj);
      }
      response.setServices(servicesResponse);

      List<ConsumerDto> consumers = consumerService.getConsumersByServiceProviderId(id).stream(). map(c->new ConsumerDto(c,c.getAnomalies())).collect(Collectors.toList());

      response.setConsumers(consumers);

      List<AnomlyDto> anomalies = anomalyService.getAnomalyByServiceProviderId(id).stream().map(c-> new AnomlyDto(c)).collect(Collectors.toList());
      response.setAnomalies(anomalies);

      return response;

   }

   @Override
   public void activateService(Long serviceProviderId, Long userId)
   {
      ServiceProvider serviceProvider = getServiceProviderById(serviceProviderId);
      serviceProvider.setStatus(ServiceProviderStatus.Active);
      serviceProvider.setApprovedBy(userId);
      serviceProviderRepository.save(serviceProvider);
   }

   @Override
   public void deactivateService(Long serviceProviderId)
   {
      ServiceProvider serviceProvider = getServiceProviderById(serviceProviderId);
      serviceProvider.setStatus(ServiceProviderStatus.Inactive);
      serviceProvider.setApprovedBy(null);
      serviceProviderRepository.save(serviceProvider);
   }

   @Override
   public Map<String, Object> getAllServiceProvidersByIndustry(Long userId)
   {
      User user = userService.getUserById(userId);
      List<ServiceProvider> serviceProviders = serviceProviderRepository.findAllByIndustryId(user.getIndustry().getId());
      Map<String, Object> serviceProvidersWithCount = new HashMap<String, Object>();
      serviceProvidersWithCount.put("data", serviceProviders);
      serviceProvidersWithCount.put("count", serviceProviders.size());
      return serviceProvidersWithCount;
   }


   public ServiceProvider createServiceProvider(ServiceProvider sp) {

      if (serviceProviderRepository.existsByNameIgnoreCase(sp.getName())) {
         throw new RuntimeException("Service Provider with name '" + sp.getName() + "' already exists!");
      }

      sp.setCreatedOn(new Date());
      sp.setDeleted(false);
      return serviceProviderRepository.save(sp);
   }


   @Transactional
   public ServiceProviderResponse update(Long id, ServiceProviderPutRequest req) {
      ServiceProvider sp = serviceProviderRepository.findById(id)
              .orElseThrow(() -> new IllegalArgumentException("ServiceProvider not found: " + id));

      sp.setName(req.name.trim());
      sp.setAddress(req.address);
      sp.setCompanyPhoneNumber(req.companyPhoneNumber);
      sp.setDeleted(Boolean.TRUE.equals(req.deleted));
      sp.setApprovedBy(req.approvedBy);
      sp.setColor(req.color);

      sp.setStatus(parseStatus(req.status)); // requires valid value

      /*Industry industry = industryRepo.findById(req.industryId)
              .orElseThrow(() -> new IllegalArgumentException("Industry not found: " + req.industryId));
      sp.setIndustry(industry);
*/
      return toResponse(serviceProviderRepository.save(sp));
   }


   private ServiceProviderStatus parseStatus(String raw) {
      if (raw == null) return null;
      String v = raw.trim().toUpperCase();
      // Map common inputs → enum (adjust names to match your enum values)
      if (v.equals("1") || v.equals("ACTIVE"))   return ServiceProviderStatus.Active;
      if (v.equals("0") || v.equals("INACTIVE")) return ServiceProviderStatus.Inactive;
      // fallback to enum name
      return ServiceProviderStatus.valueOf(v);
   }

   private ServiceProviderResponse toResponse(ServiceProvider sp) {
      ServiceProviderResponse r = new ServiceProviderResponse();
      r.id = sp.getId();
      r.name = sp.getName();
      r.address = sp.getAddress();
      r.companyPhoneNumber = sp.getCompanyPhoneNumber();
      r.deleted = sp.isDeleted();
      r.status = Optional.ofNullable(sp.getStatus()).map(Enum::name).orElse(null);
      r.approvedBy = sp.getApprovedBy();
      r.color = sp.getColor();
      if (sp.getIndustry() != null) {
         r.industryId = sp.getIndustry().getId();
         r.industryName = sp.getIndustry().getName();
      }
      return r;
   }
}
