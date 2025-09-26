package com.app.kyc.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.app.kyc.model.AnomalyTypeDto;
import com.app.kyc.model.DashboardObjectInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.crossstore.ChangeSetPersister.NotFoundException;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.AnomalyType;
import com.app.kyc.entity.User;
import com.app.kyc.repository.AnomalyTypeRepository;
import com.app.kyc.response.AnomalyTypesListResponseDTO;
import com.app.kyc.service.common.ErrorCode;
import com.app.kyc.service.exception.InvalidDataException;
import com.app.kyc.service.exception.NotFoundExceptionCustom;
import com.app.kyc.util.PaginationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

@Service
public class AnomalyTypeServiceImpl implements AnomalyTypeService
{

   @Autowired
   private AnomalyTypeRepository anomalyTypeRepository;

   @Autowired
   private ServiceTypeService serviceTypeService;

   @Autowired
   private IndustryService industryService;

   @Autowired
   private UserService userService;

   @Autowired
   private AnomalyService anomalyService;

   @Override
   public AnomalyType getAnomalyTypeById(Long id)
   {
      return anomalyTypeRepository.findById(id).get();
   }

   @Override
   public List<AnomalyType> findAll() {
      return anomalyTypeRepository.findAll();
   }

   public List<DashboardObjectInterface> getAnomalyTypeCounts(){
      return anomalyTypeRepository.getAnomalyTypeCounts();
   }

   public List<AnomalyTypeDto> getAnomalyTypes() {
      List<AnomalyType> types = anomalyTypeRepository.findAll();

      return types.stream()
              .map(type -> new AnomalyTypeDto(
                      type.getName(),
                      mapSeverity(type.getName())  // map business meaning
              ))
              .collect(Collectors.toList());
   }


   private String mapSeverity(String name) {
      switch (name) {
         case "Incomplete Data":
         case "Duplicate Records":
            return "Serious";
         case "Exceeding Threshold":
            return "Critical";
         default:
            return "Unknown";
      }
   }

   @Override
   public Map<String, Object> getAllAnomalyTypes(String params) throws JsonMappingException, JsonProcessingException
   {
      Page<AnomalyType> pageAnomalyTypes = anomalyTypeRepository.findAll(PaginationUtil.getPageable(params));
      List<AnomalyType> anomalyTypes = pageAnomalyTypes.toList();
      List<AnomalyTypesListResponseDTO> response = new ArrayList<AnomalyTypesListResponseDTO>();
      for(AnomalyType at : anomalyTypes)
      {
         AnomalyTypesListResponseDTO atResponse = null;
         // switch(at.getTargetEntityType())
         // {
         //    case IncompleteData:
         //       atResponse = new AnomalyTypesListResponseDTO(at.getId(), at.getName(), at.getTargetEntityType().name(),
         //          serviceTypeService.getServiceTypeById(at.getEntity_id()).getName(), at.getSeverity().name());
         //    break;
         //    case DuplicateRecords:
         //       atResponse = new AnomalyTypesListResponseDTO(at.getId(), at.getName(), at.getTargetEntityType().name(), industryService.getIndustryById(at.getEntity_id()).getName(),
         //          at.getSeverity().name());
         //    break;
         //    case ExceedingThreshold:
         //       atResponse = new AnomalyTypesListResponseDTO(at.getId(), at.getName(), at.getTargetEntityType().name(), null, at.getSeverity().name());
         //    break;
         // }
         atResponse = new AnomalyTypesListResponseDTO(at.getId(), at.getName(), at.getTargetEntityType().name(), null, at.getSeverity().name());
         response.add(atResponse);
      }
      Map<String, Object> anomalyTypesWithCount = new HashMap<String, Object>();
      anomalyTypesWithCount.put("data", response);
      anomalyTypesWithCount.put("count", pageAnomalyTypes.getTotalElements());
      return anomalyTypesWithCount;
   }

   @Override
   public List<AnomalyTypesListResponseDTO> getAllAnomalyTypesByUser(String username) throws NotFoundExceptionCustom
   {
      User user = userService.getUserByEmail(username);
      if(user == null)
      {
         throw new NotFoundExceptionCustom(ErrorCode.EMAIL_NOT_FOUND);
      }
      List<AnomalyType> anomalyTypes = anomalyTypeRepository.findByCreatedBy(user.getId());
      List<AnomalyTypesListResponseDTO> response = new ArrayList<AnomalyTypesListResponseDTO>();
      for(AnomalyType at : anomalyTypes)
      {
         AnomalyTypesListResponseDTO atResponse = null;
         switch(at.getTargetEntityType())
         {
            case IncompleteData:
               atResponse = new AnomalyTypesListResponseDTO(at.getId(), at.getName(), at.getTargetEntityType().name(),
                  serviceTypeService.getServiceTypeById(at.getEntity_id()).getName(), at.getSeverity().name());
            break;
            case DuplicateRecords:
               atResponse = new AnomalyTypesListResponseDTO(at.getId(), at.getName(), at.getTargetEntityType().name(), industryService.getIndustryById(at.getEntity_id()).getName(),
                  at.getSeverity().name());
            break;
            case ExceedingThreshold:
               atResponse = new AnomalyTypesListResponseDTO(at.getId(), at.getName(), at.getTargetEntityType().name(), null, at.getSeverity().name());
            break;
         }
         response.add(atResponse);
      }
      return response;
   }

   @Override
   public void addAnomalyType(String username, AnomalyType anomalyType) throws NotFoundExceptionCustom, InvalidDataException
   {
      if(checkAnomalyTypeUnique(anomalyType))
      {
         User user = userService.getUserByEmail(username);
         if(user == null)
         {
            throw new NotFoundExceptionCustom(ErrorCode.EMAIL_NOT_FOUND);
         }
         anomalyType.setCreatedBy(user.getId());
         boolean allowAdd = true;
         if(allowAdd)
         {
            anomalyType.setCreatedOn(new Date());
            anomalyTypeRepository.save(anomalyType);
         }
         else
         {
            throw new InvalidDataException(ErrorCode.INVALID_ADD_REQUEST);
         }
      }
      else
      {
         throw new InvalidDataException(ErrorCode.ALREADY_EXIST);
      }
   }

   @Override
   public AnomalyType updateAnomalyType(String username, AnomalyType anomalyType) throws InvalidDataException, NotFoundException
   {
      User user = userService.getUserByEmail(username);
      if(user == null)
      {
         throw new NotFoundExceptionCustom(ErrorCode.EMAIL_NOT_FOUND);
      }
      AnomalyType originalAnomalyType = getAnomalyTypeById(anomalyType.getId());
      if(originalAnomalyType == null)
      {
         throw new NotFoundException();
      }
      else
      {
         if(validateAnomalyTypeUpdate(originalAnomalyType, anomalyType))
         {
            //if no anomalies created allow TargetEntityType change
            List<Anomaly> anomalies = anomalyService.getAnomaliesByAnomalyTypeId(anomalyType.getId());
            if(anomalies.isEmpty())
            {
               originalAnomalyType.setTargetEntityType(anomalyType.getTargetEntityType());
               originalAnomalyType.setEntity_id(anomalyType.getEntity_id());
            }
            else
               if(!anomalies.isEmpty() && anomalyType.getTargetEntityType() != originalAnomalyType.getTargetEntityType())
               {
                  throw new InvalidDataException(ErrorCode.ANOMALIES_ALREADY_REPORTED);
               }
            boolean allowChange = true;
            if(allowChange)
            {
               originalAnomalyType.setName(anomalyType.getName());
               originalAnomalyType.setSeverity(anomalyType.getSeverity());
               return anomalyTypeRepository.save(originalAnomalyType);
            }
            else
            {
               throw new InvalidDataException(ErrorCode.INVALID_CHANGE_REQUEST);
            }
         }
         else
         {
            throw new InvalidDataException(ErrorCode.INVALID_CHANGE_REQUEST);
         }
      }
   }


   private boolean checkAnomalyTypeUnique(AnomalyType anomalyType)
   {
      List<AnomalyType> anomalyTypes = anomalyTypeRepository.checkAnomalyTypeUnique(anomalyType.getName(), anomalyType.getTargetEntityType().getCode(), anomalyType.getEntity_id());
      if(anomalyTypes.isEmpty())
      {
         return true;
      }
      else
      {
         return false;
      }
   }

   private boolean validateAnomalyTypeUpdate(AnomalyType originalAnomalyType, AnomalyType anomalyType)
   {
      if(originalAnomalyType.getName().equals(anomalyType.getName()) && originalAnomalyType.getTargetEntityType() == anomalyType.getTargetEntityType() && originalAnomalyType
         .getEntity_id() == anomalyType.getEntity_id())
      {
         return true;
      }
      List<AnomalyType> anomalyTypes = anomalyTypeRepository.checkAnomalyTypeUnique(anomalyType.getName(), anomalyType.getTargetEntityType().getCode(), anomalyType.getEntity_id());
      if(anomalyTypes.isEmpty())
      {
         return true;
      }
      else
      {
         return false;
      }
   }

}
