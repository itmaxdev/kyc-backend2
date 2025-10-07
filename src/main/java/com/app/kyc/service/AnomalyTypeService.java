package com.app.kyc.service;

import java.util.List;
import java.util.Map;

import com.app.kyc.model.AnomalyTypeDto;
import com.app.kyc.model.DashboardObjectInterface;
import org.springframework.data.crossstore.ChangeSetPersister.NotFoundException;

import com.app.kyc.entity.AnomalyType;
import com.app.kyc.response.AnomalyTypesListResponseDTO;
import com.app.kyc.service.exception.InvalidDataException;
import com.app.kyc.service.exception.NotFoundExceptionCustom;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public interface AnomalyTypeService
{

   public AnomalyType getAnomalyTypeById(Long id);

   public Map<String, Object> getAllAnomalyTypes(String params) throws JsonMappingException, JsonProcessingException;

   public List<AnomalyTypesListResponseDTO> getAllAnomalyTypesByUser(String username) throws NotFoundExceptionCustom;

   public void addAnomalyType(String username, AnomalyType document) throws NotFoundExceptionCustom, InvalidDataException;

   public AnomalyType updateAnomalyType(String username, AnomalyType document) throws NotFoundExceptionCustom, InvalidDataException, NotFoundException;

  public  List<AnomalyType> findAll();

   List<DashboardObjectInterface> getAnomalyTypeCounts();

    public List<AnomalyTypeDto> getAnomalyTypes();

    public void softDeleteAnomalyType(Long id);

    public List<AnomalyType> getAllActiveAnomalyTypes();

    AnomalyType createAnomalyType(AnomalyType anomalyType);
}
