package com.app.kyc.service;

import java.util.List;
import java.util.Map;

import org.springframework.data.crossstore.ChangeSetPersister.NotFoundException;
import org.springframework.web.multipart.MultipartFile;

import com.app.kyc.entity.Otp;
import com.app.kyc.entity.Role;
import com.app.kyc.entity.User;
import com.app.kyc.model.OtpRequest;
import com.app.kyc.request.ChangePasswordRequestDTO;
import com.app.kyc.request.ResetPasswordRequestDTO;
import com.app.kyc.service.exception.InvalidDataException;
import com.app.kyc.web.security.AuthRequest;
import com.app.kyc.web.security.AuthResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public interface UserService
{

   User getUserById(Long id);

   User getUserByEmail(String email);

   void verifyEmailToChangePassword(String email);

   Map<String, Object> getAllUsers(String params) throws JsonMappingException, JsonProcessingException;

   int getTotalUsers();

   void addUser(User user, MultipartFile file, Long createdBy) throws InvalidDataException;

   User updateUser(User user) throws NotFoundException;

   void deleteUser(Long id);

   User updateLastLogin(String email);

   User verifyUser(String email, Long code);

   User deactivateUser(Long id);

   User activateUser(Long id);

   Role getUserRoleById(Long id);

   AuthResponse authenticateUser(AuthRequest authRequest) throws InvalidDataException;

   User changePassword(ChangePasswordRequestDTO changePasswordRequestDTO) throws InvalidDataException;

   User resetPassword(ResetPasswordRequestDTO resetPasswordRequestDTO) throws InvalidDataException, NotFoundException;

   void setupUserPassword(ResetPasswordRequestDTO resetPasswordRequestDTO) throws InvalidDataException, NotFoundException;

   Map<String, Object> getAllByCreatedById(String params, Long createdById) throws JsonMappingException, JsonProcessingException;

   List<User> getByServiceProviderId(Long serviceProviderId);
   
   public void generateOtp(OtpRequest otpRequest);
}
