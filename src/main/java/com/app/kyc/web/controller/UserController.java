package com.app.kyc.web.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.app.kyc.model.OtpRequest;
import com.app.kyc.model.VerifyEmailChangePasswordDTO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.app.kyc.Masking.MaskingContext;
import com.app.kyc.entity.Otp;
import com.app.kyc.entity.User;
import com.app.kyc.enums.Channel;
import com.app.kyc.enums.ErrorKeys;
import com.app.kyc.enums.Lang;
import com.app.kyc.enums.OtpPurpose;
import com.app.kyc.exception.CustomNotFoundException;
import com.app.kyc.request.ChangePasswordRequestDTO;
import com.app.kyc.request.ResetPasswordRequestDTO;
import com.app.kyc.service.EmailService;
import com.app.kyc.service.UserService;
import com.app.kyc.service.UserServiceImpl;
import com.app.kyc.service.common.ErrorCode;
import com.app.kyc.service.exception.InvalidDataException;
import com.app.kyc.web.security.AuthRequest;
import com.app.kyc.web.security.AuthResponse;
import com.app.kyc.web.security.SecurityHelper;
import com.fasterxml.jackson.core.JsonProcessingException;

@RestController
@CrossOrigin
@RequestMapping("/users")
public class UserController
{
	private static final Logger log = LoggerFactory.getLogger(UserController.class);

   @Autowired
   UserService userService;

   @Autowired
   SecurityHelper securityHelper;
   
   @Autowired
   EmailService emailService;
   
   @PutMapping("/sendOTP")
   public ResponseEntity<?> generateOtp(HttpServletResponse response, @RequestParam(required = true) Channel channel,
	        @RequestParam(required = true) OtpPurpose purpose,
	        @RequestParam(required = true) Lang lang,
	        @RequestParam(required = true) String email) throws Exception
   {
		try {
			OtpRequest otpRequest = new OtpRequest();
		    otpRequest.setChannel(channel);
		    otpRequest.setPurpose(purpose);
		    otpRequest.setLang(lang);
		    otpRequest.setEmail(email);
			userService.generateOtp(otpRequest);
			return ResponseEntity.ok("OTP sent successfully");
		} catch (CustomNotFoundException e) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Something went wrong");
		}
   }

   @PatchMapping("/authenticate")
   public ResponseEntity<?> authenticateUser(HttpServletResponse response, @RequestBody AuthRequest authRequest) throws Exception
   {
      //log.info("UserController/authenticateUser");
	   AuthResponse authResponse = userService.authenticateUser(authRequest);
	   return ResponseEntity.ok(authResponse);

   }

   @GetMapping("/{id}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> getUserById(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("UserController/getUserById");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         roles.add("Compliance Admin");
         roles.add("SP User");
         if(securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(userService.getUserById(id));
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getByEmail/{email}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> getUserByEmail(HttpServletRequest request, @PathVariable("email") String email) throws SQLException
   {
      //log.info("UserController/getUserByEmail");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         if(securityHelper.hasRole(request, roles))
            return ResponseEntity.ok(userService.getUserByEmail(email));
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/getAll/")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> getAllUsersWithCount(HttpServletRequest request, @RequestParam(value = "params", required = false) String params)
      throws SQLException, JsonProcessingException
   {
      //log.info("UserController/getAllUsersWithCount");      
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> users = userService.getAllUsers(params);
            ResponseEntity<?> response = ResponseEntity.ok(users);
            return response;
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }
   
   @GetMapping("/getAllByCreatedById/{createdById}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> getAllUsersWithCount(HttpServletRequest request, @RequestParam(value = "params", required = false) String params,
      @PathVariable("createdById") Long createdById)
      throws SQLException, JsonProcessingException
   {
      //log.info("UserController/getAllUsersWithCount");      
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         if(securityHelper.hasRole(request, roles))
         {
            Map<String, Object> users = userService.getAllByCreatedById(params, createdById);
            ResponseEntity<?> response = ResponseEntity.ok(users);
            return response;
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }
   
   @PostMapping("/add")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> addUser(HttpServletRequest request, @ModelAttribute User user, @RequestParam("governmentIdFile") MultipartFile governmentIdFile)
      throws InvalidDataException
   {
      List<String> roles = new ArrayList<String>();
      roles.add("KYC Admin");
      roles.add("SP Admin");
      if(securityHelper.hasRole(request, roles))
      {
         String authTokenHeader = request.getHeader("Authorization");
         String jwt = "";
         if(authTokenHeader != null && authTokenHeader.startsWith(("Bearer ")))
         {
            jwt = authTokenHeader.substring(7);
         }
         User currentUser = userService.getUserByEmail(securityHelper.getUserName(jwt));
         userService.addUser(user, governmentIdFile, currentUser.getId());
         return new ResponseEntity<>(HttpStatus.OK);
      }
      else
         return ResponseEntity.ok("Not authorized");
   }

   @PutMapping("/update")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> updateUser(HttpServletRequest request, @RequestBody User user) throws SQLException
   {
      //log.info("UserController/updateUser");
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         if(securityHelper.hasRole(request, roles))
         {
            userService.updateUser(user);
            return ResponseEntity.ok(user);
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/verifyEmailToChangePassword/{email}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> verifyEmailToChangePassword(HttpServletRequest request, @PathVariable("email") String email) throws SQLException
   {
      //log.info("UserController/getUserByEmail");
      try
      {
         userService.verifyEmailToChangePassword(email);
         return new ResponseEntity<>(HttpStatus.OK);
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   //TODO:: Remove DTO
   @PutMapping("/verifyEmailToChangePassword-v2")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> verifyEmailToChangePasswordV2(HttpServletRequest request, @RequestBody VerifyEmailChangePasswordDTO verifyEmailChangePasswordDTO) throws SQLException
   {
      //log.info("UserController/getUserByEmail");
      String emailRequestBody = verifyEmailChangePasswordDTO.getEmail();

      try
      {
         userService.verifyEmailToChangePassword(emailRequestBody);
         return new ResponseEntity<>(HttpStatus.OK);
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PatchMapping("/changePassword")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> changePassword(HttpServletRequest request, @RequestBody ChangePasswordRequestDTO changePasswordRequestDTO) throws SQLException
   {
      //log.info("UserController/changePassword");
      try
      {
         User user = userService.changePassword(changePasswordRequestDTO);
         return ResponseEntity.ok(user);
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PatchMapping("/resetPassword")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> resetPassword(HttpServletRequest request, @RequestBody ResetPasswordRequestDTO resetPasswordRequestDTO) throws SQLException
   {
      //log.info("UserController/changePassword");
      try
      {
         User user = userService.resetPassword(resetPasswordRequestDTO);
         return ResponseEntity.ok(user);
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PatchMapping("/deactivateUser/{id}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> deactivateUser(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("UserController/deactivateUser");
      try
      {
         User user = userService.getUserById(id);
         if(user.getId() == null) return ResponseEntity.ok("User not found");
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         if(securityHelper.hasRole(request, roles))
         {
            userService.deactivateUser(id);
            return ResponseEntity.ok("User Deactivated");
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PatchMapping("/activateUser/{id}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> activateUser(HttpServletRequest request, @PathVariable("id") Long id) throws SQLException
   {
      //log.info("UserController/activateUser");
      try
      {
         User user = userService.getUserById(id);
         if(user.getId() == null) return ResponseEntity.ok("User not found");
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         if(securityHelper.hasRole(request, roles))
         {
            userService.activateUser(id);
            return ResponseEntity.ok("User Activated");
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @DeleteMapping("/deleteUser/{id}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> deleteUser(HttpServletRequest request, @PathVariable("id") long id) throws SQLException
   {
      //log.info("UserController/changePassword");
      try
      {
         User user = userService.getUserById(id);
         if(user.getId() == null) return ResponseEntity.ok("User not found");
         List<String> roles = new ArrayList<String>();
         roles.add("KYC Admin");
         roles.add("SP Admin");
         if(securityHelper.hasRole(request, roles))
         {
            userService.deleteUser(id);
            return ResponseEntity.ok("User Deleted");
         }
         else
            return ResponseEntity.ok("Not authorized");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @GetMapping("/verifyUser/{email}/{code}")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> verifyUser(HttpServletRequest request, @PathVariable("email") String email, @PathVariable("code") Long code) throws SQLException
   {
      //log.info("UserController/verifyUser");
      try
      {
         return ResponseEntity.ok(userService.verifyUser(email, code));
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }

   @PatchMapping("/setupUser")
   //@PreAuthorize("hasAuthority('KYC Admin')")
   public ResponseEntity<?> setupUser(HttpServletRequest request, @RequestBody ResetPasswordRequestDTO resetPasswordRequestDTO) throws SQLException
   {
      //log.info("UserController/changePassword");
      try
      {
         userService.setupUserPassword(resetPasswordRequestDTO);
         return ResponseEntity.ok("Password set up");
      }
      catch(Exception e)
      {
         //log.info(e.getMessage());
         return ResponseEntity.ok(e.getMessage());
      }
   }
   
   @PutMapping("/unMaskData")
   public ResponseEntity<?> unMaskData(HttpServletResponse response, 
		   	@RequestParam(required = true) Channel channel,
	        @RequestParam(required = true) OtpPurpose purpose,
	        @RequestParam(required = true) Lang lang,
	        @RequestParam(required = true) Long userId) throws Exception
   {
	   	OtpRequest otpRequest = new OtpRequest();
	    otpRequest.setChannel(channel);
	    otpRequest.setPurpose(purpose);
	    otpRequest.setLang(lang);
	    otpRequest.setUserId(userId);
		try {
			userService.generateOtp(otpRequest);
			return ResponseEntity.ok("OTP sent successfully");
		} catch (CustomNotFoundException e) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Something went wrong");
		}
   }
   
	@PostMapping("/verifyUnmaskOTP")
	public ResponseEntity<?> verifyOtp(@RequestParam Long userId, @RequestParam(required = true) Channel channel,
			@RequestParam String otp) throws InvalidDataException {
		User user = userService.getUserById(userId); // fetch user from DB
		if(user == null) {
			 throw new CustomNotFoundException(ErrorKeys.USER_NOT_FOUND.getValue());
		}

		boolean verified = userService.validateOtp(user.getEmail(), otp, channel, OtpPurpose.UNMASK);
		if (!verified) {
			throw new InvalidDataException(ErrorCode.OTP_INVALID.getMessage());
		}

		userService.activateUnmask(user);
		
		return ResponseEntity.ok("System unmasked for 2 minutes");
	}
}
