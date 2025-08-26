package com.app.kyc.web.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.app.kyc.request.DashboardRequestDTO;
import com.app.kyc.response.DashboardResponseDTO;
import com.app.kyc.service.DashboardService;
import com.app.kyc.web.security.SecurityHelper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/dashboard")
public class DashboardController
{

   private static final Logger log = LoggerFactory.getLogger(DashboardController.class);
   @Autowired
   DashboardService dashboardService;

   @Autowired
   SecurityHelper securityHelper;

   @GetMapping("/get")
   public ResponseEntity<?> getDashboard(HttpServletRequest request, @RequestParam("dashboardRequestDTO") String dashboardRequestDTO)
   {
      //log.info("DashboardController/getDashboard");      
      try
      {
         List<String> roles = new ArrayList<String>();
         roles.add("Compliance Admin");
         if(securityHelper.hasRole(request, roles))
         {
            ObjectMapper mapper = new ObjectMapper();
            DashboardRequestDTO obj = mapper.readValue(dashboardRequestDTO, DashboardRequestDTO.class);
//            DashboardResponseDTO dashboardResponse = dashboardService.getDashboard(obj);
            ResponseEntity<?> response = ResponseEntity.ok("moved to v2, please use /get-v2");
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

	@GetMapping("/get-v2")
	public ResponseEntity<?> getDashboardV2(HttpServletRequest request, @RequestParam("filter") String filter)
			throws JsonProcessingException, NullPointerException {
		try {
			List<String> roles = new ArrayList<String>();
			roles.add("Compliance Admin");
		    roles.add("SP Admin");
		    roles.add("KYC Admin");
		    roles.add("SP User");
			if (securityHelper.hasRole(request, roles)) {
				ObjectMapper mapper = new ObjectMapper();
				DashboardRequestDTO dashboardRequestDTO = mapper.readValue(filter, DashboardRequestDTO.class);
				DashboardResponseDTO dashboardResponse = dashboardService.getDashboardV2(dashboardRequestDTO);
				return ResponseEntity.ok(dashboardResponse);
			} else {

				return ResponseEntity.ok("Not authorized");
			}
		} catch (Exception e) {
			// log.info(e.getMessage());
			return ResponseEntity.ok(e.getMessage());
		}
	}

      @GetMapping("/get-timeseries")
      public ResponseEntity<?> getAnomalyTimeSeries(HttpServletRequest request, @RequestParam("filter") String filter) throws JsonProcessingException, NullPointerException {
            if(securityHelper.hasRole(request, Collections.singletonList("Compliance Admin")))
            {
               ObjectMapper mapper = new ObjectMapper();
               DashboardRequestDTO dashboardRequestDTO = mapper.readValue(filter, DashboardRequestDTO.class);
               DashboardResponseDTO dashboardResponse = dashboardService.getAnomalyTimeSeries(dashboardRequestDTO);
               return ResponseEntity.ok(dashboardResponse);
            }
   
               return ResponseEntity.ok("Not authorized");
         }

//   @GetMapping("/get_header_lists")
//   public ResponseEntity<?> getHeaderLists(HttpServletRequest request)
//   {
//      //log.info("DashboardController/getDashboard");
//      try
//      {
//         List<String> roles = new ArrayList<String>();
//         roles.add("Compliance Admin");
//         if(securityHelper.hasRole(request, roles))
//         {
//            ObjectMapper mapper = new ObjectMapper();
//            DashboardRequestDTO obj = new DashboardRequestDTO();
//            DahsboardHeaderListsResponse dashboardResponse = dashboardService.getHeaderLists();
//            ResponseEntity<?> response = ResponseEntity.ok(dashboardResponse);
//            return response;
//         }
//         else
//            return ResponseEntity.ok("Not authorized");
//      }
//      catch(Exception e)
//      {
//         //log.info(e.getMessage());
//         return ResponseEntity.ok(e.getMessage());
//      }
//   }

}
