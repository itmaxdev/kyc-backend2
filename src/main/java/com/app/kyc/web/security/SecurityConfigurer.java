package com.app.kyc.web.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import com.app.kyc.service.AppUserDetailsService;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, jsr250Enabled = true)
public class SecurityConfigurer extends WebSecurityConfigurerAdapter
{

   @Autowired
   private AppUserDetailsService userDetailsService;

   @Autowired
   JWTRequestFilter jwtRequestFilter;

   @Override
   protected void configure(AuthenticationManagerBuilder authenticationManagerBuilder) throws Exception
   {
      authenticationManagerBuilder.userDetailsService(userDetailsService);
   }

   @Override
   protected void configure(HttpSecurity httpSecurity) throws Exception
   {
      try
      {
         httpSecurity.csrf().disable().authorizeRequests()
            .antMatchers("/users/sendOTP","/users/authenticate", "/users/verifyEmailToChangePassword/**", "/users/resetPassword", "/users/setupUser", "/swagger-ui/**", "/swagger-ui/",
               "/swagger-resources/**", "/v2/**", "/webjars/**", "/actuator/**", "/metrics/**")
            .permitAll().anyRequest().authenticated().and().sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);
         httpSecurity.cors();
         httpSecurity.addFilterBefore(jwtRequestFilter, UsernamePasswordAuthenticationFilter.class);
      }
      catch(Exception e)
      {
         //            log.info(e.getMessage());
      }
   }

   @Override
   public void configure(WebSecurity web)
   {
      web.ignoring().antMatchers("/swagger-resources/", "/webjars/").antMatchers(HttpMethod.OPTIONS, "/**");
   }

   @Override
   @Bean
   public AuthenticationManager authenticationManagerBean() throws Exception
   {
      try
      {
         return super.authenticationManagerBean();
      }
      catch(Exception e)
      {
         //            log.info(e.getMessage());
         return null;
      }
   }

   @Bean
   public PasswordEncoder passwordEncoder()
   {
      try
      {
         return new BCryptPasswordEncoder();
      }
      catch(Exception e)
      {
         //            log.info(e.getMessage());
         return null;
      }
   }

}
