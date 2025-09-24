package com.app.kyc.service;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import com.app.kyc.enums.OtpPurpose;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class EmailService {

	private static final Logger log = LoggerFactory.getLogger(EmailService.class);
	 private final JavaMailSender javaMailSender;
	 private final TemplateEngine templateEngine;

//    @Value("${users.email-images}")
//    private String emailImages;

    @Async
    public void sendOtpEmail(String[] to, String otp, String lang, String clientName, OtpPurpose otpPurpose) {
        try {
            log.info("Sending OTP email to: [{}]", Arrays.toString(to));

            // Set email content using Thymeleaf template
            Context context = new Context();
            context.setVariable("otp", otp);
            context.setVariable("lang", lang);
            context.setVariable("clientName", clientName);

			String title = "";
			if (otpPurpose.equals(OtpPurpose.LOGIN)) {
				title = lang.equalsIgnoreCase("fr") ? "Votre code OTP pour activer votre compte"
						: "Your OTP Code to Activate Your Account";
			} else if (otpPurpose.equals(OtpPurpose.UNMASK)) {
				title = lang.equalsIgnoreCase("fr") ? "Votre OTP pour démasquer vos données"
						: "Your OTP to Unmask Data";
			}

            String htmlContent = "";
            if(otpPurpose.equals(OtpPurpose.LOGIN)) {
            	htmlContent = templateEngine.process("email/otp-email", context);
            }else if(otpPurpose.equals(OtpPurpose.UNMASK)) {
            	htmlContent = templateEngine.process("email/unMask-email", context);
            }
            
            CompletableFuture<MimeMessage> message = addInlineLogos(htmlContent, to, title);
            javaMailSender.send(message.get());
            log.info("OTP email sent successfully to: [{}]", Arrays.toString(to));
        } catch (Exception e) {
            log.info("Failed to send OTP email to: [{}]", Arrays.toString(to));
            log.error("Failed to send OTP email to: [{}]", Arrays.toString(to), e);
        }
    }
    
    @Async
    protected CompletableFuture<MimeMessage> addInlineLogos(String htmlContent, String[] to, String subject) throws MessagingException, javax.mail.MessagingException, UnsupportedEncodingException {
        javax.mail.internet.MimeMessage message = javaMailSender.createMimeMessage();
        
        // true = multipart
        MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

        // Set sender and reply-to
        helper.setFrom("KYC-dev@itmaxglobal.com", "NoReply-JD");
        helper.setReplyTo("KYC-dev@itmaxglobal.com");

        //Set HTML content
        helper.setText(htmlContent, true);

        // Add inline image
//        FileSystemResource guiLogo = new FileSystemResource(new File(emailImages + "/gui-logo.png"));
//        FileSystemResource arptcLogo = new FileSystemResource(new File(emailImages + "/arptc-logo.png"));
//        FileSystemResource linkedinLogo = new FileSystemResource(new File(emailImages + "/linkedin-logo.png"));
//        FileSystemResource fbLogo = new FileSystemResource(new File(emailImages + "/fb-logo.png"));
//        FileSystemResource instaLogo = new FileSystemResource(new File(emailImages + "/insta-logo.png"));
//        FileSystemResource twitterLogo = new FileSystemResource(new File(emailImages + "/twitter-logo.png"));

//        helper.addInline("gui-logo", guiLogo);
//        helper.addInline("arptc-logo", arptcLogo);
//        helper.addInline("linkedin-logo", linkedinLogo);
//        helper.addInline("fb-logo", fbLogo);
//        helper.addInline("insta-logo", instaLogo);
//        helper.addInline("twitter-logo", twitterLogo);

        helper.setTo(to);
        helper.setSubject(subject);

        return CompletableFuture.completedFuture(message);
    }
}
 