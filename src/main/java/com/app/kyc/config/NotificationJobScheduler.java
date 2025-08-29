package com.app.kyc.config;

import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.app.kyc.entity.Anomaly;
import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.NotificationJob;
import com.app.kyc.entity.User;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.model.NotificationType;
import com.app.kyc.repository.AnomalyRepository;
import com.app.kyc.repository.ConsumerRepository;
import com.app.kyc.repository.NotificationJobRepository;
import com.app.kyc.service.NotificationService;
import com.app.kyc.service.UserService;

@Component
public class NotificationJobScheduler {

	private final NotificationJobRepository jobRepo;
	private final AnomalyRepository anomalyRepo;
	private final ConsumerRepository consumerRepo;
	private final UserService userService;
	private final NotificationService notificationService;

	public NotificationJobScheduler(NotificationJobRepository jobRepo, AnomalyRepository anomalyRepo,
			ConsumerRepository consumerRepo, UserService userService, NotificationService notificationService) {
		this.jobRepo = jobRepo;
		this.anomalyRepo = anomalyRepo;
		this.consumerRepo = consumerRepo;
		this.userService = userService;
		this.notificationService = notificationService;
	}

	// run every 5s
	@Scheduled(fixedDelay = 5000)
	public void processJobs() {
		List<NotificationJob> jobs = jobRepo.findByState("PENDING");
		for (NotificationJob job : jobs) {
			try {
				processJob(job);
				job.markCompleted();
			} catch (Exception e) {
				job.markFailed(e.getMessage());
			}
			jobRepo.save(job);
		}
	}

	// This will find anomaly and consumer and send the notification and Email to
	// respective users
	private void processJob(NotificationJob job) {
		Anomaly anomaly = anomalyRepo.findById(job.getAnomalyId())
				.orElseThrow(() -> new RuntimeException("Anomaly not found"));

		List<Consumer> consumers = consumerRepo.getAllByAnomalies(anomaly);
		if (consumers.isEmpty())
			return;

		Consumer consumer = consumers.get(0);
		String message = buildMessage(job.getStatus(), consumer);

		if (shouldNotifySPUsers(job.getStatus())) {
			List<User> spUsers = userService.getByServiceProviderId(consumer.getServiceProvider().getId());
			for (User u : spUsers) {
				notificationService.addNotification(message, u, NotificationType.ANOMALY_REPORTED, anomaly.getId());
			}
		} else {
			notificationService.addNotification(message, anomaly.getReportedBy(), NotificationType.ANOMALY_REPORTED,
					anomaly.getId());
		}
	}

	// Check if status equals to this 3 status
	private boolean shouldNotifySPUsers(AnomalyStatus status) {
		return status.equals(AnomalyStatus.WITHDRAWN) || status.equals(AnomalyStatus.RESOLVED_SUCCESSFULLY)
				|| status.equals(AnomalyStatus.QUESTION_ANSWERED);
	}

	// Prepare a messages based on the status
	private String buildMessage(AnomalyStatus status, Consumer consumer) {

		switch (status) {
		case WITHDRAWN:
			return "The anomaly for " + consumer.getFirstName() + " " + consumer.getLastName() + " has been withdrawn";
		case RESOLVED_SUCCESSFULLY:
			return "The anomaly for " + consumer.getFirstName() + " " + consumer.getLastName() + " has been resolved";
		case QUESTION_ANSWERED:
			return "Question answered for anomaly on " + consumer.getFirstName() + " " + consumer.getLastName();
		case QUESTION_SUBMITTED:
			return consumer.getServiceProvider().getName() + " raised a question for anomaly on "
					+ consumer.getFirstName() + " " + consumer.getLastName();
		case RESOLUTION_SUBMITTED:
			return consumer.getServiceProvider().getName() + " submitted a resolution for anomaly on "
					+ consumer.getFirstName() + " " + consumer.getLastName();
		case UNDER_INVESTIGATION:
			return consumer.getServiceProvider().getName() + " is investigating anomaly on " + consumer.getFirstName()
					+ " " + consumer.getLastName();
		default:
			return "Update on anomaly for " + consumer.getFirstName() + " " + consumer.getLastName();
		}
	}
}
