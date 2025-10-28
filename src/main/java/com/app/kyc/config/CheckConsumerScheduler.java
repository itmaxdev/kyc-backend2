package com.app.kyc.config;



import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.entity.User;
import com.app.kyc.repository.ConsumerRepository;
import com.app.kyc.repository.ServiceProviderRepository;
import com.app.kyc.service.ConsumerServiceImpl;
import com.app.kyc.service.UserService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
public class CheckConsumerScheduler {
    private static final Logger log = LoggerFactory.getLogger(CheckConsumerScheduler.class);

    private final ConsumerServiceImpl consumerServiceImpl;
    private final UserService userService;
    private final ServiceProviderRepository serviceProviderRepository;
    private final ConsumerRepository consumerRepository;

    // per-operator reentrancy guard (prevents overlapping runs)
    private final Map<Long, AtomicBoolean> running = new ConcurrentHashMap<>();

    // every 10 minutes (tune via property if you like)
    // @Scheduled(cron = "${kyc.check-consumer.cron:0 */10 * * * *}")
    //@Scheduled(cron = "0 */1 * * * *")
    public void run() {
        User user = userService.getUserByEmail("cadmin@itmaxglobal.com");

        for (ServiceProvider sp : serviceProviderRepository.findAll()) {
            running.computeIfAbsent(sp.getId(), id -> new AtomicBoolean(false));
            AtomicBoolean flag = running.get(sp.getId());

            if (!flag.compareAndSet(false, true)) {
                log.info("checkConsumer already running for operator={} (id={}), skipping", sp.getName(), sp.getId());
                continue;
            }

            long t0 = System.currentTimeMillis();
            try {
                log.info("Starting scheduled checkConsumer for operator={} (id={})", sp.getName(), sp.getId());

                int page = 0;
                int size = 2000; // adjust batch size for your DB box
                long total = 0;

                while (true) {
                    Page<Consumer> batch = consumerRepository.findByServiceProvider_Id(sp.getId(), PageRequest.of(page, size));
                    if (batch.isEmpty()) break;

                    consumerServiceImpl.checkConsumer(batch.getContent(), user, sp);
                    total += batch.getNumberOfElements();
                    page++;
                }

                log.info("Finished checkConsumer for operator={} (id={}) | checked {} consumers in {} ms",
                        sp.getName(), sp.getId(), total, (System.currentTimeMillis() - t0));

            } catch (Exception ex) {
                log.error("checkConsumer failed for operator={} (id={}): {}", sp.getName(), sp.getId(), ex.toString(), ex);
            } finally {
                flag.set(false);
            }
        }
    }
}
