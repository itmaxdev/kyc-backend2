package com.app.kyc.service;

import com.app.kyc.entity.Consumer;
import com.app.kyc.repository.ConsumerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerBatchService {

    private final ConsumerRepository consumerRepository;

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Persists and flushes a batch in a new transaction.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public int flushAndCommitBatch(List<Consumer> batch) {
        try {
            if (batch == null || batch.isEmpty()) return 0;

            consumerRepository.saveAll(batch);
            entityManager.flush();
            entityManager.clear();

            int size = batch.size();
            batch.clear();
            log.info("✅ Batch committed successfully with {} records", size);
            return size;

        } catch (Exception e) {
            log.error("🚫 Batch save failed ({} records): {}", batch.size(), e.getMessage());
            entityManager.clear();
            batch.clear();
            return 0;
        }
    }
}
