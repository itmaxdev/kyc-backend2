package com.app.kyc.service;

import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.repository.ConsumerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrangeIngestionService {

    private final ConsumerRepository consumerRepository;
    private final ConsumerServiceImpl consumerServiceImpl;

    @PersistenceContext
    private EntityManager entityManager;

    private static final int BATCH_SIZE = 2000;
    private static final int MAX_RETRIES = 3;

    /**
     * Transactional Orange ingestion – generates unique orange_transaction_id for deduplication.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public int ingestFileTxOrange(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        int total = 0;

        List<Consumer> batch = new ArrayList<>(BATCH_SIZE);
        SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy");
        Map<String, AtomicInteger> vendorCounters = new HashMap<>();

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;

            while ((row = csv.readNext()) != null) {
                if (isHeader) { isHeader = false; log.info("Header column count: {}", row.length); continue; }
                if (row.length == 0) continue;

                try {
                    Consumer consumer = mapRowToConsumer(row, spId, nowTs);

                    // ✅ Compute deterministic transaction ID hash
                    String txHash = DigestUtils.sha256Hex(String.join("|",
                            Optional.ofNullable(consumer.getMsisdn()).orElse(""),
                            Optional.ofNullable(consumer.getFirstName()).orElse(""),
                            Optional.ofNullable(consumer.getLastName()).orElse(""),
                            Optional.ofNullable(consumer.getBirthDate()).orElse(""),
                            Optional.ofNullable(consumer.getIdentificationNumber()).orElse(""),
                            Optional.ofNullable(consumer.getIdentificationType()).orElse(""),
                            String.valueOf(spId)
                    ));

                    consumer.setOrangeTransactionId(txHash);

                    // 🔹 Check if consumer already exists by transaction hash
                    consumerRepository.findByOrangeTransactionId(txHash).ifPresent(existing -> {
                        consumer.setId(existing.getId()); // overwrite same record instead of new insert
                    });

                    // 🔹 Apply consistency logic
                    consumerServiceImpl.updateConsistencyFlag(consumer);

                    // 🔹 Assign vendor code (Orange-<date>-seq)
                    String date = df.format(nowTs);
                    String key = "Orange-" + date;
                    vendorCounters.putIfAbsent(key, new AtomicInteger(1));
                    int seq = vendorCounters.get(key).getAndIncrement();
                    consumer.setVendorCode("Orange-" + date + "-" + seq);

                    batch.add(consumer);

                    if (batch.size() >= BATCH_SIZE) {
                        total += flushAndCommitBatch(batch);
                    }
                } catch (DataIntegrityViolationException e) {
                    log.warn("⚠️ Skipping row due to data violation: {}", e.getMostSpecificCause().getMessage());
                } catch (Exception e) {
                    log.warn("⚠️ Failed to map row (skipped): {}", Arrays.toString(row));
                }
            }

            if (!batch.isEmpty()) {
                total += flushAndCommitBatch(batch);
            }

        } catch (Exception e) {
            log.error("❌ Orange ingestion failed: {}", e.getMessage(), e);
            throw e;
        }

        log.info("✅ Orange ingestion completed: {} records processed.", total);
        return total;
    }

    /**
     * Flushes and clears persistence context with retry (to avoid deadlocks).
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public int flushAndCommitBatch(List<Consumer> batch) throws InterruptedException {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                consumerRepository.saveAll(batch);
                entityManager.flush();
                entityManager.clear();
                int size = batch.size();
                batch.clear();
                return size;
            } catch (Exception e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    log.error("❌ Batch save failed after {} retries: {}", MAX_RETRIES, e.getMessage());
                    throw e;
                }
                log.warn("⚠️ Deadlock or lock timeout, retrying batch ({}/{}): {}", attempt, MAX_RETRIES, e.getMessage());
                Thread.sleep(500L * attempt);
            }
        }
        batch.clear();
        return 0;
    }

    /**
     * Maps CSV row → Consumer safely.
     */
    private Consumer mapRowToConsumer(String[] row, Long spId, Timestamp nowTs) {
        Consumer c = new Consumer();
        c.setFirstName(safeClip(row, 0, 100));
        c.setLastName(safeClip(row, 1, 100));
        c.setMsisdn(safeClip(row, 2, 45));
        c.setGender(safeClip(row, 3, 10));
        c.setBirthDate(safeClip(row, 4, 45));
        c.setBirthPlace(safeClip(row, 5, 45));
        c.setAddress(safeClip(row, 6, 255));
        c.setRegistrationDate(safeClip(row, 7, 50));
        c.setIdentificationNumber(safeClip(row, 8, 45));
        c.setIdentificationType(safeClip(row, 9, 45));

        ServiceProvider spRef = new ServiceProvider();
        spRef.setId(spId);
        c.setServiceProvider(spRef);
        c.setCreatedOn(nowTs.toString());
        return c;
    }

    private String safeClip(String[] row, int idx, int maxLen) {
        if (row == null || idx >= row.length) return null;
        String val = row[idx];
        if (val == null) return null;
        val = val.trim();
        return (val.length() > maxLen) ? val.substring(0, maxLen) : val;
    }
}
