package com.app.kyc.service;

import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.repository.ConsumerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    private static final int BATCH_SIZE = 2000;  // 🧩 Safer for large CSVs (avoid deadlocks)
    private static final int MAX_FIELD_LEN = 255;

    /**
     * Transactional ingestion method — called from FileProcessingService.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public int ingestFileTxOrange(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        int total = 0;

        Map<String, AtomicInteger> vendorCounters = new HashMap<>();
        SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy");

        List<Consumer> batch = new ArrayList<>();

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;

            while ((row = csv.readNext()) != null) {
                if (isHeader) {
                    isHeader = false;
                    log.info("Header column count: {}", row.length);
                    continue;
                }

                if (row.length == 0) continue;

                try {
                    Consumer consumer = mapRowToConsumer(row, spId, nowTs);
                    consumerServiceImpl.updateConsistencyFlag(consumer);
                    batch.add(consumer);

                    if (batch.size() >= BATCH_SIZE) {
                        flushAndCommitBatch(batch);
                        total += BATCH_SIZE;
                    }
                } catch (DataIntegrityViolationException e) {
                    log.warn("⚠️ Skipping row due to data error: {}", e.getMostSpecificCause().getMessage());
                } catch (Exception e) {
                    log.warn("⚠️ Failed to map row (skipped): {}", Arrays.toString(row));
                }
            }

            if (!batch.isEmpty()) {
                flushAndCommitBatch(batch);
                total += batch.size();
            }

        } catch (Exception e) {
            log.error("❌ Ingest failed in OrangeIngestionService: {}", e.getMessage(), e);
            throw e;  // rollback the transaction
        }

        log.info("✅ Orange ingestion completed: {} records", total);
        return total;
    }

    /**
     * Flushes a batch to DB and clears persistence context.
     */
    private void flushAndCommitBatch(List<Consumer> batch) {
        try {
            consumerRepository.saveAll(batch);
            entityManager.flush();
            entityManager.clear();
            batch.clear();
        } catch (Exception e) {
            log.error("❌ Error during batch flush: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Safely maps CSV row → Consumer entity with clipping and validation.
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
        c.setIsConsistent(Boolean.FALSE);
        c.setConsistentOn(LocalDate.now().toString());
        return c;
    }

    /**
     * Utility: clip & null-safe field value to avoid SQL truncation.
     */
    private String safeClip(String[] row, int idx, int maxLen) {
        if (row == null || idx >= row.length) return null;
        String val = row[idx];
        if (val == null) return null;
        val = val.trim();
        if (val.length() > maxLen) {
            log.warn("Truncating value '{}' to {} chars", val, maxLen);
            return val.substring(0, maxLen);
        }
        return val;
    }
}
