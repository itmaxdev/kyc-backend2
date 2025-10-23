package com.app.kyc.service;


import com.app.kyc.entity.Consumer;
import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.repository.ConsumerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
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

    private static final int BATCH_SIZE = 5000;  // ✅ safe batch size

    @Transactional(rollbackFor = Exception.class)
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

                Consumer consumer = mapRowToConsumer(row, spId, nowTs);
                consumerServiceImpl.updateConsistencyFlag(consumer);

                batch.add(consumer);

                if (batch.size() >= BATCH_SIZE) {
                    flushAndCommitBatch(batch);
                    total += BATCH_SIZE;
                }
            }

            if (!batch.isEmpty()) {
                flushAndCommitBatch(batch);
                total += batch.size();
            }
        }

        return total;
    }

    private void flushAndCommitBatch(List<Consumer> batch) {
        consumerRepository.saveAll(batch);
        entityManager.flush();
        entityManager.clear();
        batch.clear();
    }

    private Consumer mapRowToConsumer(String[] row, Long spId, Timestamp nowTs) {
        Consumer c = new Consumer();
        c.setFirstName(row[0]);
        c.setLastName(row[1]);
        c.setMsisdn(row[2]);
        c.setGender(row[3]);
        c.setCreatedOn(nowTs.toString());
        ServiceProvider spRef = new ServiceProvider();
        spRef.setId(spId);
        c.setServiceProvider(spRef);
        c.setIsConsistent(Boolean.FALSE);
        c.setConsistentOn(LocalDate.now().toString());
        return c;
    }
}
