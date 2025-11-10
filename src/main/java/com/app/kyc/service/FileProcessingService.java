package com.app.kyc.service;
import com.app.kyc.entity.*;
import com.app.kyc.model.AnomalyStatus;
import com.app.kyc.repository.*;
import org.apache.commons.codec.digest.DigestUtils;
import com.app.kyc.config.ConsumerSpecifications;
import lombok.RequiredArgsConstructor;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;

import javax.persistence.EntityManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;


@Service
@RequiredArgsConstructor
public class FileProcessingService {

    private static final Logger log = LoggerFactory.getLogger(FileProcessingService.class);

    private final ProcessedFileRepository processedFileRepository;
    private final ServiceProviderRepository serviceProviderRepository;
    private final ConsumerRepository consumerRepository;
    private final ConsumerAnomalyRepository consumerAnomalyRepository;
    private final ConsumerServiceImpl consumerServiceImpl;
    private final UserService userService;
    private final JdbcTemplate jdbcTemplate;
    private final OrangeIngestionService orangeIngestionService;

    private final AnomalyRepository anomalyRepository;
    private final AnomalyTrackingRepository anomalyTrackingRepository;

    // Optional: will be autowired by Spring Boot if present; we fall back to new Thread otherwise.
    @Nullable private final TaskExecutor taskExecutor;

    @Value("${ingest.batchSize:1000}")
    private int BATCH_SIZE;

    @Value("${local.file.workDir:}")
    private String configuredWorkDir;

    private static final int IO_RETRIES = 12;
    private static final long IO_SLEEP_MS = 600;

    // Guard to avoid overlapping checkConsumer per-operator
    private static final Set<Long> RUNNING_CHECKS = ConcurrentHashMap.newKeySet();

    // === Helpers for MSISDN & clipping ===
    private static final int E164_MAX = 15;

    private static String digitsOnly(String raw) {
        return raw == null ? null : raw.replaceAll("\\D", "");
    }

    /** Allow empty/invalid MSISDN: return NULL instead of throwing/skip. */
    private static String normalizeMsisdnAllowNull(String raw) {
        String s = digitsOnly(raw);
        if (s == null || s.isEmpty()) return null;
        if (s.length() > E164_MAX) return null;
        return s;
    }

    /** Prevent "Data too long" for bounded VARCHAR columns. */
    private static String clip(String s, int max) {
        return (s != null && s.length() > max) ? s.substring(0, max) : s;
    }

    // registration_date is VARCHAR in DB
    private static final String UPSERT_SQL =
            "INSERT INTO consumers (" +
                    " msisdn, registration_date, first_name, middle_name, last_name, gender," +
                    " birth_date, birth_place, address, alternate_msisdn1, alternate_msisdn2," +
                    " identification_type, identification_number, created_on, service_provider_id," +
                    " is_consistent, consumer_status" +
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    " registration_date=VALUES(registration_date)," +
                    " first_name=VALUES(first_name)," +
                    " middle_name=VALUES(middle_name)," +
                    " last_name=VALUES(last_name)," +
                    " gender=VALUES(gender)," +
                    " birth_date=VALUES(birth_date)," +
                    " birth_place=VALUES(birth_place)," +
                    " address=VALUES(address)," +
                    " alternate_msisdn1=VALUES(alternate_msisdn1)," +
                    " alternate_msisdn2=VALUES(alternate_msisdn2)," +
                    " identification_type=VALUES(identification_type)," +
                    " identification_number=VALUES(identification_number)," +
                    " service_provider_id=VALUES(service_provider_id)";

    /** Orchestrator: NOT transactional. Keeps DB transactions short and kicks off post-check async. */
    public void processFileVodacom(Path filePath, String operator) throws IOException {
        long t0 = System.currentTimeMillis();
        log.info("ENTER processFile: {} | operator={}", filePath, operator);


        if (Files.notExists(filePath) || !Files.isRegularFile(filePath)) {
            log.warn("File not found or not a regular file: {}", filePath);
            return;
        }

        ServiceProvider sp = serviceProviderRepository.findByNameIgnoreCase(operator)
                .orElseThrow(() -> new IllegalArgumentException("Unknown operator: " + operator));
        Long spId = sp.getId();
        log.info("Resolved ServiceProvider id={}, name={}", spId, sp.getName());

        ServiceProvider sp1 = serviceProviderRepository.findById(spId).orElse(null);
        if (sp1 == null || sp1.isDeleted()) {
            log.info("Skipping file processing for deleted service provider: {}", spId);
            return; // Stop processing
        }

        ProcessedFile fileLog = new ProcessedFile();
        fileLog.setFilename(filePath.getFileName().toString());
        fileLog.setStatus(FileStatus.IN_PROGRESS);
        fileLog.setStartedAt(LocalDateTime.now());
        fileLog.setRecordsProcessed(0);
        processedFileRepository.save(fileLog);
        log.info("ProcessedFile row created (IN_PROGRESS)");

        Path workingCopy = null;
        boolean success = false;
        int totalProcessed = 0;

        try {
            // Work on a copy outside OneDrive (prevents locks)
            workingCopy = createWorkingCopy(filePath);

            // Detect charset & separator on the working copy
            Charset cs = pickCharset(workingCopy);
            log.info("Detected charset: {}", cs.displayName());
            char sep = detectSeparator(workingCopy, cs);
            log.info("Detected CSV separator: '{}'", sep == '\t' ? "\\t" : String.valueOf(sep));

            // Do the actual ingestion inside a short transaction
            totalProcessed = ingestFileTxVodacom(workingCopy, spId, sep, cs);
            success = true;

            fileLog.setRecordsProcessed(totalProcessed);
            fileLog.setStatus(FileStatus.COMPLETE);
            fileLog.setCompletedAt(LocalDateTime.now());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);

        } catch (Exception ex) {
            log.error("Ingest failed for {}: {}", (workingCopy != null ? workingCopy : filePath), ex.toString(), ex);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Ingestion error: " + ex.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        } finally {
            if (workingCopy != null) {
                try { Files.deleteIfExists(workingCopy); }
                catch (IOException delEx) { log.warn("Could not delete working copy {}: {}", workingCopy, delEx.toString()); }
            }
        }

        // Move original after IO closes
        try {
            moveOriginal(filePath, success ? "processed" : "failed", fileLog);
        } catch (IOException moveEx) {
            log.error("Final move failed for {}: {}", filePath, moveEx.toString(), moveEx);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Move failed: " + moveEx.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        }

        // Kick off checkConsumer WITHOUT blocking the scheduler thread
        if (success) {
            runCheckConsumerAsync(sp);
        }

        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));
    }


    public void processFileAirtel(Path filePath, String operator) throws IOException {
        long t0 = System.currentTimeMillis();
        log.info("ENTER processFile: {} | operator={}", filePath, operator);

        if (Files.notExists(filePath) || !Files.isRegularFile(filePath)) {
            log.warn("File not found or not a regular file: {}", filePath);
            return;
        }

        ServiceProvider sp = serviceProviderRepository.findByNameIgnoreCase(operator)
                .orElseThrow(() -> new IllegalArgumentException("Unknown operator: " + operator));
        Long spId = sp.getId();
        log.info("Resolved ServiceProvider id={}, name={}", spId, sp.getName());


        ServiceProvider sp1 = serviceProviderRepository.findById(spId).orElse(null);
        if (sp1 == null || sp1.isDeleted()) {
            log.info("Skipping file processing for deleted service provider: {}", spId);
            return; // Stop processing
        }


        ProcessedFile fileLog = new ProcessedFile();
        fileLog.setFilename(filePath.getFileName().toString());
        fileLog.setStatus(FileStatus.IN_PROGRESS);
        fileLog.setStartedAt(LocalDateTime.now());
        fileLog.setRecordsProcessed(0);
        processedFileRepository.save(fileLog);
        log.info("ProcessedFile row created (IN_PROGRESS)");

        Path workingCopy = null;
        boolean success = false;
        int totalProcessed = 0;

        try {
            workingCopy = createWorkingCopy(filePath);

            Charset cs = pickCharset(workingCopy);
            log.info("Detected charset: {}", cs.displayName());
            char sep = detectSeparator(workingCopy, cs);
            log.info("Detected CSV separator: '{}'", sep == '\t' ? "\\t" : String.valueOf(sep));

            totalProcessed = ingestFileTxAirtel(workingCopy, spId, sep, cs);
            success = true;

            fileLog.setRecordsProcessed(totalProcessed);
            fileLog.setStatus(FileStatus.COMPLETE);
            fileLog.setCompletedAt(LocalDateTime.now());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);

        } catch (Exception ex) {
            log.error("Ingest failed for {}: {}", (workingCopy != null ? workingCopy : filePath), ex.toString(), ex);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Ingestion error: " + ex.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        } finally {
            if (workingCopy != null) {
                try { Files.deleteIfExists(workingCopy); }
                catch (IOException delEx) { log.warn("Could not delete working copy {}: {}", workingCopy, delEx.toString()); }
            }
        }

        try {
            moveOriginal(filePath, success ? "processed" : "failed", fileLog);
        } catch (IOException moveEx) {
            log.error("Final move failed for {}: {}", filePath, moveEx.toString(), moveEx);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Move failed: " + moveEx.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        }

        if (success) {
            runCheckConsumerAsync(sp);
        }

        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));
    }


    public void processFileVodacomForPerformance(Path filePath, String operator) throws IOException {

        long t0 = System.currentTimeMillis();
        log.info("ENTER processFileOrange: {} | operator={}", filePath, operator);

        if (Files.notExists(filePath) || !Files.isRegularFile(filePath)) {
            log.warn("File not found or not a regular file: {}", filePath);
            return;
        }

        ServiceProvider sp = serviceProviderRepository.findByNameIgnoreCase(operator)
                .orElseThrow(() -> new IllegalArgumentException("Unknown operator: " + operator));
        Long spId = sp.getId();
        log.info("Resolved ServiceProvider id={}, name={}", spId, sp.getName());


        String absolutePath = filePath.toAbsolutePath().toString().replace("\\", "/");
        log.info(" Importing Vodacom consumer data from file: {}", absolutePath);

        consumerRepository.loadVodacomCsv(absolutePath);

        log.info(" Successfully imported Vodacom consumers from {}", absolutePath);


        ProcessedFile fileLog = new ProcessedFile();
        fileLog.setFilename(filePath.getFileName().toString());
        fileLog.setStatus(FileStatus.IN_PROGRESS);
        fileLog.setStartedAt(LocalDateTime.now());
        fileLog.setRecordsProcessed(0);
        processedFileRepository.save(fileLog);


        boolean success = true;
        int totalProcessed = 0;

        try {
            moveOriginal(filePath, success ? "processed" : "failed", fileLog);
        } catch (IOException moveEx) {
            log.error("Final move failed for {}: {}", filePath, moveEx.toString(), moveEx);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Move failed: " + moveEx.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        }

        if (success) {
            log.info("successs: processed={}");
            runCheckConsumerForVodacomAsync(sp);
        } else {
            log.info("Failure: processed={}");
        }

        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));


        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));
         }




    public void processFileOrangeForPerormanceCheck(Path filePath, String operator) throws IOException {
        long t0 = System.currentTimeMillis();
        log.info("ENTER processFileOrange: {} | operator={}", filePath, operator);

        if (Files.notExists(filePath) || !Files.isRegularFile(filePath)) {
            log.warn("File not found or not a regular file: {}", filePath);
            return;
        }

        ServiceProvider sp = serviceProviderRepository.findByNameIgnoreCase(operator)
                .orElseThrow(() -> new IllegalArgumentException("Unknown operator: " + operator));
        Long spId = sp.getId();
        log.info("Resolved ServiceProvider id={}, name={}", spId, sp.getName());


        String absolutePath = filePath.toAbsolutePath().toString().replace("\\", "/");
        log.info("🚀 Importing Orange consumer data from file: {}", absolutePath);

        consumerRepository.loadOrangeCsv(absolutePath);

        log.info("✅ Successfully imported Orange consumers from {}", absolutePath);


        ProcessedFile fileLog = new ProcessedFile();
        fileLog.setFilename(filePath.getFileName().toString());
        fileLog.setStatus(FileStatus.IN_PROGRESS);
        fileLog.setStartedAt(LocalDateTime.now());
        fileLog.setRecordsProcessed(0);
        processedFileRepository.save(fileLog);


        boolean success = true;
        int totalProcessed = 0;

        try {
            moveOriginal(filePath, success ? "processed" : "failed", fileLog);
        } catch (IOException moveEx) {
            log.error("Final move failed for {}: {}", filePath, moveEx.toString(), moveEx);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Move failed: " + moveEx.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        }

        if (success) {
            log.info("successs: processed={}");
            runCheckConsumerForOrangeAsync(sp);
        } else {
            log.info("Failure: processed={}");
        }

        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));


        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));
    }

    public void processFileOrange(Path filePath, String operator) throws IOException {
        long t0 = System.currentTimeMillis();
        log.info("ENTER processFileOrange: {} | operator={}", filePath, operator);

        if (Files.notExists(filePath) || !Files.isRegularFile(filePath)) {
            log.warn("File not found or not a regular file: {}", filePath);
            return;
        }

        ServiceProvider sp = serviceProviderRepository.findByNameIgnoreCase(operator)
                .orElseThrow(() -> new IllegalArgumentException("Unknown operator: " + operator));
        Long spId = sp.getId();
        log.info("Resolved ServiceProvider id={}, name={}", spId, sp.getName());

        ProcessedFile fileLog = new ProcessedFile();
        fileLog.setFilename(filePath.getFileName().toString());
        fileLog.setStatus(FileStatus.IN_PROGRESS);
        fileLog.setStartedAt(LocalDateTime.now());
        fileLog.setRecordsProcessed(0);
        processedFileRepository.save(fileLog);

        Path workingCopy = null;
        boolean success = false;
        int totalProcessed = 0;

        try {
            workingCopy = Files.copy(filePath, Files.createTempFile("kyc-working", ".work"),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            Charset cs = Charset.forName("UTF-8");
            char sep = ',';

            // ✅ call the transactional bean (ensures @Transactional works)
            totalProcessed = orangeIngestionService.ingestFileTxOrange(workingCopy, spId, sep, cs);
            success = true;

            fileLog.setRecordsProcessed(totalProcessed);
            fileLog.setStatus(FileStatus.COMPLETE);
            fileLog.setCompletedAt(LocalDateTime.now());
        } catch (Exception ex) {
            log.error("Ingest failed for {}: {}", filePath, ex.toString(), ex);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError(ex.getMessage());
        } finally {
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);

            if (workingCopy != null) {
                try { Files.deleteIfExists(workingCopy); }
                catch (IOException ignore) {}
            }
        }

        try {
            moveOriginal(filePath, success ? "processed" : "failed", fileLog);
        } catch (IOException moveEx) {
            log.error("Final move failed for {}: {}", filePath, moveEx.toString(), moveEx);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Move failed: " + moveEx.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        }

        if (success) {
            log.info("successs: processed={}");
            runCheckConsumerForOrangeAsync(sp);
        } else {
            log.info("Failure: processed={}");
        }

        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));


        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));
    }


    public void processFileAfricell(Path filePath, String operator) throws IOException {
        long t0 = System.currentTimeMillis();
        log.info("ENTER processFile: {} | operator={}", filePath, operator);

        if (Files.notExists(filePath) || !Files.isRegularFile(filePath)) {
            log.warn("File not found or not a regular file: {}", filePath);
            return;
        }

        ServiceProvider sp = serviceProviderRepository.findByNameIgnoreCase(operator)
                .orElseThrow(() -> new IllegalArgumentException("Unknown operator: " + operator));
        Long spId = sp.getId();
        log.info("Resolved ServiceProvider id={}, name={}", spId, sp.getName());


        ServiceProvider sp1 = serviceProviderRepository.findById(spId).orElse(null);
        if (sp1 == null || sp1.isDeleted()) {
            log.info("Skipping file processing for deleted service provider: {}", spId);
            return; // Stop processing
        }

        ProcessedFile fileLog = new ProcessedFile();
        fileLog.setFilename(filePath.getFileName().toString());
        fileLog.setStatus(FileStatus.IN_PROGRESS);
        fileLog.setStartedAt(LocalDateTime.now());
        fileLog.setRecordsProcessed(0);
        processedFileRepository.save(fileLog);
        log.info("ProcessedFile row created (IN_PROGRESS)");

        Path workingCopy = null;
        boolean success = false;
        int totalProcessed = 0;

        try {
            workingCopy = createWorkingCopy(filePath);

            Charset cs = pickCharset(workingCopy);
            log.info("Detected charset: {}", cs.displayName());
            char sep = detectSeparator(workingCopy, cs);
            log.info("Detected CSV separator: '{}'", sep == '\t' ? "\\t" : String.valueOf(sep));

            totalProcessed = ingestFileTxAfricell(workingCopy, spId, sep, cs);
            success = true;

            fileLog.setRecordsProcessed(totalProcessed);
            fileLog.setStatus(FileStatus.COMPLETE);
            fileLog.setCompletedAt(LocalDateTime.now());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);

        } catch (Exception ex) {
            log.error("Ingest failed for {}: {}", (workingCopy != null ? workingCopy : filePath), ex.toString(), ex);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Ingestion error: " + ex.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        } finally {
            if (workingCopy != null) {
                try { Files.deleteIfExists(workingCopy); }
                catch (IOException delEx) { log.warn("Could not delete working copy {}: {}", workingCopy, delEx.toString()); }
            }
        }

        try {
            moveOriginal(filePath, success ? "processed" : "failed", fileLog);
        } catch (IOException moveEx) {
            log.error("Final move failed for {}: {}", filePath, moveEx.toString(), moveEx);
            fileLog.setStatus(FileStatus.FAILED);
            fileLog.setLastError("Move failed: " + moveEx.getMessage());
            fileLog.setLastUpdated(LocalDateTime.now());
            processedFileRepository.save(fileLog);
        }

        if (success) {
            log.info("successs: processed={}");
            runCheckConsumerAsync(sp);
        } else {
            log.info("Failure: processed={}");
        }

        log.info("DONE: processed={} in {} ms", totalProcessed, (System.currentTimeMillis() - t0));
    }

    /* ================= Ingest (short TX) ================= */

    /*@Transactional(rollbackFor = Exception.class)
    protected int ingestFileTxVodacom(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        final List<RowData> batch = new ArrayList<>(BATCH_SIZE);
        int total = 0;

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;

            while ((row = csv.readNext()) != null) {
                stripBomInPlace(row);

                if (isHeader) { isHeader = false; log.info("Header column count: {}", row.length); continue; }
                if (row.length == 0) continue;

                RowData r = mapRowVodacom(row, spId, nowTs);
                if (r == null) continue;

                // Allow empty/invalid msisdn: store NULL, do not skip
                r.msisdn = normalizeMsisdnAllowNull(r.msisdn);

                // clip to schema bounds
                r.firstName           = clip(r.firstName, 100);
                r.middleName          = clip(r.middleName, 255);
                r.lastName            = clip(r.lastName, 45);
                r.gender              = clip(r.gender, 45);
                r.birthDateStr        = clip(r.birthDateStr, 45);
                r.birthPlace          = clip(r.birthPlace, 45);
                r.alt1                = clip(r.alt1, 255);
                r.alt2                = clip(r.alt2, 255);
                r.idType              = clip(r.idType, 45);
                r.idNumber            = clip(r.idNumber, 45);
                r.registrationDateStr = clip(r.registrationDateStr, 50);
                // address is TEXT

                batch.add(r);
                if (batch.size() >= BATCH_SIZE) {
                    total += executeBatch(batch);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                total += executeBatch(batch);
                batch.clear();
            }
        }

        return total;
    }*/


    // remove the old private updateConsistencyFlag(Consumer) from FileProcessingService

    // ✅ Ingest method now delegates to ConsumerServiceImpl
    protected int ingestFileTxVodacom(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        int total = 0;

        // 🔹 Consumers to update/save
        List<Consumer> toSave = new ArrayList<>();
        List<Consumer> duplicatesToSoftDelete = new ArrayList<>();

        // 🔹 Track per-vendor/date counters
        Map<String, AtomicInteger> vendorCounters = new HashMap<>();
        SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy");

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;

            while ((row = csv.readNext()) != null) {
                stripBomInPlace(row);

                if (isHeader) {
                    isHeader = false;
                    log.info("Header column count: {}", row.length);
                    continue;
                }
                if (row.length == 0) continue;

                RowData r = mapRowVodacom(row, spId, nowTs);
                if (r == null) continue;

                // Normalize MSISDN
                r.msisdn = normalizeMsisdnAllowNull(r.msisdn);

                // 🔹 Try to find an existing consumer
                Consumer consumer = findOrMergeConsumer(r, duplicatesToSoftDelete);

                // 🔹 Merge data into consumer (update only if missing)
                mergeConsumerFields(consumer, r, nowTs, spId);

                // 🔹 Update consistency flag
                consumerServiceImpl.updateConsistencyFlag(consumer);

                // ✅ Assign vendorCode (per vendor/date, sequential)
                String vendor = getServiceProviderName(spId); // helper method or fetch from DB
                String date   = df.format(nowTs);
                String key    = vendor + "-" + date;

                vendorCounters.putIfAbsent(key, new AtomicInteger(1));
                int seq = vendorCounters.get(key).getAndIncrement();

                consumer.setVendorCode(vendor + "-" + date + "-" + seq);

                toSave.add(consumer);

                // 🔹 Batch flush
                if (toSave.size() >= BATCH_SIZE) {
                    flushBatch(toSave, duplicatesToSoftDelete);
                    total += toSave.size();
                    toSave.clear();
                }
            }

            if (!toSave.isEmpty()) {
                flushBatch(toSave, duplicatesToSoftDelete);
                total += toSave.size();
                toSave.clear();
            }
        }

        return total;
    }







    @Transactional(rollbackFor = Exception.class)
    protected int ingestFileTxAirtel(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        int total = 0;

        // 🔹 Collect active and soft-deleted consumers
        List<Consumer> toSave = new ArrayList<>();
        List<Consumer> duplicatesToSoftDelete = new ArrayList<>();
        // 🔹 Track per-vendor/date counters
        Map<String, AtomicInteger> vendorCounters = new HashMap<>();
        SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy");

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;

            while ((row = csv.readNext()) != null) {
                stripBomInPlace(row);

                if (isHeader) {
                    isHeader = false;
                    log.info("Header column count: {}", row.length);
                    continue;
                }
                if (row.length == 0) continue;

                RowData r = mapRowAirtel(row, spId, nowTs);
                if (r == null) continue;

                // 🔹 Normalize & clip
                r.msisdn              = normalizeMsisdnAllowNull(r.msisdn);
                r.firstName           = clip(r.firstName, 100);
                r.middleName          = clip(r.middleName, 255);
                r.lastName            = clip(r.lastName, 45);
                r.gender              = clip(r.gender, 45);
                r.birthDateStr        = clip(r.birthDateStr, 45);
                r.birthPlace          = clip(r.birthPlace, 45);
                r.alt1                = clip(r.alt1, 255);
                r.alt2                = clip(r.alt2, 255);
                r.idType              = clip(r.idType, 45);
                r.idNumber            = clip(r.idNumber, 45);
                r.registrationDateStr = clip(r.registrationDateStr, 50);
                r.airtelTransactionId = clip(r.airtelTransactionId, 200);

                // 🔹 Deduplication-aware match
                Consumer consumer = findOrMergeConsumer(r, duplicatesToSoftDelete);

                // 🔹 Merge new row data into consumer
                mergeConsumerFields(consumer, r, nowTs, spId);

                // 🔹 Re-check consistency
                consumerServiceImpl.updateConsistencyFlag(consumer);

                // 🔹 Stamp consistentOn (if entity hook not used)
                if (Boolean.TRUE.equals(consumer.getIsConsistent())) {
                    if (consumer.getConsistentOn() == null || "N/A".equalsIgnoreCase(consumer.getConsistentOn())) {
                        consumer.setConsistentOn(LocalDate.now().toString());
                    }
                } else {
                    consumer.setConsistentOn("N/A");
                }

                //  Assign vendorCode (per vendor/date, sequential)
                String vendor = getServiceProviderName(spId); // helper method or fetch from DB
                String date   = df.format(nowTs);
                String key    = vendor + "-" + date;

                vendorCounters.putIfAbsent(key, new AtomicInteger(1));
                int seq = vendorCounters.get(key).getAndIncrement();

                consumer.setVendorCode(vendor + "-" + date + "-" + seq);


                toSave.add(consumer);

                // 🔹 Batch flush (smaller for Airtel)
                if (toSave.size() >= BATCH_SIZE) {
                    total += flushBatch(toSave, duplicatesToSoftDelete);
                }
            }

            if (!toSave.isEmpty()) {
                total += flushBatch(toSave, duplicatesToSoftDelete);
            }
        }

        return total;
    }




    private int flushBatch(List<Consumer> toSave, List<Consumer> duplicatesToSoftDelete) throws InterruptedException {
        if (!duplicatesToSoftDelete.isEmpty()) {
            // Do soft deletes in separate transaction
            for (Consumer dup : duplicatesToSoftDelete) {
                softDeleteConsumer(dup);
            }
            duplicatesToSoftDelete.clear();
        }

        if (toSave.isEmpty()) return 0;

        int retries = 3;
        while (true) {
            try {
                consumerRepository.saveAll(toSave);
                int size = toSave.size();
                toSave.clear();
                return size;
            } catch (PessimisticLockingFailureException e) {
                if (--retries > 0) {
                    log.warn("Lock wait timeout on batch save, retrying… attempts left={}", retries);
                    Thread.sleep(500L);
                } else {
                    log.error("Batch save failed permanently after retries", e);
                    throw e;
                }
            }
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void softDeleteConsumer(Consumer dup) {
        dup.setConsumerStatus(0);
        consumerRepository.save(dup);
    }


    /*@Transactional(rollbackFor = Exception.class)
    protected int ingestFileTxOrange(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        int total = 0;

        // 🔹 Track per-vendor/date counters
        Map<String, AtomicInteger> vendorCounters = new HashMap<>();
        SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy");

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;
            List<Consumer> toSave = new ArrayList<>();

            while ((row = csv.readNext()) != null) {
                stripBomInPlace(row);

                if (isHeader) {
                    isHeader = false;
                    log.info("Header column count: {}", row.length);
                    continue;
                }
                if (row.length == 0) continue;

                RowData r = mapRowOrange(row, spId, nowTs);
                if (r == null) continue;

                // 🔹 Normalize & clip
                r.msisdn              = normalizeMsisdnAllowNull(r.msisdn);
                r.firstName           = clip(r.firstName, 100);
                r.middleName          = clip(r.middleName, 255);
                r.lastName            = clip(r.lastName, 45);
                r.gender              = clip(r.gender, 45);
                r.birthDateStr        = clip(r.birthDateStr, 45);
                r.birthPlace          = clip(r.birthPlace, 45);
                r.alt1                = clip(r.alt1, 255);
                r.alt2                = clip(r.alt2, 255);
                r.idType              = clip(r.idType, 45);
                r.idNumber            = clip(r.idNumber, 45);
                r.registrationDateStr = clip(r.registrationDateStr, 50);

                // 🔹 Try to match existing consumer using spec
                Specification<Consumer> spec = ConsumerSpecifications.matchConsumer(r);
                List<Consumer> matches = consumerRepository.findAll(spec);

                Consumer consumer;
                if (!matches.isEmpty()) {
                    consumer = matches.get(0);
                    // ✅ Merge fields only if missing
                    if (isEmpty(consumer.getMsisdn()) && notEmpty(r.msisdn)) consumer.setMsisdn(r.msisdn);
                    if (isEmpty(consumer.getFirstName()) && notEmpty(r.firstName)) consumer.setFirstName(r.firstName);
                    if (isEmpty(consumer.getLastName()) && notEmpty(r.lastName)) consumer.setLastName(r.lastName);
                    if (isEmpty(consumer.getIdentificationNumber()) && notEmpty(r.idNumber)) consumer.setIdentificationNumber(r.idNumber);
                    if (isEmpty(consumer.getIdentificationType()) && notEmpty(r.idType)) consumer.setIdentificationType(r.idType);
                    if (isEmpty(consumer.getGender()) && notEmpty(r.gender)) consumer.setGender(r.gender);
                    if (isEmpty(consumer.getBirthPlace()) && notEmpty(r.birthPlace)) consumer.setBirthPlace(r.birthPlace);
                    if (isEmpty(consumer.getAddress()) && notEmpty(r.address)) consumer.setAddress(r.address);
                    if (isEmpty(consumer.getRegistrationDate()) && notEmpty(r.registrationDateStr)) consumer.setRegistrationDate(r.registrationDateStr);
                    if (isEmpty(consumer.getBirthDate()) && notEmpty(r.birthDateStr)) consumer.setBirthDate(r.birthDateStr);
                    if (isEmpty(consumer.getAlternateMsisdn1()) && notEmpty(r.alt1)) consumer.setAlternateMsisdn1(r.alt1);
                    if (isEmpty(consumer.getAlternateMsisdn2()) && notEmpty(r.alt2)) consumer.setAlternateMsisdn2(r.alt2);
                    if (isEmpty(consumer.getMiddleName()) && notEmpty(r.middleName)) consumer.setMiddleName(r.middleName);

                } else {
                    // ✅ New consumer
                    consumer = new Consumer();
                    consumer.setFirstName(r.firstName);
                    consumer.setLastName(r.lastName);
                    consumer.setIdentificationNumber(r.idNumber);
                    consumer.setIdentificationType(r.idType);
                    consumer.setMsisdn(r.msisdn);
                    consumer.setGender(r.gender);
                    consumer.setBirthPlace(r.birthPlace);
                    consumer.setAddress(r.address);
                    consumer.setRegistrationDate(r.registrationDateStr);
                    consumer.setBirthDate(r.birthDateStr);
                    consumer.setMiddleName(r.middleName);
                    consumer.setAlternateMsisdn1(r.alt1);
                    consumer.setAlternateMsisdn2(r.alt2);

                    ServiceProvider spRef = new ServiceProvider();
                    spRef.setId(spId);
                    consumer.setServiceProvider(spRef);

                    consumer.setCreatedOn(nowTs.toString());
                }

                // 🔹 Apply consistency check
                consumerServiceImpl.updateConsistencyFlag(consumer);

                // 🔹 Stamp consistentOn (if not using @PreUpdate hook)
                if (Boolean.TRUE.equals(consumer.getIsConsistent())) {
                    if (consumer.getConsistentOn() == null || "N/A".equalsIgnoreCase(consumer.getConsistentOn())) {
                        consumer.setConsistentOn(LocalDate.now().toString());
                    }
                } else {
                    consumer.setConsistentOn("N/A");
                }

                //  Assign vendorCode (per vendor/date, sequential)
                String vendor = getServiceProviderName(spId); // helper method or fetch from DB
                String date   = df.format(nowTs);
                String key    = vendor + "-" + date;

                vendorCounters.putIfAbsent(key, new AtomicInteger(1));
                int seq = vendorCounters.get(key).getAndIncrement();

                consumer.setVendorCode(vendor + "-" + date + "-" + seq);


                toSave.add(consumer);

                // 🔹 Batch flush
                if (toSave.size() >= BATCH_SIZE) {
                    consumerRepository.saveAll(toSave);
                    total += toSave.size();
                    toSave.clear();
                }
            }

            if (!toSave.isEmpty()) {
                consumerRepository.saveAll(toSave);
                total += toSave.size();
            }
        }

        return total;
    }*/


    @Transactional(rollbackFor = Exception.class)
    protected int ingestFileTxOrange(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        int total = 0;

        Map<String, AtomicInteger> vendorCounters = new HashMap<>();
        SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy");

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;
            List<Consumer> batch = new ArrayList<>();

            while ((row = csv.readNext()) != null) {
                stripBomInPlace(row);

                if (isHeader) {
                    isHeader = false;
                    log.info("Header column count: {}", row.length);
                    continue;
                }
                if (row.length == 0) continue;

                RowData r = mapRowOrange(row, spId, nowTs);
                if (r == null) continue;

                // --- Normalize / clip ---
                r.msisdn = normalizeMsisdnAllowNull(r.msisdn);
                r.firstName = clip(r.firstName, 100);
                r.middleName = clip(r.middleName, 255);
                r.lastName = clip(r.lastName, 45);
                r.gender = clip(r.gender, 45);
                r.birthDateStr = clip(r.birthDateStr, 45);
                r.birthPlace = clip(r.birthPlace, 45);
                r.alt1 = clip(r.alt1, 255);
                r.alt2 = clip(r.alt2, 255);
                r.idType = clip(r.idType, 45);
                r.idNumber = clip(r.idNumber, 45);
                r.registrationDateStr = clip(r.registrationDateStr, 50);

                // --- Find or create consumer ---
                Specification<Consumer> spec = ConsumerSpecifications.matchConsumer(r);
                List<Consumer> matches = consumerRepository.findAll(spec);

                Consumer consumer;
                if (!matches.isEmpty()) {
                    consumer = matches.get(0);
                    mergeIfEmpty(consumer, r); // helper (see below)
                } else {
                    consumer = buildNewConsumer(r, spId, nowTs);
                }

                // --- Update consistency ---
                consumerServiceImpl.updateConsistencyFlag(consumer);

                consumer.setConsistentOn(Boolean.TRUE.equals(consumer.getIsConsistent())
                        ? LocalDate.now().toString()
                        : "N/A");

                // --- Vendor code sequence ---
                String vendor = getServiceProviderName(spId);
                String date = df.format(nowTs);
                String key = vendor + "-" + date;
                vendorCounters.putIfAbsent(key, new AtomicInteger(1));
                int seq = vendorCounters.get(key).getAndIncrement();
                consumer.setVendorCode(vendor + "-" + date + "-" + seq);

                batch.add(consumer);

                // --- Commit every 1000 ---
                if (batch.size() >= 1000) {
                    flushAndCommitBatch(batch);
                    total += batch.size();
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                flushAndCommitBatch(batch);
                total += batch.size();
            }
        }

        return total;
    }

    /** Merge helper */
    private void mergeIfEmpty(Consumer consumer, RowData r) {
        if (isEmpty(consumer.getMsisdn()) && notEmpty(r.msisdn)) consumer.setMsisdn(r.msisdn);
        if (isEmpty(consumer.getFirstName()) && notEmpty(r.firstName)) consumer.setFirstName(r.firstName);
        if (isEmpty(consumer.getLastName()) && notEmpty(r.lastName)) consumer.setLastName(r.lastName);
        if (isEmpty(consumer.getIdentificationNumber()) && notEmpty(r.idNumber)) consumer.setIdentificationNumber(r.idNumber);
        if (isEmpty(consumer.getIdentificationType()) && notEmpty(r.idType)) consumer.setIdentificationType(r.idType);
        if (isEmpty(consumer.getGender()) && notEmpty(r.gender)) consumer.setGender(r.gender);
        if (isEmpty(consumer.getBirthPlace()) && notEmpty(r.birthPlace)) consumer.setBirthPlace(r.birthPlace);
        if (isEmpty(consumer.getAddress()) && notEmpty(r.address)) consumer.setAddress(r.address);
        if (isEmpty(consumer.getRegistrationDate()) && notEmpty(r.registrationDateStr)) consumer.setRegistrationDate(r.registrationDateStr);
        if (isEmpty(consumer.getBirthDate()) && notEmpty(r.birthDateStr)) consumer.setBirthDate(r.birthDateStr);
        if (isEmpty(consumer.getAlternateMsisdn1()) && notEmpty(r.alt1)) consumer.setAlternateMsisdn1(r.alt1);
        if (isEmpty(consumer.getAlternateMsisdn2()) && notEmpty(r.alt2)) consumer.setAlternateMsisdn2(r.alt2);
        if (isEmpty(consumer.getMiddleName()) && notEmpty(r.middleName)) consumer.setMiddleName(r.middleName);
    }

    /** Build helper */
    private Consumer buildNewConsumer(RowData r, Long spId, Timestamp nowTs) {
        Consumer consumer = new Consumer();
        consumer.setFirstName(r.firstName);
        consumer.setLastName(r.lastName);
        consumer.setIdentificationNumber(r.idNumber);
        consumer.setIdentificationType(r.idType);
        consumer.setMsisdn(r.msisdn);
        consumer.setGender(r.gender);
        consumer.setBirthPlace(r.birthPlace);
        consumer.setAddress(r.address);
        consumer.setRegistrationDate(r.registrationDateStr);
        consumer.setBirthDate(r.birthDateStr);
        consumer.setMiddleName(r.middleName);
        consumer.setAlternateMsisdn1(r.alt1);
        consumer.setAlternateMsisdn2(r.alt2);
        consumer.setCreatedOn(nowTs.toString());
        ServiceProvider spRef = new ServiceProvider();
        spRef.setId(spId);
        consumer.setServiceProvider(spRef);
        return consumer;
    }

    /** Flush + detach batch safely */
    @Autowired
    private EntityManager entityManager;
    private void flushAndCommitBatch(List<Consumer> batch) {
        consumerRepository.saveAll(batch);
        entityManager.flush();
        entityManager.clear();
    }


    @Transactional(rollbackFor = Exception.class)
    protected int ingestFileTxAfricell(Path workingCopy, Long spId, char sep, Charset cs) throws Exception {
        final Timestamp nowTs = new Timestamp(System.currentTimeMillis());
        int total = 0;
        // 🔹 Track per-vendor/date counters
        Map<String, AtomicInteger> vendorCounters = new HashMap<>();
        SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy");

        try (InputStream in = Files.newInputStream(workingCopy);
             Reader reader = new InputStreamReader(in, cs);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                     .build()) {

            String[] row;
            boolean isHeader = true;
            List<Consumer> toSave = new ArrayList<>();

            while ((row = csv.readNext()) != null) {
                stripBomInPlace(row);

                if (isHeader) {
                    isHeader = false;
                    log.info("Header column count: {}", row.length);
                    continue;
                }
                if (row.length == 0) continue;

                RowData r = mapRowAfricell(row, spId, nowTs);
                if (r == null) continue;

                // 🔹 Normalize & clip
                r.msisdn              = normalizeMsisdnAllowNull(r.msisdn);
                r.firstName           = clip(r.firstName, 100);
                r.middleName          = clip(r.middleName, 255);
                r.lastName            = clip(r.lastName, 45);
                r.gender              = clip(r.gender, 45);
                r.birthDateStr        = clip(r.birthDateStr, 45);
                r.birthPlace          = clip(r.birthPlace, 45);
                r.alt1                = clip(r.alt1, 255);
                r.alt2                = clip(r.alt2, 255);
                r.idType              = clip(r.idType, 45);
                r.idNumber            = clip(r.idNumber, 45);
                r.registrationDateStr = clip(r.registrationDateStr, 50);

                // 🔹 Match existing consumer (by specification)
                Specification<Consumer> spec = ConsumerSpecifications.matchConsumer(r);
                List<Consumer> matches = consumerRepository.findAll(spec);

                Consumer consumer;
                if (!matches.isEmpty()) {
                    consumer = matches.get(0);

                    if (isEmpty(consumer.getMsisdn()) && notEmpty(r.msisdn)) consumer.setMsisdn(r.msisdn);
                    if (isEmpty(consumer.getFirstName()) && notEmpty(r.firstName)) consumer.setFirstName(r.firstName);
                    if (isEmpty(consumer.getLastName()) && notEmpty(r.lastName)) consumer.setLastName(r.lastName);
                    if (isEmpty(consumer.getIdentificationNumber()) && notEmpty(r.idNumber)) consumer.setIdentificationNumber(r.idNumber);
                    if (isEmpty(consumer.getIdentificationType()) && notEmpty(r.idType)) consumer.setIdentificationType(r.idType);
                    if (isEmpty(consumer.getGender()) && notEmpty(r.gender)) consumer.setGender(r.gender);
                    if (isEmpty(consumer.getBirthPlace()) && notEmpty(r.birthPlace)) consumer.setBirthPlace(r.birthPlace);
                    if (isEmpty(consumer.getAddress()) && notEmpty(r.address)) consumer.setAddress(r.address);
                    if (isEmpty(consumer.getRegistrationDate()) && notEmpty(r.registrationDateStr)) consumer.setRegistrationDate(r.registrationDateStr);
                    if (isEmpty(consumer.getBirthDate()) && notEmpty(r.birthDateStr)) consumer.setBirthDate(r.birthDateStr);
                    if (isEmpty(consumer.getAlternateMsisdn1()) && notEmpty(r.alt1)) consumer.setAlternateMsisdn1(r.alt1);
                    if (isEmpty(consumer.getAlternateMsisdn2()) && notEmpty(r.alt2)) consumer.setAlternateMsisdn2(r.alt2);
                    if (isEmpty(consumer.getMiddleName()) && notEmpty(r.middleName)) consumer.setMiddleName(r.middleName);

                } else {
                    // New consumer
                    consumer = new Consumer();
                    consumer.setFirstName(r.firstName);
                    consumer.setLastName(r.lastName);
                    consumer.setIdentificationNumber(r.idNumber);
                    consumer.setIdentificationType(r.idType);
                    consumer.setMsisdn(r.msisdn);
                    consumer.setGender(r.gender);
                    consumer.setBirthPlace(r.birthPlace);
                    consumer.setAddress(r.address);
                    consumer.setRegistrationDate(r.registrationDateStr);
                    consumer.setBirthDate(r.birthDateStr);
                    consumer.setMiddleName(r.middleName);
                    consumer.setAlternateMsisdn1(r.alt1);
                    consumer.setAlternateMsisdn2(r.alt2);

                    ServiceProvider spRef = new ServiceProvider();
                    spRef.setId(spId);
                    consumer.setServiceProvider(spRef);

                    consumer.setCreatedOn(nowTs.toString());
                }

                // 🔹 Apply consistency check
                consumerServiceImpl.updateConsistencyFlag(consumer);

                // 🔹 Stamp consistentOn if not using @PreUpdate hook
                if (Boolean.TRUE.equals(consumer.getIsConsistent())) {
                    if (consumer.getConsistentOn() == null || "N/A".equalsIgnoreCase(consumer.getConsistentOn())) {
                        consumer.setConsistentOn(LocalDate.now().toString());
                    }
                } else {
                    consumer.setConsistentOn("N/A");
                }

                //  Assign vendorCode (per vendor/date, sequential)
                String vendor = getServiceProviderName(spId); // helper method or fetch from DB
                String date   = df.format(nowTs);
                String key    = vendor + "-" + date;

                vendorCounters.putIfAbsent(key, new AtomicInteger(1));
                int seq = vendorCounters.get(key).getAndIncrement();

                consumer.setVendorCode(vendor + "-" + date + "-" + seq);

                toSave.add(consumer);

                if (toSave.size() >= BATCH_SIZE) {
                    consumerRepository.saveAll(toSave);
                    total += toSave.size();
                    toSave.clear();
                }
            }

            if (!toSave.isEmpty()) {
                consumerRepository.saveAll(toSave);
                total += toSave.size();
            }
        }

        return total;
    }


    private Consumer findOrMergeConsumer(RowData r, List<Consumer> duplicatesToSoftDelete) {

        // 1. Strongest match: Vodacom transaction ID
        if (notEmpty(r.vodacomTransactionId)) {
            Optional<Consumer> existing = consumerRepository.findByVodacomTransactionId(r.vodacomTransactionId.trim());
            if (existing.isPresent()) {
                Consumer consumer = existing.get();
                System.out.println("Found existing consumer by vodacomTransactionId=" + r.vodacomTransactionId +
                        " → id=" + consumer.getId());
                return consumer; // always update existing instead of insert
            } else {
                System.out.println("New consumer for vodacomTransactionId=" + r.vodacomTransactionId);
                Consumer consumer = new Consumer();
                consumer.setVodacomTransactionId(r.vodacomTransactionId.trim());
                return consumer;
            }
        }

        // 2. Strongest match: Airtel transaction ID
        if (notEmpty(r.airtelTransactionId)) {
            Optional<Consumer> existing = consumerRepository.findByAirtelTransactionId(r.airtelTransactionId.trim());
            if (existing.isPresent()) {
                Consumer consumer = existing.get();
                System.out.println("Found existing consumer by airtelTransactionId=" + r.airtelTransactionId +
                        " → id=" + consumer.getId());
                return consumer;
            } else {
                System.out.println("New consumer for airtelTransactionId=" + r.airtelTransactionId);
                Consumer consumer = new Consumer();
                consumer.setAirtelTransactionId(r.airtelTransactionId.trim());
                return consumer;
            }
        }

        // 3. Exact match fallback: ID + ID Type + MSISDN
        if (notEmpty(r.idNumber) && notEmpty(r.idType) && notEmpty(r.msisdn)) {
            List<Consumer> exactMatches = consumerRepository.findAll((root, query, cb) -> cb.and(
                    cb.equal(root.get("identificationNumber"), r.idNumber),
                    cb.equal(root.get("identificationType"), r.idType),
                    cb.equal(root.get("msisdn"), r.msisdn)
            ));
            if (!exactMatches.isEmpty()) {
                return exactMatches.get(0);
            }
        }

        // 4. Person match: same person (Name + DOB + MSISDN)
        if (notEmpty(r.firstName) && notEmpty(r.lastName) && notEmpty(r.birthDateStr) && notEmpty(r.msisdn)) {
            List<Consumer> personMatches = consumerRepository.findAll((root, query, cb) -> cb.and(
                    cb.equal(root.get("firstName"), r.firstName),
                    cb.equal(root.get("lastName"), r.lastName),
                    cb.equal(root.get("birthDate"), r.birthDateStr),
                    cb.equal(root.get("msisdn"), r.msisdn)
            ));

            if (!personMatches.isEmpty()) {
                Consumer primary = personMatches.get(0);

                if (personMatches.size() > 1) {
                    System.out.println("Merging " + personMatches.size() + " duplicate person matches into id=" + primary.getId());
                    for (int i = 1; i < personMatches.size(); i++) {
                        Consumer dup = personMatches.get(i);
                        mergeDuplicates(primary, dup, duplicatesToSoftDelete);
                    }
                }

                return primary;
            }
        }

        // 5. No match → new consumer
        return new Consumer();
    }










    private void mergeDuplicates(Consumer primary, Consumer dup, List<Consumer> duplicatesToSoftDelete) {
        // Merge missing fields
        applyIfEmpty(primary::getFirstName, primary::setFirstName, dup.getFirstName());
        applyIfEmpty(primary::getLastName, primary::setLastName, dup.getLastName());
        applyIfEmpty(primary::getAddress, primary::setAddress, dup.getAddress());
        applyIfEmpty(primary::getBirthPlace, primary::setBirthPlace, dup.getBirthPlace());
        applyIfEmpty(primary::getGender, primary::setGender, dup.getGender());
        applyIfEmpty(primary::getNationality, primary::setNationality, dup.getNationality());
        applyIfEmpty(primary::getRegistrationDate, primary::setRegistrationDate, dup.getRegistrationDate());

        // Merge MSISDNs
        if (dup.getMsisdn() != null && !dup.getMsisdn().equals(primary.getMsisdn())) {
            if (primary.getAlternateMsisdn1() == null) {
                primary.setAlternateMsisdn1(dup.getMsisdn());
            } else if (primary.getAlternateMsisdn2() == null &&
                    !primary.getAlternateMsisdn1().equals(dup.getMsisdn())) {
                primary.setAlternateMsisdn2(dup.getMsisdn());
            }
        }

        // Soft delete duplicate
        dup.setConsumerStatus(0);
        duplicatesToSoftDelete.add(dup);

        log.info("Soft-deleted duplicate consumer id={} merged into id={}", dup.getId(), primary.getId());
    }








    private void mergeConsumerFields(Consumer consumer, FileProcessingService.RowData r, Timestamp nowTs, Long spId) {
        // Handle MSISDN updates anchored on transaction IDs
        if (notEmpty(r.vodacomTransactionId) || notEmpty(r.airtelTransactionId)) {
            if (notEmpty(r.msisdn)) {
                if (consumer.getMsisdn() == null) {
                    System.out.println("Setting MSISDN for consumer id=" + consumer.getId() + " → " + r.msisdn);
                    consumer.setMsisdn(r.msisdn);
                } else if (!consumer.getMsisdn().equals(r.msisdn)) {
                    // Preserve old MSISDNs
                    if (consumer.getAlternateMsisdn1() == null) {
                        consumer.setAlternateMsisdn1(consumer.getMsisdn());
                    } else if (consumer.getAlternateMsisdn2() == null &&
                            !consumer.getAlternateMsisdn1().equals(r.msisdn)) {
                        consumer.setAlternateMsisdn2(consumer.getMsisdn());
                    }


                    LocalDateTime now = LocalDateTime.now();
                    String formattedDate = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    //consumer.setConsistentOn(formattedDate);
                    System.out.println("Updating MSISDN for consumer id=" + consumer.getId() + " → " + r.msisdn);
                    consumer.setMsisdn(r.msisdn);
                }
            }
        } else {
            // Fallback when no transactionId
            if (notEmpty(r.msisdn)) {
                if (consumer.getMsisdn() == null) {
                    consumer.setMsisdn(r.msisdn);
                    System.out.println("Set MSISDN (no trxId) for consumer id=" + consumer.getId() + " → " + r.msisdn);
                } else if (!consumer.getMsisdn().equals(r.msisdn)) {
                    System.out.println("Skipping MSISDN change without trxId (" + consumer.getMsisdn() +
                            " → " + r.msisdn + ") for consumer id=" + consumer.getId());
                }
            }
        }

        // Always overwrite ID fields
        if (notEmpty(r.idNumber)) {
            if (consumer.getIdentificationNumber() == null || consumer.getIdentificationNumber().isBlank()) {
                System.out.println("Setting identificationNumber for consumer id=" + consumer.getId() + " → " + r.idNumber);
            } else if (!consumer.getIdentificationNumber().equals(r.idNumber)) {
                System.out.println("Updating identificationNumber for consumer id=" + consumer.getId() +
                        " from " + consumer.getIdentificationNumber() + " → " + r.idNumber);
            }
            consumer.setIdentificationNumber(r.idNumber);
        }

        if (notEmpty(r.idType)) {
            if (consumer.getIdentificationType() == null || consumer.getIdentificationType().isBlank()) {
                System.out.println("Setting identificationType for consumer id=" + consumer.getId() + " → " + r.idType);
            } else if (!consumer.getIdentificationType().equals(r.idType)) {
                System.out.println("Updating identificationType for consumer id=" + consumer.getId() +
                        " from " + consumer.getIdentificationType() + " → " + r.idType);
            }
            consumer.setIdentificationType(r.idType);
        }

        if (notEmpty(r.vodacomTransactionId)) {
            if (consumer.getVodacomTransactionId() == null ||
                    !consumer.getVodacomTransactionId().equals(r.vodacomTransactionId)) {
                consumer.setVodacomTransactionId(r.vodacomTransactionId.trim());
                System.out.println("Updated Vodacom TRX_ID → " + r.vodacomTransactionId);
            }
        }

        if (notEmpty(r.airtelTransactionId)) {
            if (consumer.getAirtelTransactionId() == null ||
                    !consumer.getAirtelTransactionId().equals(r.airtelTransactionId)) {
                consumer.setAirtelTransactionId(r.airtelTransactionId.trim());
                System.out.println("Updated Airtel TRX_ID → " + r.airtelTransactionId);
            }
        }

        // Apply other fields only if empty (don’t overwrite existing good data)
        applyIfEmpty(consumer::getFirstName, consumer::setFirstName, r.firstName);
        applyIfEmpty(consumer::getLastName, consumer::setLastName, r.lastName);
        applyIfEmpty(consumer::getMiddleName, consumer::setMiddleName, r.middleName);
        applyIfEmpty(consumer::getGender, consumer::setGender, r.gender);
        applyIfEmpty(consumer::getBirthPlace, consumer::setBirthPlace, r.birthPlace);
        applyIfEmpty(consumer::getAddress, consumer::setAddress, r.address);
        applyIfEmpty(consumer::getRegistrationDate, consumer::setRegistrationDate, r.registrationDateStr);
        applyIfEmpty(consumer::getBirthDate, consumer::setBirthDate, r.birthDateStr);

        // Fill alternate MSISDNs if present
        applyIfEmpty(consumer::getAlternateMsisdn1, consumer::setAlternateMsisdn1, r.alt1);
        applyIfEmpty(consumer::getAlternateMsisdn2, consumer::setAlternateMsisdn2, r.alt2);

        // Bootstrap new consumer
        if (consumer.getId() == null) {
            consumer.setCreatedOn(nowTs.toString());
            ServiceProvider spRef = new ServiceProvider();
            spRef.setId(spId);
            consumer.setServiceProvider(spRef);
            System.out.println("Bootstrapping new consumer for serviceProvider=" + spId + " createdOn=" + nowTs);
        }
    }







    /** Utility: update only if current value is empty. */

    private void applyIfEmpty(Supplier<String> getter, java.util.function.Consumer<String> setter, String newValue) {
        if ((getter.get() == null || getter.get().trim().isEmpty())
                && newValue != null && !newValue.trim().isEmpty()) {
            setter.accept(newValue);
        }
    }


    /** Utility: mark consumer consistent/inconsistent based on MSISDN duplicates. */


    // small helper
    private boolean isNullOrEmpty(String s) {
        return (s == null || s.trim().isEmpty());
    }





    private int executeBatch(List<RowData> rows) {
        jdbcTemplate.batchUpdate(UPSERT_SQL, new BatchPreparedStatementSetter() {
            @Override public void setValues(PreparedStatement ps, int i) throws SQLException {
                RowData r = rows.get(i);
                int p = 1;
                ps.setString(p++, r.msisdn);
                ps.setString(p++, r.registrationDateStr);
                ps.setString(p++, r.firstName);
                ps.setString(p++, r.middleName);
                ps.setString(p++, r.lastName);
                ps.setString(p++, r.gender);
                ps.setString(p++, r.birthDateStr);
                ps.setString(p++, r.birthPlace);
                ps.setString(p++, r.address);
                ps.setString(p++, r.alt1);
                ps.setString(p++, r.alt2);
                ps.setString(p++, r.idType);
                ps.setString(p++, r.idNumber);
                ps.setString(p++, r.createdOnTs);
                ps.setLong(p++, r.serviceProviderId);
                ps.setInt(p++, 1); // is_consistent
                ps.setInt(p++, 0); // consumer_status
            }
            @Override public int getBatchSize() { return rows.size(); }
        });
        return rows.size();
    }

    /* ================= Post-check (async) ================= */

    private void runCheckConsumerAsync(ServiceProvider sp) {
        log.info("checkConsumer already to use");
        Long spId = sp.getId();
        if (!RUNNING_CHECKS.add(spId)) {
            log.info("checkConsumer already running for operator {}, skipping", sp.getName());
            return;
        }
        Runnable job = () -> {
            try {
                log.info("Starting checkConsumer for operator {}", sp.getName());
                // Scope reduction: only that operator’s consumers
                List<Consumer> list = consumerRepository.findAllByServiceProvider_Id(spId);
                //User user = userService.getUserByEmail("system@itmaxglobal.com");
                User user = userService.getUserByEmail("cadmin@itmaxglobal.com");
                consumerServiceImpl.checkConsumer(list, user, sp);

                consumerAnomalyRepository.findAllByConsumerIn(list);
                consumerServiceImpl.updateAnomalyStatusForConsumers(list, user, sp);

                log.info("Finished checkConsumer for operator {}", sp.getName());
            } catch (Exception ex) {
                log.error("checkConsumer failed for operator {}: {}", sp.getName(), ex.toString(), ex);
            } finally {
                RUNNING_CHECKS.remove(spId);
            }
        };

        if (taskExecutor != null) {
            taskExecutor.execute(job);
        } else {
            new Thread(job, "check-consumer-" + spId).start();
        }
    }


    private void runCheckConsumerForVodacomAsync(ServiceProvider sp) {
        log.info("Preparing to run checkConsumer for operator {}", sp.getName());
        Long spId = sp.getId();

        // Prevent concurrent runs for the same operator
        if (!RUNNING_CHECKS.add(spId)) {
            log.info("checkConsumer already running for operator {}, skipping", sp.getName());
            return;
        }

        Runnable job = () -> {
            try {
                log.info("Starting checkConsumer for operator {}", sp.getName());

                // 1. Fetch all consumers for the given service provider
                List<Consumer> allConsumers = consumerRepository.findAllByServiceProvider_Id(spId);

                if (allConsumers == null || allConsumers.isEmpty()) {
                    log.warn("No consumers found for operator {}", sp.getName());
                    return;
                }

                // 2. Filter only active consumers (estat = ACTIF)
                List<Consumer> activeConsumers = allConsumers.stream()
                        .filter(c -> "accepted".equalsIgnoreCase(c.getEstat()))
                        .collect(Collectors.toList());

                // 3. Log inactive consumers
                allConsumers.stream()
                        .filter(c -> !"accepted".equalsIgnoreCase(c.getEstat()))
                        .forEach(c ->
                                log.info("Skipping inactive consumer: status='{}' | TransactionId={}",
                                        c.getEstat(), c.getOrangeTransactionId())
                        );

                // 4. Process only active consumers
                if (!activeConsumers.isEmpty()) {
                    log.info("Found {} active consumers for operator {}", activeConsumers.size(), sp.getName());

                    User user = userService.getUserByEmail("cadmin@itmaxglobal.com");

                    consumerServiceImpl.checkConsumerForVodacom(activeConsumers, user, sp);
                    consumerServiceImpl.updateAnomalyStatusForConsumers(activeConsumers, user, sp);

                    log.info("checkConsumer completed successfully for operator {}", sp.getName());
                } else {
                    log.info("No active (ACTIF) consumers found for operator {}", sp.getName());
                }

            } catch (Exception ex) {
                log.error("checkConsumer failed for operator {}: {}", sp.getName(), ex.getMessage(), ex);
            } finally {
                RUNNING_CHECKS.remove(spId);
            }
        };

        // 5. Execute asynchronously
        if (taskExecutor != null) {
            taskExecutor.execute(job);
        } else {
            new Thread(job, "check-consumer-" + spId).start();
        }
    }

    private void runCheckConsumerForOrangeAsync(ServiceProvider sp) {
        log.info("Preparing to run checkConsumer for operator {}", sp.getName());
        Long spId = sp.getId();

        // Prevent concurrent runs for the same operator
        if (!RUNNING_CHECKS.add(spId)) {
            log.info("checkConsumer already running for operator {}, skipping", sp.getName());
            return;
        }

        Runnable job = () -> {
            try {
                log.info("Starting checkConsumer for operator {}", sp.getName());

                // 1. Fetch all consumers for the given service provider
                List<Consumer> allConsumers = consumerRepository.findAllByServiceProvider_Id(spId);

                if (allConsumers == null || allConsumers.isEmpty()) {
                    log.warn("No consumers found for operator {}", sp.getName());
                    return;
                }

                // 2. Filter only active consumers (estat = ACTIF)
                List<Consumer> activeConsumers = allConsumers.stream()
                        .filter(c -> "ACTIF".equalsIgnoreCase(c.getEstat()))
                        .collect(Collectors.toList());

                // 3. Log inactive consumers
                allConsumers.stream()
                        .filter(c -> !"ACTIF".equalsIgnoreCase(c.getEstat()))
                        .forEach(c ->
                                log.info("Skipping inactive consumer: status='{}' | TransactionId={}",
                                        c.getEstat(), c.getOrangeTransactionId())
                        );

                // 4. Process only active consumers
                if (!activeConsumers.isEmpty()) {
                    log.info("Found {} active consumers for operator {}", activeConsumers.size(), sp.getName());

                    User user = userService.getUserByEmail("cadmin@itmaxglobal.com");

                    consumerServiceImpl.checkConsumerForOrange(activeConsumers, user, sp);
                    consumerServiceImpl.updateAnomalyStatusForConsumers(activeConsumers, user, sp);

                    log.info("checkConsumer completed successfully for operator {}", sp.getName());
                } else {
                    log.info("No active (ACTIF) consumers found for operator {}", sp.getName());
                }

            } catch (Exception ex) {
                log.error("checkConsumer failed for operator {}: {}", sp.getName(), ex.getMessage(), ex);
            } finally {
                RUNNING_CHECKS.remove(spId);
            }
        };

        // 5. Execute asynchronously
        if (taskExecutor != null) {
            taskExecutor.execute(job);
        } else {
            new Thread(job, "check-consumer-" + spId).start();
        }
    }


    /* ================= IO helpers ================= */

    private Path createWorkingCopy(Path source) throws IOException {
        Path workDir = resolveWorkDir();
        Files.createDirectories(workDir);
        String stamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").format(LocalDateTime.now());
        String base = source.getFileName().toString();
        Path target = workDir.resolve(base + "." + stamp + ".work");
        safeCopyWithRetry(source, target);
        return target;
    }

    private Path resolveWorkDir() {
        if (configuredWorkDir != null && !configuredWorkDir.isBlank()) {
            return Paths.get(configuredWorkDir);
        }
        return Paths.get(System.getProperty("java.io.tmpdir"), "kyc-working");
    }

    private void moveOriginal(Path original, String subfolder, ProcessedFile fileLog) throws IOException {
        Path destDir = original.getParent().resolve(subfolder);
        Files.createDirectories(destDir);
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String base = original.getFileName().toString();
        String newName = base.replace(".csv", "_" + ts + ".csv");
        Path target = destDir.resolve(newName);
        safeMoveWithRetry(original, target, true);
        fileLog.setFilenameNew(newName);
        processedFileRepository.save(fileLog);
        log.info("Moved file to: {}", target);
    }

    private void safeCopyWithRetry(Path from, Path to) throws IOException {
        IOException last = null;
        for (int i = 0; i < IO_RETRIES; i++) {
            try { Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING); return; }
            catch (IOException ex) { last = ex; sleep(IO_SLEEP_MS * (i + 1)); }
        }
        throw (last != null ? last : new IOException("Copy failed for " + from + " -> " + to));
    }

    private void safeMoveWithRetry(Path from, Path to, boolean copyFallback) throws IOException {
        IOException last = null;
        for (int i = 0; i < IO_RETRIES; i++) {
            try {
                try {
                    Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
                }
                return;
            } catch (IOException ex) {
                last = ex; sleep(IO_SLEEP_MS * (i + 1));
            }
        }
        if (copyFallback) {
            safeCopyWithRetry(from, to);
            IOException lastDel = null;
            for (int j = 0; j < IO_RETRIES; j++) {
                try { Files.deleteIfExists(from); return; }
                catch (IOException delEx) { lastDel = delEx; sleep(IO_SLEEP_MS * (j + 1)); }
            }
            throw (lastDel != null ? lastDel : last);
        } else {
            throw (last != null ? last : new IOException("Move failed for " + from + " -> " + to));
        }
    }

    /* ================= Charset & CSV helpers ================= */

    private Charset pickCharset(Path file) throws IOException {
        Charset[] candidates = new Charset[] {
                StandardCharsets.UTF_8,
                StandardCharsets.UTF_16LE,
                StandardCharsets.UTF_16BE,
                Charset.forName("windows-1252"),
                StandardCharsets.ISO_8859_1
        };
        for (Charset cs : candidates) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(file), cs))) {
                for (int i = 0; i < 3; i++) { if (br.readLine() == null) break; }
                return cs;
            } catch (MalformedInputException mie) { /* try next */ }
        }
        return StandardCharsets.UTF_8;
    }

    private char detectSeparator(Path file, Charset cs) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(file), cs))) {
            String line;
            while ((line = br.readLine()) != null) {
                String l = stripBom(line.trim());
                if (l.isEmpty()) continue;
                char[] candidates = new char[]{',', ';', '\t', '|'};
                int bestCount = -1; char best = ',';
                for (char c : candidates) {
                    int cnt = count(l, c);
                    if (cnt > bestCount) { bestCount = cnt; best = c; }
                }
                return best;
            }
        }
        return ',';
    }

    private int count(String s, char c) { int n=0; for (int i=0;i<s.length();i++) if (s.charAt(i)==c) n++; return n; }

    private static String stripBom(String s) {
        return (s != null && !s.isEmpty() && s.charAt(0) == '\uFEFF') ? s.substring(1) : s;
    }

    private static void stripBomInPlace(String[] row) {
        if (row != null && row.length > 0 && row[0] != null && !row[0].isEmpty() && row[0].charAt(0) == '\uFEFF') {
            row[0] = row[0].substring(1);
        }
    }

    private RowData mapRowVodacom(String[] f, Long spId, Timestamp nowTs) {
        RowData r = new RowData();
        r.msisdn              = idx(f, 0);
        r.registrationDateStr = idx(f, 1); // keep as String
        r.firstName           = idx(f, 2);
        r.middleName          = idx(f, 3);
        r.lastName            = idx(f, 4);
        r.gender              = idx(f, 5);
        r.birthDateStr        = idx(f, 6);
        r.birthPlace          = idx(f, 7);
        r.address             = join(" ", idx(f,8), idx(f,9), idx(f,10), idx(f,11), idx(f,12));
        r.alt1                = idx(f,13);
        r.alt2                = idx(f,14);
        r.idType              = idx(f,15);
        r.idNumber            = idx(f,16);
        r.vodacomTransactionId= idx(f,17);
        r.createdOnTs         =  idx(f, 1);;
        r.serviceProviderId   = spId;
        return r;
    }

    private RowData mapRowAirtel(String[] f, Long spId, Timestamp nowTs) {
        Date date = new Date();



        RowData r = new RowData();
        r.msisdn              = idx(f, 1);
        r.firstName           = idx(f, 2);
        r.middleName          = idx(f, 3);
        r.lastName            = idx(f, 4);
        r.gender              = idx(f, 5);
        r.birthDateStr        = idx(f, 6);
        r.birthPlace          = idx(f, 7);
        r.address             = idx(f, 13);
        r.alt1                = idx(f,16);
        r.alt2                = idx(f,17);
        r.idType              = idx(f,11);
        r.idNumber            = idx(f,14);
        r.registrationDateStr = idx(f, 19);;
        r.createdOnTs         =  idx(f, 19);
        r.serviceProviderId   = spId;
        r.airtelTransactionId=  idx(f, 0);
        System.out.println("registrationDateStr is "+r.registrationDateStr);
        return r;
    }

    private RowData mapRowOrange(String[] f, Long spId, Timestamp nowTs) {
        RowData r = new RowData();
        r.msisdn              = idx(f, 0);
        r.registrationDateStr = idx(f, 1);
        r.firstName           = idx(f, 3);
        r.lastName            = idx(f, 2);
        r.gender              = idx(f, 4);
        r.address             = idx(f, 7);
        r.createdOnTs         =  idx(f, 1);
        r.birthDateStr        = idx(f, 5);
        r.birthPlace          = idx(f, 6);
        r.idType =  idx(f, 8);
        r.idNumber =  idx(f, 9);
        r.serviceProviderId   = spId;
        return r;
    }

    private RowData mapRowAfricell(String[] f, Long spId, Timestamp nowTs) {

        RowData r = new RowData();
        r.msisdn              = idx(f, 0);
        r.firstName           = idx(f, 1);
        r.lastName            = idx(f, 2);
        r.address             = idx(f, 3);
        r.registrationDateStr = idx(f, 4);;
        r.birthDateStr        = idx(f, 5);
        r.serviceProviderId   = spId;
        return r;
    }

    private String idx(String[] a, int i) {
        if (a == null || i < 0 || i >= a.length) return null;
        String t = a[i];
        if (t == null) return null;
        t = t.trim();
        return t.isEmpty() ? null : t;
    }

    private String getServiceProviderName(Long spId) {
        return serviceProviderRepository.findById(spId)
                .map(ServiceProvider::getName)
                .orElse("UNKNOWN");
    }


    private String join(String sep, String... parts) {
        StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            if (p != null && !p.isEmpty()) {
                if (sb.length() > 0) sb.append(sep);
                sb.append(p);
            }
        }
        return sb.toString();
    }

    private void sleep(long ms) { try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); } }

    public static final class RowData {
        public String msisdn;
        public String registrationDateStr;
        public String firstName;
        public String middleName;
        public String lastName;
        public String gender;
        public String birthDateStr;
        public String birthPlace;
        public String address;
        public String alt1;
        public String alt2;
        public String idType;
        public String idNumber;
        public String createdOnTs;
        public Long serviceProviderId;

        // transaction IDs (provider-specific)
        public String vodacomTransactionId;
        public String airtelTransactionId;




    }

    private String computeSignature(FileProcessingService.RowData r) {
        StringBuilder base = new StringBuilder();

        // Core identifiers
        base.append(safe(r.msisdn, "NO_MSISDN")).append("|")
                .append(safe(r.firstName, "NO_FNAME")).append("|")
                .append(safe(r.lastName, "NO_LNAME")).append("|")
                .append(safe(r.birthDateStr, "NO_DOB")).append("|")
                .append(safe(String.valueOf(r.serviceProviderId), "NO_SP"));

        // Always append IDs (with placeholders if null)
        base.append("|").append(safe(r.idNumber, "NO_ID"));
        base.append("|").append(safe(r.idType, "NO_TYPE"));

        return DigestUtils.sha256Hex(base.toString());
    }

    private String safe(String value, String placeholder) {
        return (value == null || value.trim().isEmpty()) ? placeholder : value.trim();
    }



    private String safe(String s) {
        return (s == null ? "" : s.trim().toUpperCase());
    }

    private boolean isEmpty(String s) { return s == null || s.trim().isEmpty(); }
    private boolean notEmpty(String s) { return !isEmpty(s); }
    private String normalize(String s) { return (s == null ? null : s.trim().toUpperCase()); }


}
