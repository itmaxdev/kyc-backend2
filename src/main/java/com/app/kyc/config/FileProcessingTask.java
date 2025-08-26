package com.app.kyc.config;

import com.app.kyc.service.FileProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Slf4j
@Component
@RequiredArgsConstructor
public class FileProcessingTask {

    private final FileProcessingService fileProcessingService;

    /** Comma-separated list of directories to scan (as you have in application.properties) */
    @Value("#{'${local.file.paths}'.split(',')}")
    private List<String> localFilePaths;

    /** Only CSVs; ignore temp markers like .filepart/.done/.processing (override if needed) */
    @Value("${local.file.acceptPattern:(?i)^(?!.*\\.(?:filepart|done|processing)$).*\\.csv$}")
    private String acceptPattern;

    /** Cron configurable; default every minute */
    @Value("${local.file.cron:0 */1 * * * *}")
    private String cron; // not used in code, just documented

    private final AtomicBoolean running = new AtomicBoolean(false);

    @Scheduled(cron = "${local.file.cron:0 */1 * * * *}")
    public void processLocalFiles() {
        if (!running.compareAndSet(false, true)) {
            log.info("Previous scan still running; skipping this tick");
            return;
        }

        final Pattern pattern = Pattern.compile(acceptPattern);

        try {
            log.info("Scan start {}", LocalDateTime.now());

            for (String rawPath : localFilePaths) {
                if (rawPath == null || rawPath.isBlank()) continue;

                Path base = Paths.get(rawPath.trim());
                log.debug("Scanning: {}", base);

                if (Files.notExists(base)) {
                    log.warn("Path does not exist: {}", base);
                    continue;
                }

                if (Files.isDirectory(base)) {
                    try (Stream<Path> entries = Files.list(base)) {
                        entries.filter(Files::isRegularFile)
                                .filter(p -> pattern.matcher(p.getFileName().toString()).matches())
                                .sorted()
                                .forEach(this::routeByParentFolder);
                    }
                } else if (Files.isRegularFile(base)) {
                    if (pattern.matcher(base.getFileName().toString()).matches()) {
                        routeByParentFolder(base);
                    } else {
                        log.debug("Skipping non-matching file: {}", base);
                    }
                } else {
                    log.debug("Skipping non-regular path: {}", base);
                }
            }

        } catch (Exception ex) {
            log.error("File scan failed: {}", ex.toString(), ex);
        } finally {
            running.set(false);
        }
    }

    /** Decide operator by directory name (case-insensitive), not by filename */
    private void routeByParentFolder(Path file) {
        String operator = operatorFromPath(file);
        log.info("Found file: {} | Operator: {}", file, operator);

        try {
            switch (operator) {
                case "Vodacom":
                    fileProcessingService.processFileVodacom(file, operator);
                    break;
                case "Airtel":
                    fileProcessingService.processFileAirtel(file, operator);
                    break;
                case "Orange":
                    fileProcessingService.processFileOrange(file, operator);
                    break;
                case "Africell":
                    // make sure this exists; if not, add it or map to a generic handler
                    fileProcessingService.processFileAfricell(file, operator);
                    break;
                default:
                    log.warn("Unknown operator (path did not include vodacom/airtel/orange/africell) for file {}. Skipping.", file);
            }
        } catch (IOException ioe) {
            log.error("I/O error while processing {}: {}", file, ioe.toString(), ioe);
        } catch (Exception ex) {
            log.error("Processing failed for {}: {}", file, ex.toString(), ex);
        }
    }

    private static String operatorFromPath(Path file) {
        Path parent = file.getParent();
        // Walk up a couple of levels to find a folder name that contains the operator
        for (int i = 0; i < 4 && parent != null; i++, parent = parent.getParent()) {
            String seg = parent.getFileName() != null ? parent.getFileName().toString().toLowerCase(Locale.ROOT) : "";
            if (seg.contains("vodacom"))  return "Vodacom";
            if (seg.contains("airtel"))   return "Airtel";
            if (seg.contains("orange"))   return "Orange";
            if (seg.contains("africell")) return "Africell";
        }
        return "Unknown";
    }
}
