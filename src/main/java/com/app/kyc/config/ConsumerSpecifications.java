package com.app.kyc.config;

import com.app.kyc.entity.Consumer;
import com.app.kyc.service.FileProcessingService;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;

public class ConsumerSpecifications {

    public static Specification<Consumer> matchConsumer(FileProcessingService.RowData r) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            // ✅ If MSISDN is present, ALWAYS anchor on it first
            if (notEmpty(r.msisdn)) {
                predicates.add(cb.equal(root.get("msisdn"), r.msisdn));

                // Add optional enrichment checks (to reduce false matches, but not required)
                if (notEmpty(r.idNumber)) {
                    predicates.add(cb.equal(root.get("identificationNumber"), r.idNumber));
                }
                if (notEmpty(r.idType)) {
                    predicates.add(cb.equal(root.get("identificationType"), r.idType));
                }

                return cb.and(predicates.toArray(new Predicate[0]));
            }

            // 🔹 If MSISDN missing, fallback to core identity
            if (notEmpty(r.firstName) && notEmpty(r.lastName) && notEmpty(r.idNumber) && notEmpty(r.idType)) {
                predicates.add(cb.equal(root.get("firstName"), r.firstName));
                predicates.add(cb.equal(root.get("lastName"), r.lastName));
                predicates.add(cb.equal(root.get("identificationNumber"), r.idNumber));
                predicates.add(cb.equal(root.get("identificationType"), r.idType));
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    private static boolean notEmpty(String s) {
        return s != null && !s.trim().isEmpty();
    }

    private static boolean isEmpty(String s) {
        return !notEmpty(s);
    }
}

