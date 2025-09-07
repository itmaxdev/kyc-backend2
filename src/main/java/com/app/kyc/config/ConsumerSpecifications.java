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

            // 1. Try idNumber + idType
            if (notEmpty(r.idNumber) && notEmpty(r.idType)) {
                predicates.add(cb.equal(root.get("identificationNumber"), r.idNumber));
                predicates.add(cb.equal(root.get("identificationType"), r.idType));
                return cb.and(predicates.toArray(new Predicate[0]));
            }

            // 2. Fallback: msisdn
            if (notEmpty(r.msisdn)) {
                predicates.add(cb.equal(root.get("msisdn"), r.msisdn));
                return cb.and(predicates.toArray(new Predicate[0]));
            }

            // 3. Nothing matchable
            return cb.disjunction();
        };
    }


    private static boolean notEmpty(String s) {
        return s != null && !s.trim().isEmpty();
    }
}





