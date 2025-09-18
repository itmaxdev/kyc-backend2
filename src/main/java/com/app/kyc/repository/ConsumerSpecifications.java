package com.app.kyc.repository;
import com.app.kyc.entity.Consumer;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;

public class ConsumerSpecifications {

    public static Specification<Consumer> withFilters(Long serviceProviderId, String search,Boolean isConsistent,List<Integer> allowedStatuses) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            
            // isConsistent filter (only if not null)
            if (isConsistent != null) {
                if (isConsistent) {
                    predicates.add(cb.isTrue(root.get("isConsistent")));
                } else {
                    predicates.add(cb.isFalse(root.get("isConsistent")));
                }
            }

            // consumerStatus in (allowedStatuses)
            if (allowedStatuses != null && !allowedStatuses.isEmpty()) {
                predicates.add(root.get("consumerStatus").in(allowedStatuses));
            }

            // serviceProvider filter
            if (serviceProviderId != null) {
                predicates.add(cb.equal(root.get("serviceProvider").get("id"), serviceProviderId));
            }

            // free-text filter
            if (search != null && !search.trim().isEmpty()) {
                String likeSearch = "%" + search.toLowerCase() + "%";
                
                // build full name = firstName + " " + middleName + " " + lastName
                javax.persistence.criteria.Expression<String> fullName =
                        cb.concat(
                            cb.concat(
                                cb.concat(cb.lower(root.get("firstName")), " "),
                                cb.concat(cb.lower(root.get("middleName")), " ")
                            ),
                            cb.lower(root.get("lastName"))
                        );

                Predicate name = cb.like(fullName, likeSearch);
                Predicate msisdn = cb.like(cb.lower(root.get("msisdn")), likeSearch);
                Predicate nationality = cb.like(cb.lower(root.get("nationality")), likeSearch);
                Predicate gender = cb.like(cb.lower(root.get("gender")), likeSearch);
                Predicate idNumber = cb.like(cb.lower(root.get("identificationNumber")), likeSearch);
                Predicate idType = cb.like(cb.lower(root.get("identificationType")), likeSearch);
                Predicate spName = cb.like(cb.lower(root.get("serviceProvider").get("name")), likeSearch);

                predicates.add(cb.or(name, msisdn, nationality, gender, idNumber, idType, spName));
            }
            
            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }
}
