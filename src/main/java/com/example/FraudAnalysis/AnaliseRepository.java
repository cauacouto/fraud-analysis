package com.example.FraudAnalysis;

import com.example.FraudAnalysis.domin.FraudAnalytic;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface AnaliseRepository extends MongoRepository<FraudAnalytic, String> {
    Optional<FraudAnalytic> findByIdTransferecia(String idTransferecia);
}
