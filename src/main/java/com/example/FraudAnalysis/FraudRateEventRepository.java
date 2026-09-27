package com.example.FraudAnalysis;

import com.example.FraudAnalysis.domin.FraudRateEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface FraudRateEventRepository extends MongoRepository<FraudRateEvent, String> {
    long countByIdOrigemAndSegundoUtc(String idOrigem, long segundoUtc);
}
