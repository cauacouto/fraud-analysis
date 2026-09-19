package com.example.FraudAnalysis;

import com.example.FraudAnalysis.domin.FraudAnalytic;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.UUID;

public interface AnaliseRepository extends MongoRepository<UUID, FraudAnalytic> {
}
