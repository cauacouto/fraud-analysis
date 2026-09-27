package com.example.FraudAnalysis.domin;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document(collection = "FraudRateEvents")
@CompoundIndex(name = "idx_rate_origin_second", def = "{'idOrigem': 1, 'segundoUtc': 1}")
@Getter
@Setter
public class FraudRateEvent {

    @Id
    private String idTransferencia;
    private String idOrigem;
    private long segundoUtc;

    @Indexed(expireAfter = "0s")
    private Instant expiraEm;
}
