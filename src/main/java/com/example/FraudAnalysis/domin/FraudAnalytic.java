package com.example.FraudAnalysis.domin;

import com.example.FraudAnalysis.Enums.StatusTransfer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

@Document(collection = "Analysis")
@CompoundIndex(name = "idx_origem_realizada_em", def = "{'idOrigem': 1, 'realizadaEm': 1}")
@Getter
@Setter
public class FraudAnalytic {

    @Id()
    private String id;
    @Indexed(unique = true)
    private String idTransferecia;
    private String idOrigem;
    private String idDestino;
    private BigDecimal valor;
    private StatusTransfer statusTransfer;
    private String motivo;
    private Instant realizadaEm;
    private Instant analisadaEm;
}
