package com.example.FraudAnalysis.domin;

import com.example.FraudAnalysis.Enums.StatusTransfer;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;

@Document(collation = "Analysis")
public class FraudAnalytic {

    @Id
    private String id;
    private String idTransferecia;
    private String idOrigem;
    private String idDestino;
    private BigDecimal valor;
    private StatusTransfer statusTransfer;
    private String motivo;
}
