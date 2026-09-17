package com.example.FraudAnalysis.domin;

import com.example.FraudAnalysis.Enums.StatusTransfer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;

@Document(collation = "Analysis")
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
}
