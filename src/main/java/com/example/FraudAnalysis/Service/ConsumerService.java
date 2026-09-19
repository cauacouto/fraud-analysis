package com.example.FraudAnalysis.Service;

import com.account.service.TransferEvent;
import com.example.FraudAnalysis.Enums.StatusMotivo;
import com.example.FraudAnalysis.Enums.StatusTransfer;
import com.example.FraudAnalysis.domin.FraudAnalytic;
import com.example.FraudAnalysis.dto.ResultAnalitic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
public class ConsumerService {

    private static final BigDecimal LIMITE_TRANSFERENCIA =
            new BigDecimal("5000.00");

@KafkaListener(
        topics = "transfer.created",
        groupId = "fraud-service-gorup",
        containerFactory = "kafkaListenerContainerFactory"
)

    public void consumer(TransferEvent event){

    ResultAnalitic analitic = verificaTransferencia (
            event,
            LIMITE_TRANSFERENCIA
    );

    log.info("Transferência {} recebida para análise",
            event.getIdTransferencia()
    );
}





 private ResultAnalitic verificaTransferencia(TransferEvent transferEvent, BigDecimal limiteTraferecia){

   if (transferEvent.getValor().compareTo(limiteTraferecia) > 0){
       return  new ResultAnalitic(
               StatusTransfer.REJEITADO,
               StatusMotivo.VALOR_ACIMA_DO_LIMITE
       );
   }
   return new ResultAnalitic(
           StatusTransfer.APROVADO,
           null
   );

 }
}
