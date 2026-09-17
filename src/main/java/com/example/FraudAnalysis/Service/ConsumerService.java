package com.example.FraudAnalysis.Service;

import com.account.service.TransferEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerService {


@KafkaListener(
        topics = "transfer.created",
        groupId = "fraud-service-gorup",
        containerFactory = "kafkaListenerContainerFactory"
)

    public void consumer(TransferEvent event){
    log.info("transfericia recebida para analise ={}",event.getIdTransferencia());
}
}
