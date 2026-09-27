package com.example.FraudAnalysis.Service;

import com.account.service.FraudDecisionEvent;
import com.account.service.FraudDecisionStatus;
import com.account.service.TransferEvent;
import com.example.FraudAnalysis.AnaliseRepository;
import com.example.FraudAnalysis.FraudRateEventRepository;
import com.example.FraudAnalysis.Enums.StatusMotivo;
import com.example.FraudAnalysis.Enums.StatusTransfer;
import com.example.FraudAnalysis.domin.FraudAnalytic;
import com.example.FraudAnalysis.domin.FraudRateEvent;
import com.example.FraudAnalysis.dto.ResultAnalitic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ConsumerService {

    private final AnaliseRepository analiseRepository;
    private final FraudRateEventRepository rateEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String decisionTopic;
    private final int maxTransfersPerSecond;

    public ConsumerService(
            AnaliseRepository analiseRepository,
            FraudRateEventRepository rateEventRepository,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${fraud.kafka.topic.decision}") String decisionTopic,
            @Value("${fraud.analysis.max-transfers-per-second:1}") int maxTransfersPerSecond) {
        this.analiseRepository = analiseRepository;
        this.rateEventRepository = rateEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.decisionTopic = decisionTopic;
        if (maxTransfersPerSecond < 1) {
            throw new IllegalArgumentException("fraud.analysis.max-transfers-per-second deve ser pelo menos 1");
        }
        this.maxTransfersPerSecond = maxTransfersPerSecond;
    }

    @KafkaListener(
            topics = "${fraud.kafka.topic.request}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumer(TransferEvent event) throws Exception {
        validar(event);

        FraudAnalytic analytic = analiseRepository.findByIdTransferecia(event.getIdTransferencia().toString())
                .map(existing -> validarReentrega(event, existing))
                .orElseGet(() -> analiseRepository.save(criarAnalise(event)));

        publicarDecisao(analytic);
        log.info("Análise {} publicada para transferência {}",
                analytic.getStatusTransfer(), analytic.getIdTransferecia());
    }

    private void validar(TransferEvent event) {
        if (event == null
                || event.getIdTransferencia() == null
                || event.getIdOrigem() == null
                || event.getIdDestino() == null
                || event.getValor() == null
                || event.getRealizadaEm() == null
                || event.getIdTransferencia().toString().isBlank()
                || event.getIdOrigem().toString().isBlank()
                || event.getIdDestino().toString().isBlank()) {
            throw new IllegalArgumentException("Evento de transferência contém campos obrigatórios ausentes");
        }
        if (event.getValor().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("O valor da transferência deve ser positivo");
        }
    }

    private FraudAnalytic validarReentrega(TransferEvent event, FraudAnalytic existing) {
        if (!Objects.equals(existing.getIdOrigem(), event.getIdOrigem().toString())
                || !Objects.equals(existing.getIdDestino(), event.getIdDestino().toString())
                || existing.getValor().compareTo(event.getValor()) != 0) {
            throw new IllegalArgumentException("Evento repetido contém dados diferentes para a mesma transferência");
        }
        return existing;
    }

    private FraudAnalytic criarAnalise(TransferEvent event) {
        String origem = event.getIdOrigem().toString();
        Instant realizadaEm = event.getRealizadaEm();
        registrarEventoDeTaxa(event, origem, realizadaEm);
        long transferenciasNoSegundo = rateEventRepository.countByIdOrigemAndSegundoUtc(
                origem, realizadaEm.getEpochSecond());
        ResultAnalitic result = verificaFrequencia(transferenciasNoSegundo - 1);
        FraudAnalytic analytic = new FraudAnalytic();
        analytic.setIdTransferecia(event.getIdTransferencia().toString());
        analytic.setIdOrigem(origem);
        analytic.setIdDestino(event.getIdDestino().toString());
        analytic.setValor(event.getValor());
        analytic.setStatusTransfer(result.statusTransfer());
        analytic.setMotivo(result.statusMotivo() == null ? null : result.statusMotivo().name());
        analytic.setRealizadaEm(realizadaEm);
        analytic.setAnalisadaEm(Instant.now());
        return analytic;
    }

    private void registrarEventoDeTaxa(TransferEvent event, String origem, Instant realizadaEm) {
        FraudRateEvent rateEvent = new FraudRateEvent();
        rateEvent.setIdTransferencia(event.getIdTransferencia().toString());
        rateEvent.setIdOrigem(origem);
        rateEvent.setSegundoUtc(realizadaEm.getEpochSecond());
        rateEvent.setExpiraEm(Instant.now().plus(Duration.ofDays(1)));

        try {
            rateEventRepository.insert(rateEvent);
        } catch (DuplicateKeyException duplicate) {
            FraudRateEvent existing = rateEventRepository.findById(rateEvent.getIdTransferencia())
                    .orElseThrow(() -> duplicate);
            if (!Objects.equals(existing.getIdOrigem(), origem)
                    || existing.getSegundoUtc() != rateEvent.getSegundoUtc()) {
                throw new IllegalArgumentException(
                        "Evento repetido contém dados diferentes para a mesma transferência", duplicate);
            }
        }
    }

    private void publicarDecisao(FraudAnalytic analytic) throws Exception {
        FraudDecisionEvent decision = FraudDecisionEvent.newBuilder()
                .setIdTransferencia(analytic.getIdTransferecia())
                .setStatus(analytic.getStatusTransfer() == StatusTransfer.REJEITADO
                        ? FraudDecisionStatus.REJEITADA
                        : FraudDecisionStatus.APROVADA)
                .setMotivo(analytic.getMotivo())
                .setAnalisadaEm(analytic.getAnalisadaEm())
                .build();

        kafkaTemplate.send(decisionTopic, analytic.getIdTransferecia(), decision)
                .get(10, TimeUnit.SECONDS);
    }

    private ResultAnalitic verificaFrequencia(long transferenciasAnterioresNaJanela) {
        if (transferenciasAnterioresNaJanela >= maxTransfersPerSecond) {
            return new ResultAnalitic(
                    StatusTransfer.REJEITADO,
                    StatusMotivo.FREQUENCIA_DE_TRANSFERENCIAS_ACIMA_DO_LIMITE);
        }
        return new ResultAnalitic(StatusTransfer.APROVADO, null);
    }
}
