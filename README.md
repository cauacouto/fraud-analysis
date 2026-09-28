# Fraud Analysis

Microsserviço Java que recebe solicitações de análise de transferências pelo Apache Kafka, aplica uma regra de frequência por conta de origem e publica uma decisão de aprovação ou rejeição. As análises e os eventos usados no controle de frequência são armazenados no MongoDB.

> Projeto em desenvolvimento, criado para estudo de arquitetura de microsserviços e comunicação orientada a eventos.

## Fluxo

```text
account-service ── transfer.analysis.requested ──▶ fraud-analysis
                                                     │
                                      MongoDB: análise e frequência
                                                     │
account-service ◀──── fraud.analysis.decision ───────┘
```

O serviço usa `idTransferencia` como chave Kafka e identificador de correlação. A mesma solicitação pode ser reprocessada: se já houver uma análise para o ID, os dados principais são validados e a decisão existente é publicada novamente.

## Regra de análise atual

O limite é configurado por `fraud.analysis.max-transfers-per-second` (padrão `1`). O serviço conta as transferências da mesma origem no segundo UTC de `realizadaEm`. Até o limite, aprova; as seguintes são rejeitadas com `FREQUENCIA_DE_TRANSFERENCIAS_ACIMA_DO_LIMITE`.

Cada transferência é registrada uma única vez na coleção `FraudRateEvents`, com unicidade pelo ID da transferência, para evitar que uma reentrega aumente a contagem. Esses registros expiram após um dia. As decisões são salvas na coleção `Analysis`.

## Contratos Kafka

Os schemas Avro ficam em `src/main/avro/` e as classes são geradas durante a compilação Maven.

| Direção | Tópico padrão | Schema |
|---|---|---|
| Entrada | `transfer.analysis.requested` | `TranferEvent.avsc` |
| Saída | `fraud.analysis.decision` | `FraudDecisionEvent.avsc` |
| Mensagens que falharam após retries | `fraud.analysis.decision.DLT` | — |

A solicitação inclui `idTransferencia`, `idOrigem`, `idDestino`, `formaPagamento`, `valor` e `realizadaEm`. A decisão contém `idTransferencia`, `status` (`APROVADA` ou `REJEITADA`), `motivo` e `analisadaEm`. Os eventos usam `idTransferencia` como chave; IDs de origem e destino representam as contas.

O consumer group padrão é `fraud-analysis-group`. Falhas de processamento recebem três tentativas adicionais, com intervalo de um segundo, antes de serem encaminhadas à DLT.

## Tecnologias

- Java 21
- Spring Boot 4.1.1
- Spring Kafka e Apache Kafka
- Apache Avro e Confluent Schema Registry
- Spring Data MongoDB e MongoDB
- Maven

## Pré-requisitos

- JDK 21
- Docker ou serviços locais para Kafka, Schema Registry e MongoDB
- Kafka acessível em `localhost:19092`
- Schema Registry acessível em `http://localhost:8083`
- MongoDB acessível em `localhost:27017`

O projeto não inclui um arquivo Docker Compose; inicie essas dependências separadamente ou ajuste as configurações da aplicação.

## Executar

Na raiz do repositório:

```bash
./mvnw clean package
./mvnw spring-boot:run
```

No Windows, use `mvnw.cmd` no lugar de `./mvnw`. A aplicação inicia na porta `8082`.

Para executar os testes:

```bash
./mvnw test
```

## Configuração

As configurações padrão estão em `src/main/resources/application.properties`:

| Propriedade | Padrão | Uso |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `localhost:19092` | Endereço do Kafka |
| `spring.kafka.properties.schema.registry.url` | `http://localhost:8083` | Schema Registry |
| `spring.kafka.consumer.group-id` | `fraud-analysis-group` | Consumer group |
| `spring.mongodb.uri` | `mongodb://localhost:27017/analysis_db` | MongoDB e banco |
| `fraud.kafka.topic.request` | `transfer.analysis.requested` | Tópico de solicitações |
| `fraud.kafka.topic.decision` | `fraud.analysis.decision` | Tópico de decisões |
| `fraud.kafka.topic.decision-dlt` | `fraud.analysis.decision.DLT` | Dead-letter topic |
| `fraud.analysis.max-transfers-per-second` | `1` | Máximo por origem a cada segundo UTC |
| `server.port` | `8082` | Porta HTTP |

As propriedades podem ser substituídas pelas variáveis de ambiente correspondentes do Spring Boot, por exemplo `FRAUD_ANALYSIS_MAX_TRANSFERS_PER_SECOND=3`.

## Estrutura do projeto

```text
src/main/avro/                 Schemas dos eventos Kafka
src/main/java/.../Service/     Consumo, análise e publicação de decisões
src/main/java/.../config/      Configuração Kafka, Avro, retries e DLT
src/main/java/.../domin/       Documentos MongoDB
src/main/resources/            Configuração da aplicação
```

## Escopo atual e próximos passos

A regra implementada avalia frequência por conta de origem. Critérios como valor, histórico, destino ou forma de pagamento ainda não fazem parte da decisão. O documento `SPEC_TRANSFERENCIA_COM_ANALISE_DE_FRAUDE.md` descreve requisitos propostos para o fluxo integrado ao `account-service`; ele não representa funcionalidades implementadas por este repositório.
