# Fraud Analysis

Microsserviço responsável pela análise de transferências bancárias com o objetivo de identificar operações potencialmente suspeitas.

Este serviço faz parte de uma arquitetura distribuída composta por diferentes microsserviços, utilizando **Apache Kafka** para comunicação assíncrona.

---

## 🎯 Objetivo

O `fraud-analysis` recebe eventos de transferências realizadas pelo `account-service` e realiza uma análise para determinar se a operação deve ser aprovada ou rejeitada.

Fluxo de análise:

```text
Account Service
      │
      │ transfer.analysis.requested
      ▼
    Kafka
      │
      ▼
Fraud Analysis
      │
      │ fraud.analysis.decision
      ▼
┌───────────────┐
│               │
▼               ▼
APROVADO     REJEITADO
```

---

## 🚧 Status

🟡 **Em desenvolvimento**

O microsserviço consome solicitações de análise, persiste uma decisão por transferência no MongoDB e publica o resultado para o `account-service`. A regra atual rejeita transferências acima do máximo configurado da mesma conta de origem no mesmo segundo UTC.

---

## 🛠️ Tecnologias

- Java 21
- Spring Boot
- Spring Data MongoDB
- Spring Kafka
- Apache Kafka
- Apache Avro
- Confluent Schema Registry
- Maven
- Docker

---

## 📡 Kafka

O serviço consome solicitações do tópico configurável `fraud.kafka.topic.request` (padrão `transfer.analysis.requested`) e publica decisões no tópico `fraud.kafka.topic.decision` (padrão `fraud.analysis.decision`). A chave dos dois eventos é `idTransferencia`.

O evento recebido contém as informações necessárias para análise da transferência:

| Campo             | Descrição                         |
|-------------------|-----------------------------------|
| `idTransferencia` | Identificador único da transferência |
| `idOrigem`        | Conta de origem                   |
| `idDestino`       | Conta de destino                  |
| `formaPagamento`  | Forma de pagamento (ex: PIX)      |
| `valor`           | Valor da transferência            |
| `realizadaEm`     | Data e hora da operação           |

O schema da solicitação fica em `src/main/avro/TranferEvent.avsc`. A resposta segue `src/main/avro/FraudDecisionEvent.avsc` e contém `idTransferencia`, `status` (`APROVADA` ou `REJEITADA`), `motivo` e `analisadaEm`. O ID da transferência é distinto dos IDs de conta em `idOrigem` e `idDestino`.

O grupo consumidor padrão é `fraud-analysis-group`. Após três retries, falhas de processamento são encaminhadas ao tópico `fraud.analysis.decision.DLT`. Esses valores podem ser sobrescritos por propriedades Spring correspondentes.

---

## 👥 Consumer Group

O serviço utiliza um Consumer Group próprio:

```
fraud-service-group
```

A utilização de um grupo independente permite que o `fraud-analysis` consuma os eventos de transferência sem interferir em outros consumidores do sistema.

---

## 🗄️ Banco de dados

O serviço utiliza **MongoDB** para armazenar as análises realizadas, com seu próprio banco de dados independente.

```text
Fraud Analysis
      │
      ▼
   MongoDB
      │
      ▼
 analysis_db
```

Uma análise possui as seguintes informações:

| Campo             | Descrição                     |
|-------------------|-------------------------------|
| `id`              | Identificador da análise      |
| `idTransferencia` | Referência à transferência    |
| `idOrigem`        | Conta de origem               |
| `idDestino`       | Conta de destino              |
| `valor`           | Valor analisado               |
| `statusTransfer`  | Decisão interna (`APROVADO` ou `REJEITADO`) |
| `motivo`          | Motivo da decisão             |
| `realizadaEm`     | Horário informado na solicitação |
| `analisadaEm`     | Horário em que a decisão foi criada |

---

## 🔄 Fluxo de decisão

```text
             ┌──────────────────┐
             │  Account Service │
             └────────┬─────────┘
                      │
                      │ transfer.analysis.requested
                      ▼
             ┌──────────────────┐
             │      Kafka       │
             └────────┬─────────┘
                      │
                      ▼
             ┌──────────────────┐
             │  Fraud Analysis  │
             │                  │
             │ Análise de risco │
             └────────┬─────────┘
                      │ fraud.analysis.decision
                      ▼
             ┌──────────────────┐
             │  Account Service │
             │ confirma/rejeita │
             └──────────────────┘
```

O `account-service` mantém a transferência pendente até receber a decisão correlacionada pelo `idTransferencia`.

---

## ▶️ Executando localmente

### Pré-requisitos

- Java 21
- Docker
- Docker Compose
- Apache Kafka
- Confluent Schema Registry
- MongoDB

### Compilação

```bash
./mvnw clean package -DskipTests
```

### Executando com Maven

```bash
./mvnw spring-boot:run
```

O serviço é executado na porta:

```
8082
```

### Dependências de infraestrutura

```
Kafka              → localhost:19092
Schema Registry    → localhost:8083
MongoDB            → localhost:27017
Fraud Analysis     → localhost:8082
```

---

## 🔐 Análise de fraude

O máximo permitido é configurado por `fraud.analysis.max-transfers-per-second` (padrão `1`). Quando o total da mesma origem no mesmo segundo UTC ultrapassa esse máximo, a operação excedente é rejeitada com o motivo `FREQUENCIA_DE_TRANSFERENCIAS_ACIMA_DO_LIMITE`. Cada solicitação é registrada uma única vez por `idTransferencia` numa coleção de contagem no MongoDB, evitando que reentregas incrementem a frequência e coordenando instâncias concorrentes. Os registros de contagem expiram após um dia. Os buckets são segundos fixos UTC; operações que atravessam a virada do segundo ficam em buckets diferentes. Regras futuras podem incluir:

- Valor da transferência
- Histórico de operações
- Origem e destino
- Forma de pagamento
- Outros padrões considerados suspeitos

As regras serão implementadas de forma independente do `account-service`.

---

## 🎯 Objetivo do projeto

Projeto desenvolvido para estudo prático de arquitetura de microsserviços e comunicação orientada a eventos, explorando conceitos como:

- Event-driven architecture
- Comunicação assíncrona
- Apache Kafka
- Consumer Groups
- Partitions
- Avro
- Schema Registry
- Separação de responsabilidades entre microsserviços
- Persistência independente por serviço
- Análise de eventos
