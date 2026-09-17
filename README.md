# Fraud Analysis

Microsserviço responsável pela análise de transferências bancárias com o objetivo de identificar operações potencialmente suspeitas.

Este serviço faz parte de uma arquitetura distribuída composta por diferentes microsserviços, utilizando **Apache Kafka** para comunicação assíncrona.

---

## 🎯 Objetivo

O `fraud-analysis` recebe eventos de transferências realizadas pelo `account-service` e realiza uma análise para determinar se a operação deve ser aprovada ou rejeitada.

Fluxo planejado:

```text
Account Service
      │
      │ transfer.created
      ▼
    Kafka
      │
      ▼
Fraud Analysis
      │
      │ análise
      ▼
┌───────────────┐
│               │
▼               ▼
APROVADO     REJEITADO
```

---

## 🚧 Status

🟡 **Em desenvolvimento**

O microsserviço já possui a estrutura inicial para consumo dos eventos de transferência através do Kafka.
A implementação das regras de análise de fraude ainda está em construção.

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

O serviço atua como **Consumer** do tópico:

```
transfer.created
```

O evento recebido contém as informações necessárias para análise da transferência:

| Campo             | Descrição                         |
|-------------------|-----------------------------------|
| `idTransferencia` | Identificador único da transferência |
| `idOrigem`        | Conta de origem                   |
| `idDestino`       | Conta de destino                  |
| `formaPagamento`  | Forma de pagamento (ex: PIX)      |
| `valor`           | Valor da transferência            |
| `realizadaEm`     | Data e hora da operação           |

A chave utilizada no Kafka é o identificador da transferência (`idTransferencia`), o que permite manter os eventos relacionados à mesma transferência na mesma partição.

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
| `status`          | `APROVADO` ou `REJEITADO`     |
| `motivo`          | Motivo da decisão             |

---

## 🔄 Fluxo planejado

```text
             ┌──────────────────┐
             │  Account Service │
             └────────┬─────────┘
                      │
                      │ transfer.created
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
                      │
                 ┌────┴────┐
                 ▼         ▼
             APROVADO   REJEITADO
```

Posteriormente, o resultado da análise poderá ser publicado em um novo evento (`fraud.approved` / `fraud.rejected`) para que o `account-service` possa continuar o processamento da transferência.

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

A implementação das regras de fraude está em desenvolvimento. Alguns critérios que poderão ser utilizados futuramente incluem:

- Valor da transferência
- Frequência de transferências
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
