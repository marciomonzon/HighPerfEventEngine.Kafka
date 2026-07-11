# HighPerfEventEngine.Kafka

Este repositório demonstra um fluxo simples de eventos com .NET, Kafka e Redis. O objetivo é mostrar como um produtor envia mensagens para um tópico do Kafka e como um consumidor processa essas mensagens.

## Visão geral

O projeto é composto por:

- Producer.Api: API responsável por publicar eventos no Kafka.
- Consumer.Worker: worker que consome as mensagens do Kafka e processa os eventos.
- Shared: biblioteca compartilhada com contratos e constantes reutilizadas pelos projetos.

## Componentes

### Producer.Api
A API expõe um endpoint para receber requisições de criação de pedidos e publicar um evento no Kafka.

Principais responsabilidades:
- receber uma requisição de criação de pedido;
- montar o evento correspondente;
- publicar a mensagem em um tópico do Kafka.

### Consumer.Worker
O worker fica ouvindo as mensagens do Kafka e executa o processamento dos eventos recebidos.

Principais responsabilidades:
- consumir mensagens do tópico configurado;
- interpretar o payload do evento;
- aplicar a lógica de negócio ou persistência relacionada ao evento.
- tentar novamente o processamento em caso de falha temporária, com mecanismo de retry;
- encaminhar mensagens que não conseguem ser processadas após várias tentativas para um fluxo de dead letter.

### Kafka
O Kafka é utilizado como broker de mensagens assíncronas entre o produtor e o consumidor.

Ele permite:
- desacoplar a produção e o consumo de eventos;
- melhorar a escalabilidade da comunicação;
- oferecer um pipeline baseado em tópicos.

### Redis
O Redis é utilizado para suportar a idempotência no processamento das mensagens. Isso significa que, mesmo que a mesma mensagem seja entregue mais de uma vez, o sistema consegue identificar e evitar reprocessamentos duplicados.

Esse comportamento é importante para garantir consistência em cenários de retry, reconexão ou reenvio de mensagens.

### Observabilidade com OpenTelemetry e Jaeger
O projeto também inclui observabilidade baseada em OpenTelemetry. Os serviços geram spans de tracing para rastrear o fluxo do pedido, desde a requisição HTTP até a publicação e o processamento na fila.

Os traces são exportados para o Jaeger, que pode ser visualizado na interface web após subir os containers.

## Serviços do Docker Compose
O arquivo docker-compose.yml sobe os seguintes serviços:

- kafka: broker do Kafka.
- kafka-ui: interface web para visualizar tópicos e mensagens do Kafka.
- redis: instância local do Redis.
- jaeger: backend de tracing com UI para visualizar os spans do OpenTelemetry.

## Como testar

### 1. Subir os serviços necessários
Use o Docker Compose disponibilizado na raiz do projeto para iniciar o Kafka e demais dependências.

```bash
docker compose up -d
```

### 2. Executar o produtor
Em um terminal, rode:

```bash
dotnet run --project src/Producer.Api
```

### 3. Executar o consumidor
Em outro terminal, rode:

```bash
dotnet run --project src/Consumer.Worker
```

### 4. Enviar uma mensagem de teste
Acesse a API e envie uma requisição para o endpoint de criação de pedido em http://localhost:5010/orders, conforme o contrato definido em [src/Producer.Api/Contracts/CreateOrderRequest.cs](src/Producer.Api/Contracts/CreateOrderRequest.cs).

Exemplo de payload:

```json
{
  "orderId": "1cb45a19-0eb6-496d-9014-cd658d5e68a3",
  "customerId": "11111111-1111-1111-1111-111111111111",
  "amount": 299.90
}
```

### 5. Verificar o fluxo
- o produtor publica a mensagem no Kafka;
- o consumidor recebe a mensagem;
- os logs dos projetos mostram o processamento do evento;
- no Jaeger, é possível verificar os traces da requisição e do processamento.

### 6. Acessar a observabilidade
- Jaeger UI: http://localhost:16686
- Kafka UI: http://localhost:8080

## Observação importante

Este projeto é uma demonstração de arquitetura e integração com Kafka/Redis. Ele não foi pensado para uso em produção sem revisão adicional de:
- segurança;
- tolerância a falhas;
- observabilidade;
- persistência e recuperação de mensagens;
- configuração de ambientes e credenciais.

Use apenas como base para aprendizado, testes locais ou validação de ideias.
