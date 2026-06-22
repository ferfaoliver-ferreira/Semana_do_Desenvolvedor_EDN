# Dia 1 - Ingestão de Pedidos via API e EventBridge na AWS

Este laboratório documenta a construção de um fluxo inicial de ingestão de pedidos usando **Amazon API Gateway**, **AWS Lambda**, **Amazon SQS FIFO**, **Dead-Letter Queue (DLQ)**, **IAM** e **Amazon EventBridge**. A proposta foi simular a entrada de pedidos em uma API REST, realizar uma pré-validação, enfileirar os dados com garantia de ordem e, por fim, validar e publicar um evento de domínio em um barramento customizado.

## Por que este conhecimento e importante

Em cenários reais, o recebimento de pedidos costuma ser a porta de entrada para vários fluxos internos. Quando esse processo é construído de forma assíncrona e desacoplada, a aplicação ganha mais resiliência, controle operacional e capacidade de escalar sem depender de processamento síncrono ponta a ponta.

Os principais ganhos práticos deste laboratório são:

- **Desacoplamento entre entrada e processamento** com API Gateway, Lambda e SQS.
- **Garantia de ordenação** com uso de fila FIFO.
- **Tratamento de falhas** com DLQ para mensagens que excedem o número de tentativas.
- **Publicacao de eventos de negócio** com EventBridge para integrações futuras.
- **Observabilidade** via CloudWatch Logs em cada etapa do fluxo.

## Objetivo do laboratório

Construir um pipeline de ingestão de pedidos em que:

- uma **API REST** recebe os dados do pedido;
- uma **Lambda de pré-validação** valida os campos básicos e publica a mensagem na fila;
- uma **fila SQS FIFO** desacopla a entrada do processamento principal;
- uma **Lambda de validação** consome a fila, valida o payload e publica um evento no EventBridge;
- uma **DLQ FIFO** recebe mensagens com falha recorrente no consumo.

## Serviços utilizados

- Amazon API Gateway
- AWS Lambda
- Amazon SQS FIFO
- Amazon SQS Dead-Letter Queue
- AWS IAM
- Amazon EventBridge
- Amazon CloudWatch Logs

## Arquitetura implementada

![Arquitetura do laboratório](./images/arquitetura-dia-1.png)

```mermaid
flowchart LR
    A["Cliente / Requisição HTTP"] --> B["API Gateway (REST)"]
    B --> C["Lambda de Pré-Validação"]
    C --> D["SQS FIFO Pedidos"]
    D --> E["Lambda de Validação de Pedidos"]
    D --> F["SQS FIFO DLQ"]
    E --> G["Custom Event Bus"]
    G --> H["Regras futuras / novos consumidores"]
```

## Recursos criados no laboratório

- `lambda-prevalidacao-role-seu-nome`
- `lambda-validacao-pedidos-role-seu-nome`
- `pedidos-fifo-dlq-seu-nome.fifo`
- `pedidos-fifo-queue-seu-nome.fifo`
- `pre-validacao-lambda-seu-nome`
- `pedidos-api-seu-nome`
- `pedidos-event-bus-seu-nome`
- `validacao-pedidos-lambda-seu-nome`

## Fluxo funcional do Dia 1

1. O cliente envia um `POST /pedidos` para o API Gateway.
2. A Lambda `pre-validacao` recebe o payload e valida campos mínimos como `pedidoId` e `clienteId`.
3. Se o payload estiver válido, a mensagem é enviada para a fila `pedidos-fifo-queue-seu-nome.fifo`.
4. A Lambda `validacao-pedidos` é acionada pelo trigger da fila SQS.
5. A função realiza validações adicionais e publica o evento `NovoPedidoValidado` no EventBridge.
6. Se houver falhas recorrentes na etapa de consumo, a mensagem segue para a DLQ configurada.

## Passo a passo técnico

### 1. Criar as roles IAM das Lambdas

Criar duas roles para separar as permissões de cada etapa do fluxo:

- `lambda-prevalidacao-role-seu-nome`
- `lambda-validacao-pedidos-role-seu-nome`

Ambas com a política gerenciada:

- `AWSLambdaBasicExecutionRole`

Depois, complementar com inline policies específicas:

- `sqs:SendMessage` para a role da pre-validacao
- `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` e `events:PutEvents` para a role da validacao

### 2. Criar a DLQ FIFO e a fila principal

Criar primeiro a fila de mensagens mortas:

- `pedidos-fifo-dlq-seu-nome.fifo`

Depois criar a fila principal:

- `pedidos-fifo-queue-seu-nome.fifo`

Configuracoes relevantes:

- tipo `FIFO`
- redrive policy habilitada
- DLQ apontando para a fila morta criada anteriormente
- `Maximum receives = 3`

### 3. Atualizar a role da Lambda de pré-validação

Adicionar uma inline policy para permitir envio de mensagens para a fila principal:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:pedidos-fifo-queue-seu-nome.fifo"
    }
  ]
}
```

### 4. Criar a Lambda de pré-validação

Criar a função:

- `pre-validacao-lambda-seu-nome`

Runtime sugerido:

- `Python 3.12`

Variavel de ambiente:

- `SQS_QUEUE_URL`

Codigo utilizado:

```python
import json
import os
import boto3

SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
sqs = boto3.client("sqs")

def lambda_handler(event, context):
    print(f"Evento recebido do API Gateway: {event}")
    try:
        body_str = event.get("body", "{}")
        if not body_str:
            body_str = "{}"

        body = json.loads(body_str)
        pedido_id = body.get("pedidoId")
        cliente_id = body.get("clienteId")

        if not pedido_id or not cliente_id:
            print("Erro: pedidoId ou clienteId ausente.")
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "pedidoId e clienteId são obrigatórios"})
            }

        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(body),
            MessageGroupId=str(pedido_id),
            MessageDeduplicationId=str(context.aws_request_id)
        )

        print(f"Mensagem enviada para SQS: {response['MessageId']}")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "message": "Pedido recebido e enfileirado",
                "sqsMessageId": response["MessageId"]
            })
        }
    except json.JSONDecodeError:
        print("Erro: corpo da requisição não é um JSON válido.")
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Corpo da requisição inválido"})
        }
    except Exception as e:
        print(f"Erro inesperado: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": f"Erro interno do servidor: {str(e)}"})
        }
```

### 5. Criar a API REST no API Gateway

Criar uma API REST chamada:

- `pedidos-api-seu-nome`

Depois:

1. Criar o recurso `/pedidos`
2. Criar o metodo `POST`
3. Usar integração com `Lambda Function`
4. Habilitar `Use Lambda Proxy integration`
5. Apontar para `pre-validacao-lambda-seu-nome`
6. Fazer deploy no stage `dev`

Ao final, anotar a `Invoke URL`.

### 6. Testar a ingestão inicial com curl

Exemplo de teste:

```bash
curl -X POST https://abcdef123.execute-api.us-east-1.amazonaws.com/dev/pedidos \
  -H "Content-Type: application/json" \
  -d '{
    "pedidoId": "lab001-seu-nome",
    "clienteId": "clienteXYZ-seu-nome",
    "itens": [
      { "produto": "Caneta Azul", "quantidade": 10 },
      { "produto": "Caderno Universitario", "quantidade": 2 }
    ]
  }'
```

Resposta esperada:

```json
{
  "message": "Pedido recebido e enfileirado",
  "sqsMessageId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

### 7. Criar o custom event bus

Criar no EventBridge:

- `pedidos-event-bus-seu-nome`

Esse barramento será o destino dos eventos publicados pela Lambda de validação.

### 8. Atualizar a role da Lambda de validação

Adicionar uma inline policy com permissões de leitura na fila e publicação no EventBridge:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "COLE_AQUI_O_ARN_DA_SUA_FILA_PRINCIPAL_FIFO"
    },
    {
      "Effect": "Allow",
      "Action": "events:PutEvents",
      "Resource": "COLE_AQUI_O_ARN_DO_SEU_EVENT_BUS"
    }
  ]
}
```

### 9. Criar a Lambda de validação de pedidos

Criar a função:

- `validacao-pedidos-lambda-seu-nome`

Runtime sugerido:

- `Python 3.12`

Variavel de ambiente:

- `EVENT_BUS_NAME`

Codigo utilizado:

```python
import json
import os
import boto3
from datetime import datetime

EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]
event_bridge = boto3.client("events")

def lambda_handler(event, context):
    print(f"Evento SQS recebido: {event}")

    for record in event["Records"]:
        try:
            pedido_data_str = record["body"]
            pedido_data = json.loads(pedido_data_str)
            print(f"Processando pedido do SQS: {pedido_data}")

            if not pedido_data.get("itens") or not isinstance(pedido_data.get("itens"), list) or len(pedido_data.get("itens")) == 0:
                print(f"Erro de validação: Pedido {pedido_data.get('pedidoId')} não possui itens válidos.")
                continue

            print(f"Pedido {pedido_data.get('pedidoId')} validado com sucesso.")

            if "timestamp" not in pedido_data:
                pedido_data["timestamp"] = datetime.utcnow().isoformat() + "Z"

            response = event_bridge.put_events(
                Entries=[{
                    "Source": "lab.aula1.pedidos.validação",
                    "DetailType": "NovoPedidoValidado",
                    "Detail": json.dumps(pedido_data),
                    "EventBusName": EVENT_BUS_NAME
                }]
            )

            print(f"Evento publicado no EventBridge: {response}")

        except json.JSONDecodeError as je:
            print(f"Erro de JSON ao processar registro {record['messageId']}: {str(je)}")
            raise je
        except Exception as e:
            print(f"Erro geral ao processar registro {record['messageId']}: {str(e)}")
            raise e

    return {"statusCode": 200, "body": "Processamento de mensagens SQS concluido"}
```

### 10. Configurar o trigger SQS da Lambda de validação

Na Lambda `validacao-pedidos-lambda-seu-nome`, adicionar um trigger:

- fonte `SQS`
- fila `pedidos-fifo-queue-seu-nome.fifo`
- `Batch size = 1`

Esse ajuste e importante para respeitar o comportamento FIFO e facilitar a leitura dos logs.

### 11. Testar o fluxo completo do Dia 1

Enviar um novo pedido para a API:

```bash
curl -X POST <INVOKE_URL>/pedidos \
  -H "Content-Type: application/json" \
  -d '{
    "pedidoId": "lab001-seu-nome",
    "clienteId": "clienteABC-seu-nome",
    "itens": [
      { "produto": "Borracha Branca", "quantidade": 5 }
    ]
  }'
```

Validações esperadas:

- a Lambda de pré-validação registra o recebimento da requisição
- a mensagem aparece e e consumida da fila SQS
- a Lambda de validação registra o processamento do pedido
- o EventBridge recebe um evento `NovoPedidoValidado`

## Exemplo do evento publicado

```json
{
  "Source": "lab.aula1.pedidos.validação",
  "DetailType": "NovoPedidoValidado",
  "Detail": {
    "pedidoId": "lab001-seu-nome",
    "clienteId": "clienteABC-seu-nome",
    "itens": [
      {
        "produto": "Borracha Branca",
        "quantidade": 5
      }
    ],
    "timestamp": "2026-06-21T00:00:00Z"
  }
}
```

## Aprendizados consolidados

Este laboratório reforça um padrão muito comum em arquiteturas orientadas a eventos. Em vez de concentrar toda a lógica em uma única API síncrona, o pedido entra por um endpoint simples, e o processamento segue por etapas desacopladas. Isso reduz acoplamento, melhora a resiliência operacional e prepara o sistema para novas integrações sem alterar o ponto de entrada.

Os principais conceitos práticados foram:

- **API Gateway como porta de entrada** para ingestão de pedidos
- **Lambda para validação em duas etapas**
- **SQS FIFO para ordenação e desacoplamento**
- **DLQ para tratamento de falhas**
- **EventBridge para propagacao de eventos de negócio**
- **CloudWatch Logs para acompanhamento do fluxo**

## Limpeza dos recursos

Ao final do laboratório, remover:

- API Gateway criada para o endpoint `/pedidos`
- Lambdas `pre-validacao` e `validacao-pedidos`
- fila principal FIFO e sua DLQ
- custom event bus
- roles IAM criadas para o laboratório

## Evidencias visuais em ordem das orientacoes

### Fase 1 - IAM e preparacao inicial
Tela inicial do laboratório com foco na area de funções e permissão.
![Evidencia 01](./images/step-01.png)
Criação da role para a Lambda de pré-validação.
![Evidencia 02](./images/step-02.png)
Seleção do tipo de trusted entity para Lambda.
![Evidencia 03](./images/step-03.png)
Associacao da política AWSLambdaBasicExecutionRole.
![Evidencia 04](./images/step-04.png)
Revisão da primeira role criada para o fluxo.
![Evidencia 05](./images/step-05.png)
Criação da role para a Lambda de validação de pedidos.
![Evidencia 06](./images/step-06.png)
Conferência das roles IAM disponiveis para o laboratório.
![Evidencia 07](./images/step-07.png)

### Fase 2 - SQS FIFO e DLQ
Inicio da criação da dead-letter queue FIFO.
![Evidencia 08](./images/step-08.png)
Configuração da DLQ `pedidos-fifo-dlq-seu-nome.fifo`.
![Evidencia 09](./images/step-09.png)
DLQ criada com sucesso no console do Amazon SQS.
![Evidencia 10](./images/step-10.png)
Criação da fila principal FIFO de pedidos.
![Evidencia 11](./images/step-11.png)
Associacao da redrive policy com a fila morta.
![Evidencia 12](./images/step-12.png)
Definição de `Maximum receives = 3`.
![Evidencia 13](./images/step-13.png)
Revisão final da fila principal de pedidos.
![Evidencia 14](./images/step-14.png)

### Fase 3 - Permissão de envio para a fila
Retorno ao IAM para criar a policy inline da pré-validação.
![Evidencia 15](./images/step-15.png)
Edicao da policy em formato JSON com `sqs:SendMessage`.
![Evidencia 16](./images/step-16.png)
Substituicao do ARN da fila principal no documento da policy.
![Evidencia 17](./images/step-17.png)
Revisão da policy antes da criação.
![Evidencia 18](./images/step-18.png)
Policy criada e associada à role da Lambda de pré-validação.
![Evidencia 19](./images/step-19.png)

### Fase 4 - Lambda de pré-validação
Criação da função `pre-validacao-lambda-seu-nome`.
![Evidencia 20](./images/step-20.png)
Seleção do runtime Python para a primeira Lambda.
![Evidencia 21](./images/step-21.png)
Escolha da role de execução da pré-validação.
![Evidencia 22](./images/step-22.png)
Funcao criada e pronta para edicao do codigo.
![Evidencia 23](./images/step-23.png)
Edição do arquivo `lambda_function.py` com a lógica de enfileiramento.
![Evidencia 24](./images/step-24.png)
Deploy do codigo da Lambda.
![Evidencia 25](./images/step-25.png)
Configuração da variável de ambiente `SQS_QUEUE_URL`.
![Evidencia 26](./images/step-26.png)
Conferência final da Lambda de pré-validação.
![Evidencia 27](./images/step-27.png)

### Fase 5 - API Gateway REST
Criação da API REST `pedidos-api-seu-nome`.
![Evidencia 28](./images/step-28.png)
Definição dos detalhes iniciais da API.
![Evidencia 29](./images/step-29.png)
Criação do recurso `/pedidos`.
![Evidencia 30](./images/step-30.png)
Configuração do metodo `POST`.
![Evidencia 31](./images/step-31.png)
Integração do método com a Lambda de pré-validação.
![Evidencia 32](./images/step-32.png)
Deploy da API no stage `dev`.
![Evidencia 33](./images/step-33.png)
Visualizacao da `Invoke URL` para testes.
![Evidencia 34](./images/step-34.png)

### Fase 6 - Teste inicial API -> Lambda -> SQS
Execucao do primeiro teste com `curl`.
![Evidencia 35](./images/step-35.png)
Payload JSON enviado para o endpoint `/pedidos`.
![Evidencia 36](./images/step-36.png)
Resposta de sucesso indicando o enfileiramento do pedido.
![Evidencia 37](./images/step-37.png)
Consulta aos logs da Lambda de pré-validação no CloudWatch.
![Evidencia 38](./images/step-38.png)
Validação da mensagem publicada na fila SQS.
![Evidencia 39](./images/step-39.png)
Detalhes da mensagem presente na fila principal.
![Evidencia 40](./images/step-40.png)

### Fase 7 - EventBridge custom event bus
Criação do barramento customizado no Amazon EventBridge.
![Evidencia 41](./images/step-41.png)
Definição do nome `pedidos-event-bus-seu-nome`.
![Evidencia 42](./images/step-42.png)
Event bus criado e pronto para receber eventos.
![Evidencia 43](./images/step-43.png)

### Fase 8 - Permissoes da Lambda de validação
Retorno ao IAM para editar a role de validação.
![Evidencia 44](./images/step-44.png)
Criação da policy inline com leitura da SQS e `events:PutEvents`.
![Evidencia 45](./images/step-45.png)
Inserção do ARN da fila principal FIFO na policy.
![Evidencia 46](./images/step-46.png)
Inserção do ARN do custom event bus.
![Evidencia 47](./images/step-47.png)
Revisão final da policy da Lambda de validação.
![Evidencia 48](./images/step-48.png)

### Fase 9 - Lambda de validação e trigger SQS
Criação da função `validacao-pedidos-lambda-seu-nome`.
![Evidencia 49](./images/step-49.png)
Seleção do runtime Python e da role da segunda Lambda.
![Evidencia 50](./images/step-50.png)
Edicao do codigo que consome a SQS e publica no EventBridge.
![Evidencia 51](./images/step-51.png)
Deploy do codigo da Lambda de validação.
![Evidencia 52](./images/step-52.png)
Configuração da variável de ambiente `EVENT_BUS_NAME`.
![Evidencia 53](./images/step-53.png)
Adição do trigger SQS na função.
![Evidencia 54](./images/step-54.png)
Seleção da fila FIFO principal como origem do trigger.
![Evidencia 55](./images/step-55.png)
Definição de `Batch size = 1`.
![Evidencia 56](./images/step-56.png)
Trigger configurado e pronto para processar mensagens.
![Evidencia 57](./images/step-57.png)

### Fase 10 - Teste completo do fluxo
Novo envio de pedido para validar o pipeline completo.
![Evidencia 58](./images/step-58.png)
Resposta da API com o pedido aceito e enfileirado.
![Evidencia 59](./images/step-59.png)
Logs da pré-validação confirmando o envio para a fila.
![Evidencia 60](./images/step-60.png)
Logs da Lambda de validação consumindo a mensagem do SQS.
![Evidencia 61](./images/step-61.png)
Confirmação da validação bem-sucedida do pedido.
![Evidencia 62](./images/step-62.png)
Publicacao do evento no EventBridge registrada nos logs.
![Evidencia 63](./images/step-63.png)
Monitoramento do barramento mostrando atividade após o teste.
![Evidencia 64](./images/step-64.png)
Conferência final do laboratório e das metricas do fluxo.
![Evidencia 65](./images/step-65.png)
Validação de encerramento da etapa do Dia 1.
![Evidencia 66](./images/step-66.png)
