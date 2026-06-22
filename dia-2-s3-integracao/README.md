# Dia 2 - Ingestão de Arquivos via S3, Rastreamento e Integração com o Fluxo Principal de Pedidos

Este laboratório documenta a construção de um canal alternativo de entrada de pedidos usando **Amazon S3**, **Amazon SQS Standard**, **AWS Lambda**, **Amazon DynamoDB**, **Amazon SNS** e integração com a **fila FIFO de pedidos criada no Dia 1**. A proposta foi permitir o processamento de pedidos em lote a partir de arquivos JSON enviados para um bucket S3.

## Por que este conhecimento e importante

Nem todo sistema recebe pedidos apenas em tempo real por API. Em muitos cenários, integrações legadas, parceiros ou rotinas internas enviam arquivos em lote para processamento posterior. Esse laboratório mostra como transformar esse tipo de entrada em um pipeline confiável, rastreável e integrado ao fluxo principal já existente.

Os ganhos práticos deste desenho são:

- **Multiplas fontes de entrada** para o mesmo pipeline de pedidos
- **Desacoplamento** com filas SQS no inicio do processamento
- **Rastreabilidade** do status de cada arquivo no DynamoDB
- **Tratamento de erros** com SNS e DLQ
- **Reaproveitamento do fluxo central** do Dia 1 usando a mesma fila FIFO de pedidos

## Objetivo do laboratório

Construir um pipeline em que:

- um **bucket S3** receba arquivos JSON com lotes de pedidos
- o **S3** envie notificações para uma **fila SQS Standard**
- uma **Lambda** leia a fila, baixe o arquivo, valide o schema e extraia os pedidos
- os pedidos válidos sejam enviados para a **fila FIFO principal do Dia 1**
- o resultado do processamento do arquivo seja registrado em uma **tabela DynamoDB**
- erros de validação do arquivo gerem notificações em um **tópico SNS**

## Serviços utilizados

- Amazon S3
- Amazon SQS Standard
- Amazon SQS Dead-Letter Queue
- AWS Lambda
- Amazon DynamoDB
- Amazon SNS
- AWS IAM
- Amazon CloudWatch Logs

## Arquitetura implementada

![Arquitetura do laboratório](./images/arquitetura-dia-2.png)

```mermaid
flowchart LR
    A["Amazon S3 (Data Lake)"] --> B["S3 Notification"]
    B --> C["Amazon SQS Arquivos JSON"]
    C --> D["Lambda de Validação de Arquivos"]
    C --> E["Amazon SQS DLQ"]
    D --> F["DynamoDB Controle Arquivo Historico"]
    D --> G["SNS Notificacao de Erro"]
    D --> H["Fila FIFO de Pedidos (Dia 1)"]
```

## Recursos criados no laboratório

- `lambda-s3-validation-role-seu-nome`
- `datalake-arquivos-seu-nome`
- `s3-arquivos-json-dlq-seu-nome`
- `s3-arquivos-json-queue-seu-nome`
- `validacao-s3-arquivos-lambda-seu-nome`
- `controle-arquivos-histórico-seu-nome`
- `notificacao-erro-arquivos-seu-nome`

## Fluxo funcional do Dia 2

1. Um arquivo JSON e enviado ao bucket S3.
2. O S3 publica uma notificação para a fila `s3-arquivos-json-queue-seu-nome`.
3. A Lambda `validacao-s3-arquivos-lambda-seu-nome` é acionada pela fila.
4. A função baixa o arquivo, valida o JSON e verifica a chave `lista_pedidos`.
5. Cada pedido válido do arquivo é transformado para o formato do pipeline principal.
6. Os pedidos válidos são enviados para a fila FIFO `pedidos-fifo-queue-seu-nome.fifo`.
7. O processamento do arquivo é registrado no DynamoDB.
8. Se houver erro de schema ou JSON inválido, uma notificação é enviada ao SNS.

## Passo a passo técnico

### 1. Criar a role IAM da Lambda de validação de arquivos

Criar a role:

- `lambda-s3-validation-role-seu-nome`

Com a política gerenciada:

- `AWSLambdaBasicExecutionRole`

Depois, complementar com permissões específicas para:

- leitura do S3
- leitura da fila SQS de arquivos
- escrita no DynamoDB
- publicação no SNS
- envio de mensagens para a fila FIFO do Dia 1

### 2. Criar a DLQ e a fila SQS Standard de arquivos

Criar primeiro a fila morta:

- `s3-arquivos-json-dlq-seu-nome`

Depois criar a fila principal:

- `s3-arquivos-json-queue-seu-nome`

Configuracoes importantes:

- tipo `Standard`
- `Visibility timeout = 30`
- DLQ habilitada
- `Maximum receives = 3`

### 3. Criar a tabela DynamoDB de histórico

Criar a tabela:

- `controle-arquivos-histórico-seu-nome`

Com chave primaria:

- `nomeArquivo` do tipo `String`

Essa tabela será usada para registrar o status final de validação de cada arquivo.

### 4. Criar o tópico SNS de notificação de erro

Criar o tópico:

- `notificacao-erro-arquivos-seu-nome`

Depois criar ao menos uma subscription, por exemplo:

- protocolo `Email`

Assim, erros de validação de arquivos podem gerar alerta imediato.

### 5. Criar a Lambda de validação de arquivos

Criar a função:

- `validacao-s3-arquivos-lambda-seu-nome`

Runtime sugerido:

- `Python 3.12`

Variaveis de ambiente:

- `DYNAMODB_TABLE_NAME`
- `SNS_TOPIC_ARN`
- `SQS_FIFO_PEDIDOS_URL`

Codigo utilizado:

```python
import json
import os
import boto3
from datetime import datetime
import urllib.parse
import uuid

DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
SQS_FIFO_PEDIDOS_URL = os.environ["SQS_FIFO_PEDIDOS_URL"]

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
sqs = boto3.client("sqs")
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def lambda_handler(event, context):
    print(f"Evento SQS (arquivos) recebido: {event}")

    for record in event["Records"]:
        s3_event_message_body = record["body"]
        object_key = None
        bucket_name = None

        try:
            s3_event_message = json.loads(s3_event_message_body)

            if "Message" in s3_event_message and isinstance(s3_event_message["Message"], str):
                s3_notification = json.loads(s3_event_message["Message"])
            else:
                s3_notification = s3_event_message

            for s3_record in s3_notification["Records"]:
                bucket_name = s3_record["s3"]["bucket"]["name"]
                object_key_encoded = s3_record["s3"]["object"]["key"]
                object_key = urllib.parse.unquote_plus(object_key_encoded)

                print(f"Processando arquivo: s3://{bucket_name}/{object_key}")

                s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
                file_content = s3_object["Body"].read().decode("utf-8")

                status_validacao_arquivo = "ERRO_VALIDACAO_ARQUIVO"
                detalhes_erro_arquivo = "Conteúdo do arquivo não é JSON válido."

                try:
                    arquivo_data = json.loads(file_content)

                    if isinstance(arquivo_data, dict) and "lista_pedidos" in arquivo_data and isinstance(arquivo_data["lista_pedidos"], list):
                        status_validacao_arquivo = "ARQUIVO_VALIDADO"
                        detalhes_erro_arquivo = None
                        print(f"Schema do arquivo {object_key} validado.")

                        for pedido_item in arquivo_data["lista_pedidos"]:
                            if "id_pedido_arquivo" not in pedido_item or "id_cliente_arquivo" not in pedido_item:
                                print(f"Pedido inválido no arquivo {object_key}: {pedido_item}. Campos obrigatórios ausentes.")
                                continue

                            pedido_formatado = {
                                "pedidoId": str(pedido_item.get("id_pedido_arquivo")),
                                "clienteId": str(pedido_item.get("id_cliente_arquivo")),
                                "itens": pedido_item.get("itens_pedido_arquivo", []),
                                "origem": "S3_FILE",
                                "nomeArquivoOriginal": object_key,
                                "timestamp_extracao_arquivo": datetime.utcnow().isoformat() + "Z"
                            }

                            deduplication_id = f"{object_key}-{pedido_formatado['pedidoId']}-{uuid.uuid4()}"

                            sqs.send_message(
                                QueueUrl=SQS_FIFO_PEDIDOS_URL,
                                MessageBody=json.dumps(pedido_formatado),
                                MessageGroupId=str(pedido_formatado["pedidoId"]),
                                MessageDeduplicationId=deduplication_id
                            )

                            print(f"Pedido {pedido_formatado['pedidoId']} do arquivo {object_key} enviado para SQS FIFO Pedidos.")
                    else:
                        detalhes_erro_arquivo = "Schema do arquivo inválido. Esperada a chave 'lista_pedidos' contendo uma lista."
                        print(detalhes_erro_arquivo)

                except json.JSONDecodeError as je:
                    print(f"Arquivo {object_key} não é um JSON válido: {str(je)}")

                item_to_put = {
                    "nomeArquivo": object_key,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "statusValidacao": status_validacao_arquivo,
                    "bucket": bucket_name
                }

                if detalhes_erro_arquivo:
                    item_to_put["detalhesErro"] = detalhes_erro_arquivo

                table.put_item(Item=item_to_put)

                if status_validacao_arquivo != "ARQUIVO_VALIDADO":
                    message_sns = f"Erro de validação no ARQUIVO: {object_key} do bucket {bucket_name}.\n"
                    message_sns += f"Status: {status_validacao_arquivo}\n"
                    if detalhes_erro_arquivo:
                        message_sns += f"Detalhes: {detalhes_erro_arquivo}"

                    sns.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Message=message_sns,
                        Subject=f"Erro de Validacao de ARQUIVO S3 - {object_key}"
                    )

                    print(f"Notificacao de erro de ARQUIVO enviada para SNS para {object_key}")

        except Exception as e:
            err_msg = f"Erro crítico ao processar SQS record: {str(e)}"
            if object_key:
                err_msg = f"Erro crítico ao processar SQS record para arquivo {object_key}: {str(e)}"
            print(err_msg)
            sns.publish(TopicArn=SNS_TOPIC_ARN, Message=err_msg, Subject="Erro Critico no Processamento de Arquivo S3")
            raise e

    return {"statusCode": 200, "body": "Processamento de arquivos S3 e envio de pedidos concluido"}
```

### 6. Adicionar permissão inicial para o trigger SQS

Antes de criar o trigger da Lambda, adicionar uma inline policy na role para leitura da fila:

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
      "Resource": "COLE_AQUI_O_ARN_DA_SUA_FILA_SQS_s3-arquivos-json-queue-seu-nome"
    }
  ]
}
```

### 7. Configurar o trigger SQS da Lambda

Na Lambda `validacao-s3-arquivos-lambda-seu-nome`, adicionar um trigger:

- fonte `SQS`
- fila `s3-arquivos-json-queue-seu-nome`
- `Batch size = 1`

Se ocorrer erro de timeout entre fila e Lambda, ajustar:

- `Visibility timeout` da fila para `60 segundos`

### 8. Criar o bucket S3 e configurar a notificação

Criar o bucket:

- `datalake-arquivos-seu-nome`

Depois configurar em `Properties > Event notifications`:

- nome do evento `s3-json-upload-to-sqs-seu-nome`
- sufixo `.json`
- tipo `All object creaté events`
- destino `SQS queue`
- fila `s3-arquivos-json-queue-seu-nome`

Se o S3 não conseguir validar o destino, ajustar a policy da fila para permitir `SQS:SendMessage` do servico S3.

### 9. Atualizar as permissões completas da role da Lambda

Adicionar uma inline policy com acesso aos recursos do fluxo:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::datalake-arquivos-seu-nome/*"
    },
    {
      "Effect": "Allow",
      "Action": "dynamodb:PutItem",
      "Resource": "COLE_AQUI_O_ARN_DA_SUA_TABELA_controle-arquivos-histórico-seu-nome"
    },
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "COLE_AQUI_O_ARN_DO_SEU_SNS_notificacao-erro-arquivos-seu-nome"
    },
    {
      "Effect": "Allow",
      "Action": "sqs:SendMessage",
      "Resource": "COLE_AQUI_O_ARN_DA_SUA_FILA_pedidos-fifo-queue-seu-nome.fifo"
    }
  ]
}
```

### 10. Preparar os arquivos de teste

Arquivo válido `arquivo_com_pedidos.json`:

```json
{
  "metadata_arquivo": {
    "data_geracao": "2024-03-15",
    "origem_lote": "sistema_X"
  },
  "lista_pedidos": [
    {
      "id_pedido_arquivo": "S3P001-seu-nome",
      "id_cliente_arquivo": "S3C001",
      "itens_pedido_arquivo": [
        { "sku": "PROD-A", "qtd": 2 },
        { "sku": "PROD-B", "qtd": 1 }
      ]
    },
    {
      "id_pedido_arquivo": "S3P002-seu-nome",
      "id_cliente_arquivo": "S3C002",
      "itens_pedido_arquivo": [
        { "sku": "PROD-C", "qtd": 5 }
      ]
    },
    {
      "id_pedido_arquivo_invalido": "S3P003-falta-cliente",
      "itens_pedido_arquivo": [
        { "sku": "PROD-D", "qtd": 1 }
      ]
    }
  ]
}
```

Arquivo inválido `arquivo_schema_invalido.json`:

```json
{
  "alguma_outra_chave": "sem_lista_pedidos"
}
```

### 11. Validar o fluxo completo

Ao subir `arquivo_com_pedidos.json`, o resultado esperado e:

- a fila `s3-arquivos-json-queue-seu-nome` recebe e encaminha a mensagem
- a Lambda processa o arquivo e envia `S3P001` e `S3P002` para a fila FIFO do Dia 1
- o pedido inválido é ignorado por falta de campos obrigatórios
- o DynamoDB registra o arquivo com `statusValidacao = ARQUIVO_VALIDADO`
- nenhuma notificacao SNS e enviada

Ao subir `arquivo_schema_invalido.json`, o resultado esperado é:

- a Lambda detecta erro de schema
- o DynamoDB registra `statusValidacao = ERRO_VALIDACAO_ARQUIVO`
- uma notificacao SNS e enviada
- nenhum pedido segue para a fila FIFO principal

## Aprendizados consolidados

Este laboratório mostra como integrar uma segunda fonte de entrada ao mesmo pipeline principal sem duplicar a lógica central de negócio. Em vez de criar um fluxo paralelo isolado, os pedidos extraídos do S3 são normalizados e reaproveitam a mesma fila FIFO e a mesma cadeia de validação iniciada no Dia 1.

Os principais conceitos reforçados foram:

- **S3 como origem de eventos em lote**
- **SQS Standard para desacoplamento do processamento de arquivos**
- **Lambda para validação, extracao e transformação**
- **DynamoDB para histórico e rastreabilidade**
- **SNS para notificação de erros de arquivo**
- **Integração entre múltiplas fontes de entrada e um pipeline unico**

## Limpeza dos recursos

Ao final do laboratório, remover:

- bucket S3 criado para os arquivos
- notificações de evento do bucket
- fila principal SQS e sua DLQ
- Lambda de validação de arquivos
- tabela DynamoDB
- tópico SNS e subscriptions
- policies inline e role IAM criadas para o laboratório

## Evidencias visuais em ordem das orientacoes

### Fase 1 - IAM e preparacao inicial
Criação da role para a Lambda de validação de arquivos do S3.
![Evidencia 01](./images/step-01.png)
Seleção do caso de uso Lambda na role IAM.
![Evidencia 02](./images/step-02.png)
Associação da policy básica de execução da Lambda.
![Evidencia 03](./images/step-03.png)
Revisão final da role `lambda-s3-validation-role-seu-nome`.
![Evidencia 04](./images/step-04.png)

### Fase 2 - Filas SQS Standard para arquivos
Criação da DLQ `s3-arquivos-json-dlq-seu-nome`.
![Evidencia 05](./images/step-05.png)
Confirmação da fila morta criada com sucesso.
![Evidencia 06](./images/step-06.png)
Criação da fila principal `s3-arquivos-json-queue-seu-nome`.
![Evidencia 07](./images/step-07.png)
Configuração da DLQ e `Maximum receives = 3`.
![Evidencia 08](./images/step-08.png)
Revisão da fila principal de arquivos JSON.
![Evidencia 09](./images/step-09.png)

### Fase 3 - DynamoDB e SNS
Criação da tabela `controle-arquivos-histórico-seu-nome`.
![Evidencia 10](./images/step-10.png)
Definição da chave `nomeArquivo`.
![Evidencia 11](./images/step-11.png)
Conferência do ARN da tabela DynamoDB.
![Evidencia 12](./images/step-12.png)
Criação do tópico SNS de erro de arquivos.
![Evidencia 13](./images/step-13.png)
Definição do nome `notificacao-erro-arquivos-seu-nome`.
![Evidencia 14](./images/step-14.png)
Criação da subscription de e-mail no SNS.
![Evidencia 15](./images/step-15.png)
Confirmação da subscription do tópico.
![Evidencia 16](./images/step-16.png)

### Fase 4 - Lambda de validação de arquivos
Criação da função `validacao-s3-arquivos-lambda-seu-nome`.
![Evidencia 17](./images/step-17.png)
Seleção do runtime Python 3.12.
![Evidencia 18](./images/step-18.png)
Escolha da role da Lambda criada para o laboratório.
![Evidencia 19](./images/step-19.png)
Edicao do codigo da função de validação e extracao.
![Evidencia 20](./images/step-20.png)
Deploy do codigo da Lambda.
![Evidencia 21](./images/step-21.png)
Configuração das variaveis de ambiente.
![Evidencia 22](./images/step-22.png)
Definição de `DYNAMODB_TABLE_NAME`, `SNS_TOPIC_ARN` e `SQS_FIFO_PEDIDOS_URL`.
![Evidencia 23](./images/step-23.png)
Configuração do timeout da função.
![Evidencia 24](./images/step-24.png)

### Fase 5 - Permissão inicial e trigger SQS
Criação da policy inline para leitura da fila de arquivos.
![Evidencia 25](./images/step-25.png)
JSON com permissões `ReceiveMessage`, `DeleteMessage` e `GetQueueAttributes`.
![Evidencia 26](./images/step-26.png)
Policy criada para liberar o trigger.
![Evidencia 27](./images/step-27.png)
Adição do trigger SQS na Lambda.
![Evidencia 28](./images/step-28.png)
Seleção da fila `s3-arquivos-json-queue-seu-nome`.
![Evidencia 29](./images/step-29.png)
Ajuste do `Batch size = 1`.
![Evidencia 30](./images/step-30.png)
Revisão do erro de timeout entre fila e Lambda.
![Evidencia 31](./images/step-31.png)
Ajuste do `Visibility timeout` para 60 segundos.
![Evidencia 32](./images/step-32.png)
Trigger configurado corretamente após o ajuste.
![Evidencia 33](./images/step-33.png)

### Fase 6 - Bucket S3 e notificações para SQS
Criação do bucket `datalake-arquivos-seu-nome`.
![Evidencia 34](./images/step-34.png)
Configuração do bucket com bloqueio de acesso público.
![Evidencia 35](./images/step-35.png)
Conferência do bucket criado.
![Evidencia 36](./images/step-36.png)
Edicao da queue policy para permitir `s3.amazonaws.com`.
![Evidencia 37](./images/step-37.png)
JSON da policy vinculando bucket e fila SQS.
![Evidencia 38](./images/step-38.png)
Criação da notificação de eventos do bucket.
![Evidencia 39](./images/step-39.png)
Configuração de sufixo `.json` e destino SQS.
![Evidencia 40](./images/step-40.png)
Notificacao de evento criada com sucesso.
![Evidencia 41](./images/step-41.png)

### Fase 7 - Permissoes completas da role
Criação da policy final com acesso ao S3, DynamoDB, SNS e fila FIFO.
![Evidencia 42](./images/step-42.png)
Definição da permissão `s3:GetObject`.
![Evidencia 43](./images/step-43.png)
Definição da permissão `dynamodb:PutItem`.
![Evidencia 44](./images/step-44.png)
Definição da permissão `sns:Publish`.
![Evidencia 45](./images/step-45.png)
Definição da permissão `sqs:SendMessage` para a fila FIFO do Dia 1.
![Evidencia 46](./images/step-46.png)
Policy final revisada e criada.
![Evidencia 47](./images/step-47.png)

### Fase 8 - Testes com arquivos válidos e inválidos
Preparação e upload do `arquivo_com_pedidos.json`.
![Evidencia 48](./images/step-48.png)
Validação dos logs da Lambda após o upload do arquivo válido.
![Evidencia 49](./images/step-49.png)
Conferência do item gravado no DynamoDB.
![Evidencia 50](./images/step-50.png)
Verificacao final do fluxo completo e integração com a fila FIFO de pedidos.
![Evidencia 51](./images/step-51.png)
