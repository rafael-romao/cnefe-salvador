# Projeto Geolinkage
O objetivo desse projeto é a condução de um processo de pipeline ETL para extrair, tratar e  base de dados 


## Arquitetura

### Spark

`3.5.0`

| Tipo  | Quantidade | Memória Disp.
| ------------- | ------------- | ------------- |
| `MASTER`  | 1  | 1GB|
| `WORKER` | 2  | 1GB |

### Airflow

`2.8.4`

### MinIO

`>=RELEASE.2023-11-22`

### Jupyter

`7.2.1`

## Pré-requisitos
É recomendável que este projeto rode com a versão `3.11.8` ou superior do Python. Mais recomendável é rodá-lo em um ambiente virtual com essa versão, o que é possível ser feito com o [pyenv](https://github.com/pyenv/pyenv-virtualenv). Também é necessário o gerenciador de containers [Docker](https://docs.docker.com/engine/install/).



| Requisito  | Versão |
| ------------- | ------------- |
| `Python`  | `>=3.11.8`  |
| `Docker Engine` | `latest`  |

## Instalação

Clone o repositório para sua máquina

```shell
$ git clone https://github.com/rafael-romao/cnefe-salvador.git
$ cd Project
```

Para instalar as dependências
```shell
$ make prepare_environment
```

Para build dos serviços
```shell
$ make build
```

A partir desse ponto, o projeto possui seus serviços prontos para execução. Para levantar todo o projeto

```shell
$ make up
```

## Configuração

| Serviço  | URL |
| ------------- | ------------- |
| Airflow  | http://localhost:8080  |
| Jupyter  | http://localhost:8080  |
| MinIO | http://localhost:9001  |
| Spark | http://localhost:7077  |
| Spark Apps| http://localhost:4040  |


Acesse o Airflow e no menu Admin > Connections adicione a conexão com o Spark conforme a imagem
![alt text](/imagens/airflow_exemplo.png)

Acesse o MinIO e no menu Bucket > Create Bucket adicione três buckets: `landing`,`raw`,`cleaned`
![alt text](/imagens/minio_bucket_exemplo.png)

Acesse o MinIO e no menu Bucket > Access Keys crie uma chave de acesso. Importante: copie a `Access Key` e a `Secret Key`, pois essas serão as credenciais para o PySpark acessar o Datalake
![alt text](/imagens/minio_bucket_exemplo.png)

No meu Object Browse > Upload, adicione a base `29.txt` no bucket `landing`



