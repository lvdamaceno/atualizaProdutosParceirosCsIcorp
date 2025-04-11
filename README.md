# Sankhya API Integration

Este projeto automatiza a integração entre o sistema ERP **Sankhya** e uma API externa (iCorp), realizando cadastros e atualizações de **clientes**, **produtos** e **estoque**.

## 📌 Objetivo

Automatizar a sincronização de dados da plataforma Sankhya com uma API REST externa, com envio periódico e seguro dos dados via cron job, utilizando autenticação com tokens e headers personalizados.

## 🚀 Tecnologias Utilizadas

- **Python 3.10+**
- **requests** – para requisições HTTP
- **tqdm** – barra de progresso para o terminal
- **dotenv** – leitura segura de variáveis de ambiente
- **logging** – para logs de execução
- **Render** – hospedagem do script com agendamento via cron job

## ⚙️ Funcionalidades

- Autenticação segura com a API Sankhya usando `.env` e headers personalizados
- Leitura de arquivos `.sql` e execução de consultas dinâmicas via API Sankhya
- Conversão de resposta JSON para envio à API iCorp
- Envio em lote para endpoints como:
  - `/Cliente`
  - `/ProdutoUpdate`
  - `/Saldos_Atualiza`
- Logging detalhado da execução
- Totalmente desacoplado com suporte a reutilização via pacote Python

## 📦 Estrutura Reutilizável

A parte de autenticação e requisição foi empacotada em um módulo reutilizável chamado `sankhya_api`, permitindo o uso da mesma lógica em outros projetos que utilizem o mesmo padrão da API Sankhya.

### Instalação do pacote localmente

```bash
pip install /caminho/para/sankhya_api_package
```

### Exemplo de uso

```python
from sankhya_api.request import SankhyaClient
service = "DbExplorerSP.executeQuery"
endpoint_sankhya = f"https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName={service}&outputType=json"
client = SankhyaClient(service, endpoint_sankhya, 5)
resultado = client.execute_query("SELECT * FROM TGFPAR")
print(resultado)
```

## 🌐 Deploy na Render

O projeto foi implantado na plataforma [Render](https://render.com), utilizando a função de **cron job** que executa o script automaticamente a cada **1 hora**.

### Cron configurado:

```bash
@hourly python main.py
```

## 🔐 Configuração (.env)

O arquivo `.env` deve conter as seguintes variáveis:

```dotenv
SANKHYA_TOKEN=seu_token
SANKHYA_APPKEY=sua_appkey
SANKHYA_PASSWORD=sua_senha
SANKHYA_USER=seu_usuario
```

## 📁 Organização de Diretórios

```
.
├── main.py
├── queries/
│   ├── PARCEIROS.sql
│   ├── PRODUTOS.sql
│   ├── JSON_PARCEIRO.sql
│   ├── JSON_PRODUTO.sql
│   └── JSON_ESTOQUE.sql
├── sankhya_api/             # Pacote reutilizável
│   ├── __init__.py
│   ├── auth.py
│   └── client.py
├── .env
└── requirements.txt
```

## 📄 Licença

Este projeto é de uso interno e pode ser adaptado para outras integrações ERP + API REST, desde que mantida a estrutura modular e segura.

---

> Desenvolvido com ❤️ por Vinícius Damaceno – integração inteligente entre sistemas com Python.