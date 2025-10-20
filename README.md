# Desafio Técnico — Ingestão de Dados (API e CSV)

Este projeto implementa **duas pipelines de ingestão de dados** — uma via **API REST** e outra via **CSV** — com foco em **boas práticas de engenharia de dados**, **validação de schema**, **governança via metadados** e **estrutura de partição bronze** no padrão Data Lake.

As soluções foram projetadas para serem **auditáveis, previsíveis e fáceis de operar**, com geração automática de metadados e separação clara de responsabilidades entre os módulos utilitários.

---

## Estrutura do Projeto

app/
├── ingestao_api.py
├── ingestao_csv.py
├── utils/
│ ├── casting.py
│ ├── validate.py
│ ├── date.py
│ ├── metadata.py
└── config/
├── simulacao_api.json
└── indicadores_municipios.json

**Principais módulos utilitários:**
- **`casting.py`** → tipagem e limpeza de campos numéricos e string.  
- **`validate.py`** → validação de schema, tipos e colunas obrigatórias.  
- **`metadata.py`** → geração de manifestos `.manifest.json` com hash, colunas, linhas e preview.  
- **`date.py`** → utilitários de data e geração de partição `anomesdia`.

## Ingestão via API — `ingestao_api.py`

**Desafio:**  
Consumir a API pública [JSONPlaceholder](https://jsonplaceholder.typicode.com), capturando:
- Nome, usuário e e-mail de todos os usuários.  
- Todos os posts do usuário **Kurtis Weissnat**.

**Etapas da solução:**
1. Leitura de configurações via `simulacao_api.json`.  
2. Consumo de API com `requests.Session`, retries e timeout controlados.  
3. Criação e limpeza de DataFrames (`users_df`, `posts_df`).  
4. Exibição dos resultados no stdout.  
5. Salvamento em **TXT (CSV com “;”)** particionado por `anomesdia=AAAAMMDD` (Em produção, o formato Parquet seria preferível por performance e custo, mas nesta versão optou-se por TXT/CSV pela portabilidade e facilidade de leitura em qualquer editor ou planilha)
6. Geração de metadados ao lado dos arquivos.

**Saídas esperadas:**
data-lake/bronze
├── tb_simulacao_api_users/anomesdia=20251020/users.txt
├── tb_simulacao_api_users/anomesdia=20251020/users.txt.manifest.json
├── tb_simulacao_api_posts/anomesdia=20251020/posts.txt
└── tb_simulacao_api_posts/anomesdia=20251020/posts.txt.manifest.json

## Ingestão de CSV — `ingestao_csv.py`

**Desafio:**  
Ler e validar o arquivo `IBC_municipios_indicadores_normalizados.csv` (dados abertos) e exibir no stdout as colunas **Município**, **UF** e **Densidade SMP** (10 primeiras linhas).

**Etapas da solução:**
1. Leitura configurável via pasta data-lake/temp/ `IBC_municipios_indicadores_normalizados.csv`.  
2. Normalização de nomes de colunas (`columns_normalization`).  
3. Validação de colunas obrigatórias e tipos (`ensure_required_columns`, `check_dtypes`).  
4. Aviso de colunas adicionais não previstas no schema.  
5. Validação das colunas esperadas no preview (`municipio`, `uf`, `densidade_smp`).  
6. Exibição da prévia no stdout.  
7. Salvamento em **TXT (CSV “;”)** com partição dinâmica `anomesdia=AAAAMMDD`. (Em produção, o formato Parquet seria preferível por performance e custo, mas nesta versão optou-se por TXT/CSV pela portabilidade e facilidade de leitura em qualquer editor ou planilha) 
8. Geração automática de **metadados**  com estatísticas e hash do arquivo.

**Saídas esperadas:**
data-lake/bronze
└── tb_indicadores_municipio/anomesdia=20251020/indmunicipios.txt
└── tb_indicadores_municipio/anomesdia=20251020/indmunicipios.txt.manifest.json

## Padrões e Boas Práticas

- **Modularização**: funções utilitárias isoladas em `utils/`.  
- **Validação explícita**: garantias de schema e tipos antes e depois dos casts.  
- **Governança**: geração de manifestos com hash MD5, linhas, colunas, nulos e preview.  
- **Particionamento dinâmico**: estrutura `tabela/anomesdia=AAAAMMDD`.  
- **Robustez**: tratamento de erros (`SchemaError`, `FileNotFoundError`, `Timeout`, `ConnectionError`).  
- **Reprodutibilidade**: ingestões parametrizadas por arquivos de configuração `.json`.  
- **Auditabilidade**: logs padronizados e fluxo previsível de saída.  

##  Execução

### Requisitos
- Python 3.9+
Dependências:
- pandas>=2.2.2
- pyarrow>=16.0.0
- requests>=2.32.3s

## Executar a ingestão da API
python app/ingestao_api.py

## Executar a ingestão do CSV
python app/ingestao_csv.py

Os resultados e metadados são salvos automaticamente na pasta configurada em data-lake/bronze/

## Uso de Inteligência Artificial

Utilizei Copilot e ChatGPT para acelerar a escrita de partes genéricas do código, como:
	•	Estrutura base das funções (load_config, get_session, leitura de CSV/API);
	•	Moldes de DataFrame.rename, print(...to_string(index=False)) e to_csv;
	•	Assinatura e esqueleto da função write_metadata_from_df;
	•	Comentários e docstrings explicativos.

Também utilizei IA para gerar o template inicial das funções em utils:
	•	validate.py
	•	casting.py
	•	metadata.py

Toda a lógica de negócio e governança, incluindo validações de schema, checagem de colunas extras, validação de colunas do preview, integração de metadados e estrutura de partição — foi curada e implementada manualmente, priorizando auditabilidade, previsibilidade e boas práticas de engenharia de dados.
 
Autor: Bruna Oliveira de Sousa