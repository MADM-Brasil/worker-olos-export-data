# Fase 3B — contratos Olos ExportDataV3

Data: 15/10/2026. **Contratos documentais consolidados, consumo real bloqueado e nenhuma aprovação automática.** Escopo: aquisição fiel e persistência da origem; projeção e banco definitivo não participam desta fase.

## 1. Documento lido e níveis de evidência

Foi lido integralmente o [manual DBA-MN002, ExportData V3.0 Cliente, revisão 1 de 03/06/2020](reference/olos-exportdata-v3.pdf), **33 páginas**. O caminho solicitado inicialmente não existia; foi localizada a cópia `/home/madm/Downloads/DBA-MN002 - Export Data V3_Cliente.pdf` e copiada sem alteração para `docs/reference/olos-exportdata-v3.pdf`.

SHA-256: `db4f934af564f0e5a3bb244cdb056e50e8b294dfcf6f94c25d14987ee95c1b71`. A extração textual com Ghostscript apresentou todos os marcadores de página 1–33; a página 17 também foi conferida visualmente para PK e comprimentos Unicode. O aviso final do dispositivo de texto não impediu a geração dessas 33 páginas. Não foram instaladas dependências no projeto para ler o PDF.

Evidências mantidas separadas no código e neste relatório:

| Evidência | O que comprova | O que não comprova |
| --- | --- | --- |
| PDF, com página/revisão/hash | Nomes, tipos declarados, descrições e comportamento documentado em 2020. | Layout físico atual, nulabilidade, índices atuais, semântica observada ou aquisição exata pelo driver. |
| `sys.parameters` coletado no ambiente real e fornecido pela empresa nesta solicitação | Assinaturas declaradas atuais de sete procedures no banco ExportDataV3, schema dbo, conforme relato fornecido. | Colunas dos datasets, defaults, nulabilidade não fornecida, implementações internas ou efeitos transacionais. O worker não repetiu essas consultas. |
| Testes sintéticos e parser instalado | Comportamento local dos codecs, limites, validação e persistência no fixture. | Compatibilidade ponta a ponta com SQL Server real ou ausência de perda operacional. |

Nenhum dado de catálogo de colunas das seis tabelas foi fornecido nesta etapa. Portanto todos os campos abaixo têm **tipo documentado**, mas **tipo atual/nulabilidade/constraints ainda não verificados**. Não se transforma documentação antiga em aprovação de produção.

## 2. Comparação com a Fase 3A e versionamento

O snapshot v1 da Fase 3A permanece em `tableContracts`: somente nomes das chaves, tipos desconhecidos e approved=false. Isso preserva os 221 testes e o histórico do contrato. A versão v2 está em `documentedTableContracts`, com transcrição completa do manual, evidência por campo e exclusões. `contracts.ts` reexporta os novos catálogos sem ativar qualquer fluxo.

Há **87 campos elegíveis catalogados**, mais um campo documental excluído: Password. Totais: AttemptsRawData 18; InboundRawData 17; PbxBillingData 32; Users 11 elegíveis + Password; Campaign 7; Disposition 2.

As três PKs transacionais são **INT no manual**, assim como UserId, CampaignId e DispositionId. Os exemplos BIGINT da Fase 3A eram sintéticos e não confirmavam tipos da Olos. O intervalo BIGINT de ReReadData não altera o tipo documentado da PK. O codec de BIGINT continua disponível para parâmetros e eventuais layouts atuais comprovados, sem ampliar silenciosamente um INT documentado.

`documentedLayoutComplete=true` significa transcrição completa das seis seções do manual, com a exclusão explícita de senha; `currentLayoutConfirmed=false` e `approved=false` permanecem. Não se gera SourceContract aprovado nem política de nulabilidade a partir desses catálogos. Nenhuma versão pending no banco real foi alterada.

## 3. Matriz completa dos campos

`Não informado` na coluna do servidor significa ausência de evidência de colunas atuais, não ausência da coluna no servidor. Para todas as linhas, continuam pendentes nulabilidade, defaults, collation, precisão efetiva e igualdade com o dataset instalado. Os tamanhos NVARCHAR abaixo são reproduzidos literalmente do manual, sem presumir que o autor os escreveu em bytes.

### AttemptsRawData — AttemptsRawData/v2

| Campo | Tipo no PDF | Página | SQL Server atual | Pendência |
| --- | --- | --- | --- | --- |
| CallId | INT | 16 | Não informado | Layout/nulabilidade atuais. |
| AgentId | INT | 16 | Não informado | Layout/nulabilidade atuais. |
| CampaignId | INT | 16 | Não informado | Layout/nulabilidade atuais. |
| DispositionId | INT | 16 | Não informado | Layout/nulabilidade atuais. |
| StartDate | DATETIME | 16 | Não informado | Layout/nulabilidade atuais. |
| EndWrap | DATETIME | 16 | Não informado | Layout/nulabilidade atuais. |
| TableName | VARCHAR(255) | 16 | Não informado | Layout/nulabilidade atuais. |
| EndCall | DATETIME | 16 | Não informado | Layout/nulabilidade atuais. |
| OriginalPhoneNumber | VARCHAR(16) | 16 | Não informado | Layout/nulabilidade atuais. |
| Dnis | VARCHAR(255) | 16 | Não informado | Layout/nulabilidade atuais. |
| PhoneTypeId | INT | 16 | Não informado | Layout/nulabilidade atuais. |
| Route | VARCHAR(256) | 16 | Não informado | Layout/nulabilidade atuais. |
| CampaignData | VARCHAR(MAX) | 16 | Não informado | Layout/nulabilidade atuais. |
| CustomerId | VARCHAR(255) | 16 | Não informado | Layout/nulabilidade atuais. |
| MailingRecordId | INT | 16 | Não informado | Layout/nulabilidade atuais. |
| MailingPhoneNumberId | INT | 16 | Não informado | Layout/nulabilidade atuais. |
| CallIdTelecom | VARCHAR(16) | 17 | Não informado | Layout/nulabilidade atuais. |
| AttemptsRawDataID | INT | 17 | Não informado | Layout/nulabilidade atuais. |

### InboundRawData — InboundRawData/v2

| Campo | Tipo no PDF | Página | SQL Server atual | Pendência |
| --- | --- | --- | --- | --- |
| CallId | INT | 23 | Não informado | Layout/nulabilidade atuais. |
| CampaignId | INT | 23 | Não informado | Layout/nulabilidade atuais. |
| AgentId | INT | 23 | Não informado | Layout/nulabilidade atuais. |
| DispositionId | INT | 23 | Não informado | Layout/nulabilidade atuais. |
| CallStart | DATETIME | 23 | Não informado | Layout/nulabilidade atuais. |
| AgentStart | DATETIME | 23 | Não informado | Layout/nulabilidade atuais. |
| CallEnd | DATETIME | 23 | Não informado | Layout/nulabilidade atuais. |
| WrapEnd | DATETIME | 23 | Não informado | Layout/nulabilidade atuais. |
| QueueStart | DATETIME | 23 | Não informado | Layout/nulabilidade atuais. |
| QueueEnd | DATETIME | 23 | Não informado | Layout/nulabilidade atuais. |
| Ani | VARCHAR(255) | 23 | Não informado | Layout/nulabilidade atuais. |
| Dnis | VARCHAR(20) | 23 | Não informado | Layout/nulabilidade atuais. |
| CampaignData | VARCHAR(MAX) | 23 | Não informado | Layout/nulabilidade atuais. |
| CustomerId | VARCHAR(255) | 23 | Não informado | Layout/nulabilidade atuais. |
| InboundDispositionId | INT | 23 | Não informado | Layout/nulabilidade atuais. |
| CallIdTelecom | VARCHAR(16) | 23 | Não informado | Layout/nulabilidade atuais. |
| InboundRawDataID | INT | 23 | Não informado | Layout/nulabilidade atuais. |

### PbxBillingData — PbxBillingData/v2

| Campo | Tipo no PDF | Página | SQL Server atual | Pendência |
| --- | --- | --- | --- | --- |
| PbxBillingDataId | INT | 26 | Não informado | Layout/nulabilidade atuais. |
| StartDate | DATETIME | 26 | Não informado | Layout/nulabilidade atuais. |
| TelecomCallid | CHAR(16) | 26 | Não informado | Layout/nulabilidade atuais. |
| CallDirectionId | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| DeviceName | VARCHAR(70) | 27 | Não informado | Layout/nulabilidade atuais. |
| Route | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| Ani | VARCHAR(16) | 27 | Não informado | Layout/nulabilidade atuais. |
| Dnis | VARCHAR(20) | 27 | Não informado | Layout/nulabilidade atuais. |
| SignalingExtraInfo | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| TotalTime | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| SignalingTime | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| AlertingTime | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| AnalysisTime | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| ConversationTime | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| PbxDispositionId | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| CampaignId | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| MailingName | VARCHAR(100) | 27 | Não informado | Layout/nulabilidade atuais. |
| AgentId | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| Company | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| CampaignCode | VARCHAR(100) | 27 | Não informado | Layout/nulabilidade atuais. |
| BillingCode | VARCHAR(200) | 27 | Não informado | Layout/nulabilidade atuais. |
| SessionId | INT | 27 | Não informado | Layout/nulabilidade atuais. |
| TrunkNumber | VARCHAR(20) | 27 | Não informado | Layout/nulabilidade atuais. |
| OriginalPhoneNumber | VARCHAR(20) | 27 | Não informado | Layout/nulabilidade atuais. |
| PbxName | VARCHAR(20) | 27 | Não informado | Layout/nulabilidade atuais. |
| Board | VARCHAR(10) | 27 | Não informado | Layout/nulabilidade atuais. |
| Link | VARCHAR(2) | 27 | Não informado | Layout/nulabilidade atuais. |
| Channel | VARCHAR(5) | 28 | Não informado | Layout/nulabilidade atuais. |
| Signaling | VARCHAR(10) | 28 | Não informado | Layout/nulabilidade atuais. |
| CustomerId | VARCHAR(100) | 28 | Não informado | Layout/nulabilidade atuais. |
| RecordId | INT | 28 | Não informado | Layout/nulabilidade atuais. |
| PhoneNumberId | INT | 28 | Não informado | Layout/nulabilidade atuais. |

### Users — Users/v2

| Campo | Tipo no PDF | Página | SQL Server atual | Pendência |
| --- | --- | --- | --- | --- |
| UserId | INT | 30 | Não informado | Layout/nulabilidade atuais. |
| AgentId | INT | 30 | Não informado | Layout/nulabilidade atuais. |
| Flag_Agent | INT | 30 | Não informado | Layout/nulabilidade atuais. |
| Flag_Supervisor | INT | 30 | Não informado | Layout/nulabilidade atuais. |
| Flag_Manager | INT | 30 | Não informado | Layout/nulabilidade atuais. |
| Activated | INT | 30 | Não informado | Layout/nulabilidade atuais. |
| LastLogin | DATETIME | 30 | Não informado | Layout/nulabilidade atuais. |
| Email | VARCHAR(MAX) | 30 | Não informado | Layout/nulabilidade atuais. |
| Login | NVARCHAR(100) | 30 | Não informado | Layout/nulabilidade atuais. |
| UserName | NVARCHAR(510) | 30 | Não informado | Layout/nulabilidade atuais. |
| EnablePersonalTransfer | INT | 30 | Não informado | Layout/nulabilidade atuais. |
| Password | NVARCHAR(100) | 30 | Não informado | **Excluído da ingestão; senha.** |

### Campaign — Campaign/v2

| Campo | Tipo no PDF | Página | SQL Server atual | Pendência |
| --- | --- | --- | --- | --- |
| CampaignId | INT | 17 | Não informado | Layout/nulabilidade atuais. |
| CampaignTypeId | INT | 17 | Não informado | Layout/nulabilidade atuais. |
| Description | NVARCHAR(510) | 17 | Não informado | Layout/nulabilidade atuais. |
| CampaignCode | NVARCHAR(100) | 17 | Não informado | Layout/nulabilidade atuais. |
| BillingCode | NVARCHAR(100) | 17 | Não informado | Layout/nulabilidade atuais. |
| DispositionPlanId | INT | 17 | Não informado | Layout/nulabilidade atuais. |
| Activated | INT | 17 | Não informado | Layout/nulabilidade atuais. |

### Disposition — Disposition/v2

| Campo | Tipo no PDF | Página | SQL Server atual | Pendência |
| --- | --- | --- | --- | --- |
| DispositionId | INT | 20 | Não informado | Layout/nulabilidade atuais. |
| Description | NVARCHAR(200) | 20 | Não informado | Layout/nulabilidade atuais. |


### Chaves, correlações e diferenças relevantes

- O manual declara PK para AttemptsRawDataID (p.17), InboundRawDataID (p.23), PbxBillingDataId (p.26) e UserId (p.30). Para CampaignId/DispositionId, descreve identificadores de referência; suas seções não declaram PK/UNIQUE. Não foram criadas constraints por inferência.
- As transacionais são diárias e imutáveis segundo o manual (p.1). A unicidade de cada PK **entre partições/dias e instâncias** não está demonstrada. Isso bloqueia escolher somente o ID como chave global de envelope sem evidência adicional.
- CallId é INT em Attempts/Inbound; CallIdTelecom é VARCHAR(16). Pbx usa outro nome, **TelecomCallid CHAR(16)**, e SessionId INT. Não há equivalência implementada entre esses campos ou entre suas PKs.
- Route é VARCHAR(256) em Attempts e INT em Pbx. Dnis é VARCHAR(255) em Attempts e VARCHAR(20) em Inbound/Pbx. CustomerId e OriginalPhoneNumber também têm comprimentos diferentes conforme a origem. Não renomear/unificar esses campos perdendo o contexto da tabela.
- Flags de Users e Activated são INT, mesmo quando as descrições usam 0/1. Não converter para boolean. CHAR preserva espaços; nenhum trim/padding novo é aplicado.
- Users contém UserId **e** AgentId. O manual cita AgentID de Users em várias correlações (por exemplo Attempts, p.16), mas UserID em Inbound (p.23). Documentar essa diferença e validar cardinalidade; não fazer joins automáticos nem equiparar esses identificadores.
- Todas as datas das seis seções são **DATETIME**, sem offset ou definição de timezone. Não presumir DATETIME2(7), UTC ou fuso deste host.
- Nenhuma das seis tabelas documentadas possui DECIMAL. Sua investigação permanece necessária como capacidade de aquisição, sem inventar campos monetários ou tipos para a instalação atual.
- Password consta como NVARCHAR(100) no manual e somente em excludedFields no catálogo v2. Não integra fields, fixture de payload ou envelope. Email/Login/UserName/CampaignData e telefones exigem revisão de coleta mínima antes de ativação; presença documental não é autorização de coleta indiscriminada.

## 4. Assinaturas documentadas versus verificadas no ambiente

Os parâmetros abaixo foram fornecidos pela empresa a partir de sys.parameters, em ordem e com o case informado; não foram obtidos por conexão deste worker. O catálogo v2 registra a origem dessa evidência e mantém `semanticsValidated=false`, `executionApproved=false` e `realExecutionAllowed=false`.

| Procedure | Assinatura do servidor informada | Páginas do manual |
| --- | --- | --- |
| ReadData | @TableName VARCHAR(128); @Message VARCHAR(MAX) OUTPUT; @ErrorMessage VARCHAR(MAX) OUTPUT | 4, 5 |
| ReadDataCurrentDay | @TableName VARCHAR(128); @Message VARCHAR(4000) OUTPUT; @ErrorMessage VARCHAR(4000) OUTPUT | 5, 6 |
| ReadDataPreviousDays | @TableName VARCHAR(128); @Message VARCHAR(4000) OUTPUT; @ErrorMessage VARCHAR(4000) OUTPUT | 6, 7, 8 |
| LogReadData | @tablename NVARCHAR(128); @DateExport DATE; @Message VARCHAR(4000) OUTPUT; @ErrorMessage VARCHAR(4000) OUTPUT | 10, 11 |
| ReReadData | @TableName VARCHAR(128); @DateExport DATE; @PKIDStart BIGINT; @PKIDEnd BIGINT; @Message VARCHAR(4000) OUTPUT; @ErrorMessage VARCHAR(4000) OUTPUT | 11, 12 |
| ResetReadData | @TableName VARCHAR(128); @DateExport DATE; @Message VARCHAR(4000) OUTPUT; @ErrorMessage VARCHAR(4000) OUTPUT | 8, 9 |
| DataAvailable | @TableName VARCHAR(128); @DateExport DATE | Não documentada |

### Divergências que precisam permanecer visíveis

| Procedure | PDF | Servidor, conforme coleta fornecida | Decisão |
| --- | --- | --- | --- |
| ReadData | Message/ErrorMessage VARCHAR(4000), p.4. | Ambos VARCHAR(MAX). | Preparar pelo tipo verificado, sem truncar para 4000; limite de memória independente. |
| LogReadData | Tabela de parâmetros p.10: TableName VARCHAR(128), ErrorMessage VARCHAR(MAX). Exemplo p.11: tablename NVARCHAR(128), outputs VARCHAR(4000); descrição de retorno p.10 também usa 4000. | tablename NVARCHAR(128), ErrorMessage VARCHAR(4000). | Preservar case e Unicode do servidor; registrar contradição interna do manual. |
| ReReadData | Tabela p.11: ambos os outputs VARCHAR(MAX); retorno/exemplo p.12: VARCHAR(4000). | Ambos VARCHAR(4000). | Usar a assinatura fornecida, sem apagar as duas evidências documentais. |
| ResetReadData | Descrição p.8 escreve DataExport; tabela/exemplo usam DateExport. | DateExport DATE. | Não inventar parâmetro/alias DataExport. Continua bloqueada. |
| DataAvailable | Ausente nas 33 páginas. | TableName VARCHAR(128), DateExport DATE; nenhum OUTPUT informado. | Somente catálogo; nenhuma inferência de comportamento ou tentativa de chamada. |

ReadDataCurrentDay, ReadDataPreviousDays e a tabela de parâmetros de ResetReadData coincidem com o relato do servidor. O manual usa exemplos de Linked Server, TRUNCATE e timeouts zero; esses exemplos não foram executados, copiados como comandos operacionais ou adotados como configuração do worker.

### Bytes, unidades UTF-16 e MAX

Conforme a documentação de [sys.parameters](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-parameters-transact-sql), max_length é medido em bytes e -1 representa tipos MAX. Para [NVARCHAR](https://learn.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql), n é quantidade de pares de bytes/unidades UTF-16, e pode ser diferente da quantidade de caracteres Unicode percebidos.

- NVARCHAR(128) corresponde a **256 bytes de max_length esperado**, não NVARCHAR(256). Esse 256 é derivado da declaração fornecida, não uma nova medição de catálogo feita nesta tarefa.
- `textTypeFromCatalog('nvarchar',256)` produz length=128. Bytes ímpares/inválidos são recusados. VARCHAR(128) mantém 128 bytes; -1 produz MAX.
- O coletor normaliza comprimentos em bytes do metadado mssql quando presentes, inclusive o marcador MAX do TDS, preservando o tipo textual. Metadado sem comprimento não ganha um comprimento inventado.
- NVARCHAR(510), como escrito no PDF, permanece assim no catálogo. Não se divide automaticamente esse número por dois: a conversão aplica-se a metadados **sabidamente em bytes**, não à declaração impressa. Verificar o layout atual antes de resolver eventual divergência histórica.

## 5. Codecs e bloqueios implementados

### Parâmetros

- VARCHAR/NVARCHAR agora aceitam tamanho explícito ou MAX. MAX é traduzido para sql.MAX na preparação do Request. O cliente conserva o conteúdo completo ou rejeita por limite de memória; não trunca mensagens.
- VARCHAR de entrada permanece restrito a ASCII até confirmação da codepage. Para os seis nomes de tabela isso basta. NVARCHAR valida unidades UTF-16, conservando Unicode e sem normalização.
- DATE é recebido como string civil YYYY-MM-DD válida, incluindo ano 0001 e anos bissextos, sem aceitar Date, offset, formato local ou timestamp. O valor original permanece string em PreparedCall.
- Somente no binding de DATE para a API mssql é criado um Date à meia-noite UTC. O preparador exige **useUTC=true na configuração efetiva do parent do Request** antes de preencher parâmetros; false ou parent ausente falham. Isso é transporte de uma data civil de entrada, sem habilitar conversão de timestamps recebidos para Date.
- Null de DATE é aceito somente com política explícita. O manual permite DateExport=null em LogReadData para todas as datas; isso não prova default SQL, nem autoriza consultar todas as datas automaticamente. Demais nulabilidades dos parâmetros não foram fornecidas.
- BIGINT exige string signed int64, sem number. Os intervalos de ReReadData são parametrizados assim, mas a operação real permanece bloqueada.
- `reportedSignature` exige política de aceitação de null para todos os parâmetros; ela não representa constraints observadas. Não produz assinatura executável de DataAvailable, que não possui os OUTPUTs exigidos pelo cliente e permanece proibida.

### Valores dos datasets

O adaptador agora valida o intervalo INT/SMALLINT/TINYINT também quando o valor já chega em string; assim um valor de 64 bits não atravessa um campo documentado/contratado como INT por acidente. Continua permitindo BIGINT quando esse é o tipo efetivamente contratado. Comprimentos declarados no contrato são conferidos, sem alterar strings; o teto lexical de texto não substitui certificação de codepage/bytes de VARCHAR.

Para DATETIME declarado como texto exato, o adaptador recusa datas anteriores a 1753 e frações decimais que não correspondam exatamente a ticks de 1/300 segundo. `.010` é representável exatamente; `.003` é um display arredondado e não certifica o tick original. Valores não representáveis dessa forma exigem o codec bruto ainda não integrado. Isso não reduz DATETIME2 a DATETIME nem aprova uma conversão por Date.

Os contratos completos são exercitados somente com políticas de nulabilidade/chave **explicitamente sintéticas**. Flags ficam inteiros textuais; null/vazio/ausente continuam distintos; tipos/metadados/campos desconhecidos rejeitam o lote. Password continua proibido na persistência.

Todos os gates nativos da Fase 3A foram preservados: nenhuma variável ou assinatura conhecida os libera. SYNC_ENABLED=false permanece obrigatório e true continua rejeitado. Não há CLI de consumo, consulta de catálogo, aprovação de contrato ou runner de migrations novo.

## 6. Investigação de aquisição exata anterior ao parser

Foi inspecionado o código local de node-mssql/Tedious e acrescentado um teste que chama **somente readValue sobre buffers sintéticos**, sem socket/servidor. Isso reproduz a fronteira problemática diretamente no parser instalado:

| Tipo | Evidência reproduzida | Tratamento atual |
| --- | --- | --- |
| DECIMAL/NUMERIC | 9007199254740993 vira number 9007199254740992 no parser. | Rejeição do caminho nativo; protótipo offline reconstrói decimal com BigInt, sinal e escala a partir do corpo binário anterior ao parser. |
| DATETIME | Um tick de 1/300 segundo vira Date com 3 milissegundos. | Protótipo offline preserva dias desde 1900, ticks300 e bytes originais em hex; não arredonda essa tupla para timestamp decimal. |
| DATETIME2(7) | ISO perde fração submilissegundo; nanosecondsDelta existe, mas não sobrevive a structuredClone. | Date continua recusado. Fração completa precisa ser adquirida antes da conversão. |
| DATETIMEOFFSET | Buffers com offsets -180 e +180 minutos produzem o mesmo Date quando os demais componentes são iguais. | Offset original não é recuperável por toISOString; caminho nativo continua bloqueado. |

As unidades/formato usados pelos protótipos correspondem à especificação [MS-TDS para decimais](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/5e02042c-a741-4b5a-b91d-af5e236c5252), ao [MS-TDS](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/b46a581a-39de-4745-b076-ec4dbb7d13ec) e ao parser local inspecionado. São codecs de **corpo de valor**, sem framing, TLS, parser de tokens, associação de colunas ou integração ao transporte.

O protótipo DATETIME é relevante às seis tabelas: frações de 1/300 segundo nem sempre têm representação decimal finita. Uma string civil limitada a sete casas no envelope existente não é prova de preservação do tick interno. A futura solução deve decidir e versionar a representação exata do DATETIME, por exemplo preservando a tupla/bytes como representação interna aprovada. **Essa tupla não foi conectada ao envelope nem aprovada automaticamente como novo tipo de origem.**

Não foi alterado node_modules, instalado handler global, aplicado monkey patch ou criado wrapper SQL/CAST sobre procedures. O valueHandler de mssql atua depois do parser Tedious e não recupera dígitos/offset já perdidos. O transporte `exact-text/v1` permanece uma capacidade exigida, ainda não certificada em ambiente real.

Alternativas para revisão futura: extensão/versionamento de um transporte que exponha valores binários antes de readValue, ou uma serialização textual/binária autorizada no servidor preservando OUTPUTs, ordem, limites e efeitos das procedures. Não se presume que INSERT EXEC, wrappers, permissões ou SELECTs arbitrários sejam permitidos ao OEC. Nenhuma dessas alternativas foi executada nesta fase.

## 7. Semântica documentada, riscos e recuperação

Fatos do manual, separados de validação na instalação:

- p.1: transacionais diárias, alimentação incremental, dados imutáveis após geração e expurgo automático; **prazo de retenção não informado**.
- p.2: estáticas não particionadas, mutáveis por merge e sem expurgo automático.
- pp.4–5: ReadData retorna todos os registros de uma estática, sem limite documentado.
- pp.5–8: CurrentDay retorna não lidos do dia corrente e PreviousDays não lidos de dias anteriores, até **200.000 registros por execução**. Não há parâmetro de cursor, data ou limite nessas assinaturas.
- pp.3,8–12: auxiliares não devem ser usadas recorrentemente. Reset reabre a leitura da tabela/data. Log expõe intervalos; ReRead lê registros já consumidos por tabela/data/intervalo, com teto de 200.000.
- p.10: LogReadData retorna TableName VARCHAR(128), PKColumnName VARCHAR(128), DateExport DATE, PKIDStart BIGINT, PKIDEnd BIGINT, DateRead DATETIME e Quantity INT. Esse layout documental não confirma tipos atuais e não contém um identificador inequívoco deste worker.

Riscos que continuam bloqueando ingestão incremental real:

1. **Volume e consumo remoto:** o coletor nativo preparado tem default de 1000 registros/4 MiB; cliente e persistência têm teto máximo de 100.000 registros. A origem pode devolver 200.000 sem parâmetro de redução. Cancelar/descartar após o limite pode ocorrer depois de avanço remoto. Não foram elevados limites nem introduzidos commits parciais por suposição. É necessário projetar e testar recepção/spool durável/backpressure compatível antes do incremental.
2. **Data/partição da resposta:** PreviousDays pode tratar dias anteriores, mas o layout prioritário não contém uma coluna DateExport de controle. StartDate/CallStart não são automaticamente a data da partição. A Fase 2A tem stream por tabela/tipo/data; a associação correta do lote a uma data e a chave sem colisão entre dias ainda precisam de evidência. O timezone do “dia corrente” também não foi fornecido.
3. **Ausência de cursor do chamador:** não converter maior PK ou checkpoint local em parâmetro inexistente. Não usar ReRead como paginação normal. Ordenação, completude, inclusividade de intervalos, comportamento acima de 200 mil e término de repetição não estão comprovados operacionalmente.
4. **Controle compartilhado:** mutex/lease PostgreSQL protege participantes deste banco de controle; não impede outros consumidores OEC nem cerca uma procedure já em voo. O erro documentado de sessão concorrente em PreviousDays não prova exclusividade global nem reserva de dados para este worker.
5. **Janela leitura → commit:** um crash depois da leitura e antes da persistência pode perder o payload em memória. Intent/log de intervalo não são cópia dos dados. Retenção/expurgo e possibilidade real de reobter o lote precisam ser validados.
6. **COMMIT sem ACK:** consultar o recibo do mesmo lote ou reprocessar somente a persistência retida de forma idempotente. Ausência de recibo não autoriza nova leitura incremental. Checkpoint de ingestão não é o controle remoto da Olos.
7. **Precisão e deriva de layout:** DATETIME e eventual DECIMAL exigem aquisição anterior ao parser. Nullability, encoding/collation, atualização da versão Olos e novos campos podem invalidar o contrato antigo; rejeitar drift, sem preencher null por conveniência.

Estratégia de recuperação proposta: confirmar intent antes da futura chamada; preservar lote completo/proveniência antes de avançar checkpoint local; em falha incerta, interromper novas leituras da tabela, conservar estado needs_reconciliation e investigar LogReadData mediante autorização específica. Associar tabela/data/intervalo/contagens ao caso, considerando outros consumidores. ReRead exige plano explícito aprovado e validação de retenção/completude; Reset não é mecanismo automático de retry. Não fechar caso somente porque um log existe.

**Não há garantia comprovada de ausência de perda, exactly-once ou recuperabilidade integral.** O manual melhora o contrato, mas não resolve sozinho as janelas entre sistemas.

## 8. Plano da primeira leitura real controlada — não executado

### Pré-condições comuns

1. Autorizar separadamente um diagnóstico de metadados sem consumo, se necessário: identidade do servidor/instalação, assinaturas com bytes/direção/ordem, layout de retorno/colunas, nulabilidade, chaves e escopo, encoding/collation, versão e retenção. Não executar procedures apenas para descobrir o layout.
2. Registrar diferenças para o PDF, aprovar uma nova versão operacional com evidências e política de coleta mínima. Não alterar uma versão usada em operação ou transformar approved=false em true por presença no manual.
3. Validar aquisição exata no transporte escolhido e a representação persistida; testar bordas numéricas/temporais, nulls, codificação e perda de conexão/ACK em SQL Server isolado ou ambiente especificamente autorizado.
4. Coordenar consumidores, delimitar tabelas/datas/volume e estabelecer critérios de parada. Preparar um PostgreSQL de ensaio explicitamente isolado; aplicação manual de controles/permissões depende de autorização própria. Não apontar automaticamente para banco corporativo.
5. Revisar pool, TLS, useUTC para DATE, timeouts, drenagem, renovação/fencing, memória/disco para o maior lote, shutdown e monitoramento sem PII. Não contornar as proteções para viabilizar o teste.

### Primeira etapa recomendada: referência pequena

Depois das pré-condições e de nova autorização, considerar **Disposition via ReadData**, pois o manual possui somente DispositionId INT e Description NVARCHAR(200), sem data ou senha. Confirmar no ambiente o escopo/chave/nulabilidade, tamanho integral e ausência de campos extras antes do plano. ReadData é snapshot completo sem limite: não prometer TOP/amostragem que a assinatura não oferece.

Essa etapa valida conexão, parâmetro VARCHAR(128), OUTPUTs MAX, Unicode, persistência e conciliação de contagens/digests em banco de ensaio. Confirmar a política de sucesso do retorno SQL; o PDF não define returnValue=0 como regra suficiente. Não usar essa referência para afirmar que o incremental já é seguro.

### Etapa incremental posterior

Somente depois de resolver os bloqueios de volume, precisão, partição/data e recuperação: ensaio autorizado com **uma tabela, uma chamada e um plano de recuperação previamente aprovado**. Confirmar estado de leitura/log antes/depois por processo coordenado e não recorrente. Não escolher arbitrariamente uma data para PreviousDays, não simular cursor inexistente e não chamar DataAvailable. Parar em erro OUTPUT, drift, limite, timeout, lease expirada ou confirmação incerta, mantendo evidência e sem retry remoto automático.

### Critérios de aceite dessa leitura futura

- Nenhum contrato pendente, divergência silenciosa ou fonte de valor reconstruída a partir de number/Date arredondados.
- Nomes/tipos/comprimentos/NULL conferem com a versão aprovada; PKs preservadas e escopo de deduplicação comprovado; senha ausente de payloads/logs.
- Registros recebidos, persistidos e duplicados conciliados; checkpoint somente com commit correspondente; verificação completa de conteúdo exato sem logs de PII.
- Crash/timeout/ACK perdido têm procedimento ensaiado; replay local usa o mesmo lote; releitura remota apenas sob plano autorizado e comprovadamente completo.
- Uma leitura ainda em voo não é liberada por TTL; não há escritor antigo contra geração nova ou consumidor não coordenado avançando o controle.
- Gates de ativação e readiness só mudam em entrega futura revisada, com autorização operacional explícita. **Esta entrega não habilita esse teste.**

## 9. Implementação, validação e isolamento

Criados: `src/olos/documented-tables.ts`, `procedure-signatures.ts`, `parameter-codecs.ts`, `exact-value-prototypes.ts`; quatro testes `olos-documented-contracts`, `olos-parameter-codecs`, `olos-preparser-precision`, `olos-documented-ingestion`; suporte `test/support/olos-documented-fixture.ts`; este relatório e o PDF em docs/reference.

Alterados: `src/olos/contracts.ts` (histórico/reexports), `types.ts` (DATE/MAX/comprimento), `client.ts` (binding e validação), `mssql-collector.ts` (normalização de comprimento), `adapter.ts` (intervalos/comprimentos) e README (referência documental).

Preservados por comparação SHA-256 com baseline: todos os testes/suportes anteriores, as quatro migrations, permissões, persistência, projeção, boot, configuração, pools de diagnóstico, scheduler, package.json e lockfile. Nenhuma migration nova ou aprovação real foi criada.

| Comando | Resultado |
| --- | --- |
| `npm run typecheck` | Aprovado, exit 0. |
| `npm run build` | Aprovado, exit 0. |
| `npx vitest run test/olos-documented-contracts.test.ts test/olos-parameter-codecs.test.ts test/olos-preparser-precision.test.ts` | 67 testes aprovados na primeira rodada, antes dos casos complementares de OUTPUT/DATETIME. |
| `npm test` final | **297 testes em 19 arquivos aprovados**, exit 0; 221 anteriores + 76 novos. |

As seis ingestões novas usam layouts do PDF com nulabilidade e identidade aprovadas **somente para registros gerados na fixture**, datas em segundos inteiros e assinaturas fornecidas pela empresa. Não representam contratos reais aprovados ou uma prova de transporte SQL Server exato. Verificam envelopes completos, contagens/duplicatas, Unicode, CHAR com espaço, ausência de senha e checkpoint usando o role limitado em PostgreSQL descartável.

O helper PostgreSQL anterior foi preservado: cria/remover seu próprio container PostgreSQL 16, banco fixo de fixture, tmpfs, senha/porta efêmeras e exposição somente em 127.0.0.1. Não aceita URL corporativa/fallback. Os buffers TDS e Request sem conexão são exclusivamente sintéticos; as consultas do teste não acessam a Olos.

Após a suíte final, a listagem Docker pelo label exclusivo do fixture não encontrou containers remanescentes. O build gerou dist ignorado. O checkout já continha arquivos untracked antes desta fase; não houve staging ou mudança desse estado por commit.

**Nenhuma procedure real, extração real, consulta de metadados remotos, migration/INSERT corporativo, alteração de infraestrutura/WireGuard/discadora, commit, push ou deploy foi executado. SYNC_ENABLED permanece false.**
