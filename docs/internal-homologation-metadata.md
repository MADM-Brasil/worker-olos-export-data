# Diagnóstico corporativo temporário — conectividade e metadados

Esta CLI é separada do worker: `diagnose:olos-metadata`. O destino é exclusivamente `10.196.234.11:1433`, banco `ExportDataV3`, usuário `OEC`. Não existe opção para outro servidor, SQL livre, aquisição ou scheduler. Nenhuma conexão corporativa foi realizada na implementação.

## Modos TLS

- `tlsMode: "pinned"`: padrão e modalidade definitiva. Reutiliza `PinnedMessageIO` e o PEM/fingerprint aprovados do mecanismo existente, com propósito **OLOS_TDS_TLS_METADATA_ONLY**. Nunca migra automaticamente para confiança temporária.
- `tlsMode: "internal-homologation"`: excepcional e somente nesta CLI. Usa TLS 1.2 criptografado, com `trustServerCertificate=true` restrito à sessão diagnóstica. Requer responsável, referência, emissão/expiração (máximo quatro horas) e `--confirm SERVER_IDENTITY_NOT_VALIDATED`. A identidade do servidor **não é validada**; criptografia sem autenticação não impede interceptação. Em `describe`, as credenciais SQL são enviadas nessa sessão de identidade não validada. Utilizar somente na rede interna autorizada e sob aceite desse risco.

Não existe alteração global de TLS, variável genérica habilitadora, downgrade ou mudança em node_modules. A validade temporal do certificado é conferida antes de LOGIN7 em `describe`. O modo `connect` reutiliza a inspeção informativa PRELOGIN/TLS existente e não autentica no SQL Server.

## Configuração privada

Criar manualmente, fora do checkout, `~/.config/olos-worker/metadata-diagnostic.json`, proprietário atual, permissão 0600, sem symlinks. Não reutilizar nem alterar `attempts-corporate.json`. Preencher os instantes UTC reais com janela curta e a referência do aceite operacional; os textos abaixo são placeholders, não uma aprovação emitida pelo projeto:

```json
{
  "version": 1,
  "tlsMode": "internal-homologation",
  "server": "10.196.234.11",
  "port": 1433,
  "database": "ExportDataV3",
  "user": "OEC",
  "password": "PREENCHER_SOMENTE_NO_ARQUIVO_PRIVADO",
  "authorization": {
    "operator": "RESPONSAVEL",
    "reference": "REFERENCIA_DO_ACEITE",
    "issuedAt": "INSTANTE_UTC_ISO8601",
    "expiresAt": "INSTANTE_UTC_ISO8601"
  }
}
```

`password` é obrigatório somente para `describe`; pode ser omitido do arquivo usado apenas por `connect`. Não passar senha em argumentos, salvar em Git ou exibir o arquivo em logs. Para `pinned`, usar `tlsMode: "pinned"` e `pinApprovalFile` absoluto para aprovação de diagnóstico vigente; não usar o campo de autorização temporária como substituto de aprovação do pin.

## Comandos manuais, somente após autorização

Conectividade e certificado, **sem LOGIN7 nem SQL**:

```bash
cd ~/worker-olos-export-data
SYNC_ENABLED=false npm run diagnose:olos-metadata -- connect \
  --config "$HOME/.config/olos-worker/metadata-diagnostic.json" \
  --authorize OLOS_NON_CONSUMPTIVE_METADATA \
  --confirm SERVER_IDENTITY_NOT_VALIDATED
```

Metadados das **duas** procedures, sem executar suas leituras:

```bash
SYNC_ENABLED=false npm run diagnose:olos-metadata -- describe \
  --config "$HOME/.config/olos-worker/metadata-diagnostic.json" \
  --authorize OLOS_NON_CONSUMPTIVE_METADATA \
  --confirm SERVER_IDENTITY_NOT_VALIDATED
```

Uma conexão autenticada, sem retry, com dois RPCs fixos a `sys.sp_describe_first_result_set`. O argumento `tsql` contém a chamada de CurrentDay ou PreviousDays com `TableName='AttemptsRawData'` e outputs declarados; esse texto é **analisado, não executado**. O driver também aplica seus SETs de inicialização de sessão. Não há SELECT em dados operacionais, Reset, ReRead, Users ou escrita PostgreSQL. Timeout de conexão 10s, por descrição 15s, total 45s. Falha na primeira descrição interrompe a sequência; não há fallback para executar a procedure.

O JSON contém somente uma allowlist de metadados: ordinal, nome, tipo, comprimento, precisão/escala, nulabilidade e identificadores de collation/TDS. Limites: 512 colunas por resultado e 2048 bytes por valor textual de metadado. Erros SQL retornam apenas código sanitizado e número, sem mensagem original, stack ou payload. Nenhuma senha ou registro pessoal é impresso.

## Interpretação e limites

- `serverIdentityValidated:false` é obrigatório no modo temporário, mesmo quando a descrição funciona. `acquisitionAuthorized:false` e `schedulerEnabled:false` permanecem explícitos. Código de saída zero significa conclusão do diagnóstico, não TLS autenticado nesse modo.
- A descrição pode falhar por permissões, SQL dinâmico ou tabelas temporárias; também pode produzir nulabilidade conservadora. OEC ter EXECUTE não garante que a análise estática determine todas as colunas. `METADATA_DESCRIPTION_FAILED`/número SQL ou `METADATA_NOT_DETERMINED` exigem evidência do DBA/Olos; não são motivo para executar leituras incrementais.
- Identificadores de collation descritos não são promovidos automaticamente a um `collationHex` aprovado. Revisar o resultado contra o contrato TDS efetivo, inclusive CurrentDay versus PreviousDays, antes de aprovar aquisição. Este comando não cadastra contratos nem comprova recuperação, completude ou propriedade de faixas.
- A modalidade temporária não é aceita pela configuração corporativa de aquisição. Os gates, intents, spool, recibos, deduplicação e política para resultados remotos incertos não foram alterados. Nenhum dado histórico/PostgreSQL foi acessado.
- Ao corrigir o certificado, retirar o arquivo/aceite temporário e executar o diagnóstico em `pinned` com aprovação independente atualizada (sem `--confirm` temporário). O mecanismo de pin existente mantém suas restrições próprias; nova cadeia/certificado exige revisão, sem aceitação automática.

## Alternativa de catálogo após erro 11514

O erro 11514/EREQUEST em `phase=describe` foi informado pelo operador; não foi repetido. Não executar novamente `describe` para esta investigação. Existe agora uma ação separada, sem fallback para análise estática ou procedures de consumo:

```bash
SYNC_ENABLED=false npm run diagnose:olos-metadata -- catalog \
  --config "$HOME/.config/olos-worker/metadata-diagnostic.json" \
  --authorize OLOS_NON_CONSUMPTIVE_METADATA \
  --confirm SERVER_IDENTITY_NOT_VALIDATED
```

Reutiliza exatamente a conexão, TLS, credencial privada e autorização vigentes; não renova o aceite. Uma sessão e um SELECT parametrizado fixo, via `execSql`/sp_executesql do driver, somente em `[ExportDataV3].sys.objects`, `.sys.schemas`, `.sys.columns` e `.sys.types`, com conferência de `DB_NAME()`. Não há SQL livre, SELECT em tabelas de negócio ou acesso a outros bancos. Não chama sp_describe_first_result_set, CurrentDay, PreviousDays, Log, ReRead ou Reset. O limite de 512 linhas de metadados é explícito: uma 513ª linha causa rejeição, sem aceitar resultado truncado.

**Evidência documental revisada:** o manual DBA-MN002, página 6, mostra `INSERT INTO dbo.AttemptsRawData EXECUTE LinkedServerOlos.ExportDataV3.dbo.usp_ExportData_ReadDataCurrentDay ...`. Nesse exemplo, `dbo.AttemptsRawData` é o destino do cliente, enquanto a origem é a procedure remota. As páginas 16–17 documentam o nome lógico e os 18 campos. Não foi localizada evidência de um schema/tabela/view físico atual por trás dessa interface na instalação corporativa. Os objetos SQL Server criados em testes são fixtures, não evidência corporativa.

Por isso, a consulta busca **candidatos visíveis** com o nome lógico exato `AttemptsRawData` em qualquer schema, apenas tipos U/V. Não pressupõe `dbo`, partições com nomes inventados ou acesso direto à tabela. A comparação de nome segue a collation do servidor; não se fazem buscas amplas, em outros bancos ou via definição das procedures.

Resultados:

- `CATALOG_CANDIDATES_VISIBLE` (exit 0): nomes/schema/tipos e colunas visíveis; ainda `sourceObjectRelationshipConfirmed=false` e `contractAutomaticallyApproved=false`. Não seleciona automaticamente um objeto se houver mais de um.
- `CATALOG_OBJECT_NOT_VISIBLE_OR_ABSENT` (exit 2): nenhum U/V visível com esse nome. Pode estar oculto, ausente, ter outro nome ou residir fora do escopo permitido. **Não prova inexistência e não identifica qual dessas hipóteses ocorreu.**
- `CATALOG_COLUMNS_NOT_VISIBLE` (exit 2): objeto visível, porém sem metadados completos de colunas.
- `METADATA_CATALOG_PERMISSION_DENIED` (exit 1): SQL 229/230 explícito; somente número/código técnico, sem mensagem SQL.
- Outras falhas: `METADATA_CATALOG_FAILED` ou `METADATA_QUERY_TIMEOUT`, com `phase=catalog`, sem retry ou fallback.

OEC foi informado como tendo CONNECT/EXECUTE, sem SELECT/VIEW DEFINITION. EXECUTE nas procedures não garante visibilidade dos objetos internos. As views de catálogo restringem metadados aos objetos possuídos ou com permissões: consultar o catálogo pode funcionar e retornar zero linhas. Ver [sys.columns / permissões](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-columns-transact-sql?view=sql-server-ver16).

Mesmo com candidatos, `column_ordinal` nessa saída é `sys.columns.column_id`, não a ordem comprovada do resultado de uma procedure; pode conter lacunas. Tipos/nulabilidade do objeto não comprovam casts/aliases/joins do resultado, unicidade da PK ou completude do consumo. `max_length` está em bytes (-1 para tipos MAX aplicáveis); collation/codepage não são promovidos automaticamente a bytes TDS aprovados.

Se não houver visibilidade, o próximo passo é pedir ao DBA/Olos o banco/schema/objeto real, seus metadados e a relação com o resultado das duas procedures. Não ampliar permissões de dados, inventar nomes ou executar leituras para contornar o bloqueio. Nenhuma consulta corporativa foi realizada nesta implementação; o teste dessa ação é sintético.

## Validação local

Correção do diagnóstico: o valor anterior `connectionRetryInterval: 0` era recusado pelo construtor real do Tedious 18.6.2, antes de rede/TLS/LOGIN7. A opção foi removida; `maxRetriesOnTransientErrors: 0` e o bloqueio de segunda conexão/redirecionamento permanecem. Um teste com o construtor real, sem chamar `connect`, reproduz a rejeição antiga e aceita a configuração corrigida. O mock anterior não exercitava essa validação.

Falhas agora preservam a etapa em `phase` e somente códigos técnicos autorizados (`driverCode`, `tlsCode`) e número SQL inteiro, quando disponíveis. Mensagens SQL/driver, causas completas, stacks e configurações não são serializadas:

| Situação | Código |
| --- | --- |
| Configuração/JSON inválido ou opção recusada no construtor | `METADATA_CONFIGURATION_REJECTED` |
| Handshake TLS | `METADATA_TLS_HANDSHAKE_FAILED` (ou `METADATA_TLS_REQUIRED` para o gate de criptografia/protocolo) |
| Autenticação LOGIN7 | `METADATA_LOGIN7_FAILED` |
| SETs/inicialização da sessão | `METADATA_SESSION_INITIALIZATION_FAILED` |
| Execução da descrição estática | `METADATA_DESCRIPTION_FAILED` |
| Consulta ao catálogo | `METADATA_CATALOG_FAILED` / `METADATA_CATALOG_PERMISSION_DENIED` |
| Timeout antes de concluir a conexão/sessão | `METADATA_CONNECTION_TIMEOUT` |
| Timeout durante a descrição | `METADATA_QUERY_TIMEOUT` |

Os códigos específicos de autorização/pin continuam preservados. `connect` não exercita o construtor da sessão autenticada; seu sucesso não demonstrava que `describe` chegasse a LOGIN7. A correção local não comprova sucesso futuro de login, permissões ou descrição no ambiente corporativo.

Testes direcionados exercitam configuração/autorização, endpoint fixo, TLS mínimo, duas chamadas exclusivamente de análise estática, parada sem retry, sanitização e rejeição pelo contrato de aquisição. A sessão SQL é mockada; os testes TLS existentes usam certificados e peer TDS/TLS sintéticos locais. Esses testes **não certificam** SQL Server/Olos corporativo nem substituem a primeira execução diagnóstica autorizada.
