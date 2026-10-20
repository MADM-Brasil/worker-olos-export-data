# Preparação do transporte corporativo Attempts com pinning

Evolução posterior: [entrada corporativa once/start](corporate-attempts-worker.md). Este relatório descreve o estado anterior, antes do emissor restrito de tentativa durável; as pré-condições operacionais não são aprovadas automaticamente.

19/10/2026. Preparação de código, **não autorização de aquisição**. Não foram executados SQL, autenticação SQL, stored procedures ou conexões corporativas. Os testes só abriram sockets de loopback com certificados sintéticos. Não houve acesso ao PostgreSQL ou aos 570 históricos, run-001/run-002, VPS ou infraestrutura.

## Integração mínima

O caminho existente continua `LargeBatchCoordinator → IsolatedTdsSource → ExactConnection → MessageIO → ExactTokenParser`. Não foi criado outro transporte/parser/spool. O nome histórico IsolatedTdsSource foi mantido para evitar refatoração; um endpoint corporativo preparado é identificado como kind=mssql.

`ExactConnection.socketHandlingForSendPreLogin` instala o **mesmo** PinnedMessageIO por instância, antes de enviar PRELOGIN. Preserva eventos de socket, transições do Tedious e streams existentes. PinnedMessageIO agora emite `secure` após validar o canal, para que o transporte registre a segurança. O caminho normal sem pin continua delegando ao driver. O parser exato e o backpressure não foram alterados.

Uma opção de fixture permite testar esse hook em 127.0.0.1/phase3d_fixture, exclusivamente com TLS verificado e pin aprovado. Ela não autoriza endpoint corporativo. O teste usa a Connection real do Tedious até o ponto anterior ao LOGIN7: não usa SQL Server, autenticação ou RPC e não certifica compatibilidade da instalação Olos.

## Capacidade preparada e bloqueada

`prepareCorporateAttemptsEndpoint` aceita apenas:

- 10.196.234.11:1433 / ExportDataV3 / OEC; senha fornecida pelo chamador, nunca logada.
- Aprovação privada de certificado emitida pelo mecanismo existente, com propósito **OLOS_ATTEMPTS_TDS_ACQUISITION**. O propósito antigo OLOS_TDS_TLS_METADATA_ONLY não pode ser promovido automaticamente.
- Assinatura oficial verificada em sys.parameters, parâmetros tipados e TableName/tablename=AttemptsRawData.
- ReadDataCurrentDay, ReadDataPreviousDays, LogReadData ou ReReadData. ReadData, Users, outras tabelas, ResetReadData e DataAvailable são recusadas neste endpoint.

Endpoint congelado e registrado em WeakMap; cópia/JSON serializado não conserva a capacidade. A intenção é vinculada a assinatura, parâmetros e timeout. Configuração de Connection exige encrypt=true, trustServerCertificate=false, TDS 7.4, TLS 1.2, sem connector, instancia nomeada, múltiplos subnets ou retries. Nenhum destino alternativo ou parâmetro genérico de runtime habilita esse caminho.

**A capacidade é somente de preparação.** `assertCorporateAttemptsExecution` sempre recusa com ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED nesta release. Há guardas antes de connect, abertura de socket, PRELOGIN, LOGIN7 e RPC. O coordenador também verifica a capacidade antes de startRun/preflight/spool/persistência, evitando criar efeitos locais para uma origem ainda proibida. A composição automática continua aceitando somente bindings isolated-fixture.

O TLS usa o PEM público aprovado como âncora exclusiva, rejectUnauthorized=true, SHA-256 DER/timingSafeEqual e validação de validade/expiração. Não há trustServerCertificate, relaxamento global, CA global, downgrade ou monkey patch. Ver [pinning manual](olos-certificate-pinning.md) para formato do arquivo 0600, aprovação independente, prazo máximo de sete dias e rotação. Para aquisição, criar uma aprovação **separada** com o propósito acima; isso não emite autorização de consumo. Nenhuma aprovação real foi criada ou carregada nesta tarefa.

O diagnóstico continua aceitando apenas aprovações METADATA_ONLY. Para comprovar TLS sem SQL, usar o comando diagnose:olos-tls documentado anteriormente, com aprovação própria e conexão autorizada separadamente. Um sucesso nesse diagnóstico não resolve automaticamente o gate TLS da aquisição nem comprova o banco, layout ou recuperação.

## Recuperação preparada

- `planAttemptsRecovery` mantém a revisão existente e seu escopo isolated-fixture.
- `planCorporateAttemptsRecovery` reutiliza a mesma validação de origem parcial, digest, data, faixa exata, interseções ambíguas, quantidade, PK documentada INT e revisão explícita, mas marca o plano **corporate-prepared-blocked**.
- Ambos exigem identidade AttemptsRawData/AttemptsRawDataID; intervalos com CallId ou quantidades não inteiras não servem para recuperação.
- `prepareCorporateAttemptsReRead` gera somente uma intenção tipada de ReReadData com DATE e limites BIGINT exatos, vinculada ao digest do caso/log e à revisão. Não usa assinatura de fixture, não modifica checkpoint nem promove spool parcial.
- `rereadLargePlan` da fixture rejeita planos corporativos; não há passagem de um plano corporativo para execução de ensaio.
- `prepareCorporateAttemptsRecoveryPort` conecta a preparação à classe OlosReconciliation e interface OlosRecoveryPort já utilizadas por LargeBatchCoordinator.planRecovery. Valida configuração/pin/assinaturas, sem inventar datas ou faixas. A inspeção remota permanece bloqueada e registra falha pela porta de auditoria injetada; não fabrica reply de sucesso.
- O decoder existente decodeLogRecord continua recebendo somente registros de spool com layout exato. Não houve confirmação do layout corporativo atual nesta entrega.

LogReadData comprova histórico de leitura, **não o consumidor responsável**, completude ou persistência. O resultado do planejamento declara remoteOwnershipProvenByLog=false e executionAllowed=false. Textos de revisão/digests correlacionam o caso; não comprovam sozinhos que a evidência operacional é verdadeira.

Falha de leitura mantém o caso e spool parcial. Spool selado é recuperado apenas localmente pelo coordenador existente. Commit sem ACK depende do recibo/digest, não de nova chamada Olos. Lease expirada/hard crash e ausência de atribuição de faixa continuam exigindo conciliação. Não há retry remoto automático ou ResetReadData.

## Bloqueios: código versus instalação

Resolvido no código: ponto de instalação do IO com pin na Connection real, propagação do evento TLS, aprovação distinta de aquisição, endpoint/procedure/intenção restritos, bloqueio antes de efeitos locais, adaptação do planejamento/auditoria de recuperação e separação entre plano corporativo e fixture.

Ainda não resolvido/liberado:

1. PEM/fingerprint aprovados independentemente e handshake corporativo autenticado comprovado. O certificado apenas observado não é aprovação.
2. Layout atual, nulabilidade, collation, saídas/retorno e política de sucesso das procedures corporativas.
3. Data de partição/fuso/escopo da PK, controle compartilhado, atribuição de intervalos a consumidores e coordenação com outros usos.
4. Inclusividade das faixas, retenção, completude de ReReadData e política de recuperação após lease expirada/hard crash.
5. Emissão de um permit executável específico, claim durável anterior à tentativa, validação do destino e ligação da CLI/runtime a esse permit. Não foi criado um emissor executável nem removido o gate existente.

Permanecem ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED, CORPORATE_TLS_CHAIN_AND_NAME_NOT_VALIDATED, RECOVERY_AFTER_EXPIRED_LEASE_OR_HARD_CRASH_REQUIRES_REVIEW e os demais blockers atuais. Pinning não substitui a validação de recuperação.

## Primeira aquisição depois da liberação

Agora, o único comando de pré-condições é:

```bash
cd ~/worker-olos-export-data
SYNC_ENABLED=false npm run integration:attempts -- requirements
```

A primeira aquisição deverá usar a entrada manual existente `integration:attempts -- read`, nunca o scheduler. A forma prevista, **ainda bloqueada e não operacional nesta release**, é:

```bash
SYNC_ENABLED=false npm run integration:attempts -- read \
  --mode corporate \
  --work-dir /CAMINHO/PRIVADO/NOVO/CASO \
  --source-config /CAMINHO/PRIVADO/CONFIGURACAO-CORPORATIVA-APROVADA.json \
  --confirm EXECUTE_ATTEMPTS_READ_ONCE
```

Hoje `--mode corporate` é recusado pelo gate antes de ler arquivos, acessar Docker ou conectar. Não preparar esse arquivo usando o schema de fixture: o carregamento corporativo da CLI e o vínculo ao arquivo de aprovação do pin ainda precisam ser emitidos na liberação específica. Portanto **não existe um comando corporativo executável nesta release**; não basta substituir endereços ou definir SYNC_ENABLED=true.

Após resolver os itens acima: registrar aprovação operacional vinculada ao caso/configuração/pin/contrato, reservar claim durável e uma única tentativa, validar o destino autorizado, iniciar sem scheduler, selar o lote completo e conferir recibos/checkpoint/entrega. Erro/timeout/resultado incerto implica parada e preservação do caso; Log/ReRead exigem autorização e revisão próprias. Só considerar recorrência após comprovar aquisição e recuperação controladas.

## Arquivos e validação

Criados:

- src/olos/tds/attempts-access.ts
- src/integration/attempts/corporate-recovery.ts
- test/attempts-corporate-pinning.test.ts
- este documento

Modificados:

- src/security/olos-certificate-pin.ts: propósito distinto de aprovação.
- src/diagnostics/pinned-message-io.ts: evento secure após validação.
- src/diagnostics/tds-tls.ts: diagnóstico recusa aprovação de aquisição.
- src/olos/tds/connection.ts e source.ts: instalação por instância e guardas.
- src/large-batch/coordinator.ts: recusa antes de efeitos locais.
- src/integration/attempts/recovery.ts: revisão comum, escopos separados e checagem da identidade da faixa.

Typecheck e build aprovados. **72 testes direcionados aprovados em cinco arquivos**, incluindo sete testes novos. A primeira execução encontrou três erros na construção sintética de assinaturas dos testes; corrigido o helper para usar exatamente os parâmetros oficiais. Nenhuma proteção foi removida para obter aprovação.

```bash
npm run typecheck
npm run build
npm test -- test/attempts-corporate-pinning.test.ts \
  test/olos-certificate-pin.test.ts test/tls-diagnostic.test.ts \
  test/tds-parser.test.ts test/large-batch-wire.test.ts
```

Testes: certificados sintéticos sem SAN, hook real da Connection até antes do LOGIN7, certificado substituído recusado, zero bytes de aplicação, capacidade clonada/configuração alterada recusadas, zero connect corporativo, coordenador bloqueado antes de preflight/run/spool, revisão de faixa ambígua/identidade errada recusada, plano corporativo não executável e auditoria sem sucesso presumido. Parser binário e decoder exato preservados. Não foram iniciados SQL Server ou PostgreSQL de testes nesta tarefa.

Nenhuma migration, alteração de raw.olos_calls/históricos, chamada corporativa, commit, push ou deploy.
