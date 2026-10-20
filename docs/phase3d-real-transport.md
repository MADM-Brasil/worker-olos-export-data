# Fase 3D — transporte binário SQL Server/TDS

Data: 16/10/2026. Escopo: aquisição fiel de origem e alimentação do spool da Fase 3C. Sem projeção ou integração com destinos de negócio.

## 1. Estado e alcance da evidência

Foi executada integração efetiva com **SQL Server 2022 Developer, versão 16.0.4305.5**, em container exclusivo e descartável, com registros gerados para teste. O transporte recebeu os valores de uma conexão TLS/TDS real; não foi substituído por mocks nesses testes. Os cinco nomes/assinaturas de procedure foram reproduzidos **somente na fixture** com corpos SQL sintéticos. Não são implementações da Olos e não comprovam sua semântica de consumo.

A compatibilidade demonstrada limita-se a esse servidor, Tedious 18.6.2, TDS 7.4, tipos e cenários exercitados. **Não há certificação da instalação Olos, de seu TLS legado, de seus layouts atuais ou do controle incremental compartilhado.** O consumo corporativo continua bloqueado.

Foram lidos os relatórios [3A](phase3a-olos-extraction.md), [3B](phase3b-olos-contracts.md), [3C](phase3c-large-batch-ingestion.md), contratos e adaptadores anteriores. Os gates de `BlockedMssqlTransport`, `OlosProcedureClient`, boot/configuração e scheduler não foram removidos. `SYNC_ENABLED=false` continua obrigatório; true continua rejeitado.

## 2. Estratégia técnica e limitação das APIs públicas

As APIs públicas de [node-mssql](https://tediousjs.github.io/node-mssql/) e [Tedious Request](https://tediousjs.github.io/tedious/api-request.html) oferecem streaming e pause/resume. Os eventos row, porém, chegam **depois** de `readValue`: o parser padrão já transformou DATETIME em Date e DECIMAL em number. valueHandler/conversão posterior não recupera ticks ou dígitos perdidos. BIGINT de saída no driver padrão é string, mas isso não resolve os outros tipos. Não foi encontrada uma opção pública de parser de valores binários por coluna na API instalada.

Implementada uma extensão isolada por instância, sem editar ou aplicar patch em node_modules nem alterar protótipos globais:

- `ExactTokenParser` herda o parser de tokens da versão fixada e substitui somente aquisição de ROW/NBCROW e OUTPUT textual por leitura dos corpos binários limitados.
- `ExactConnection` herda Connection e substitui o ponto **interno/privado** `createTokenStreamParser` somente para `RequestTokenHandler`. Login, autenticação, TLS, framing de pacotes/mensagens, ENVCHANGE e atenção permanecem no driver. Os demais parsers de tokens são reutilizados.
- O adaptador usa os handlers originais para eventos, erros e término do request. Não chama o parser padrão de valores para células de dataset. Não instala interceptador em outra conexão.
- O pacote `tedious` tornou-se dependência direta **18.6.2 exata**, com lockfile. Há verificação de versão antes da conexão e do parser. Atualização exige revisão dos pontos internos e repetição dos ensaios; esta extensão **não é uma API pública suportada pelo fornecedor**.

Essa escolha permite testar uma implementação funcional sem fork completo de autenticação/TLS e sem patch global. O uso de API interna permanece risco explícito: não se apresenta a extensão como mecanismo oficialmente suportado de customização do Tedious. A pesquisa e inspeção incluíram `lib/value-parser.js`, `token/stream-parser.js`, `token/row-token-parser.js`, `token/nbcrow-token-parser.js`, `token/returnvalue-token-parser.js`, `token/handler.js`, `connection.js` e os bindings de node-mssql.

## 3. Interfaces, arquivos e bloqueios

| Arquivo | Implementação |
|---|---|
| `src/olos/tds/raw-parser.ts` | Parser binário exato, validação de metadados por ordinal, ROW/NBCROW, NULL bitmap, PLP conhecido/desconhecido e limites |
| `src/olos/tds/connection.ts` | Extensão privada por instância e ponte para handlers/state machine do Tedious; erros sanitizados |
| `src/olos/tds/source.ts` | RPC tipado, streaming pull, pausa/retomada, outputs/status, cancelamento e transporte isolado |
| `src/large-batch/types.ts` | Categoria explícita `isolated-sqlserver`, distinta de dados sintéticos e do transporte corporativo bloqueado |
| `src/large-batch/coordinator.ts` | Aceitação da instância isolada registrada, mantendo a recusa de mssql e de objetos que apenas alegam ser isolados |
| `test/tds-parser.test.ts` | Frames sintéticos, fragmentação e proteções; não são certificados como conexão real |
| `test/tds-integration.test.ts` | Integração efetiva com SQL Server e carga até PostgreSQL/spool descartáveis |
| `test/support/sqlserver.ts` | Fixture Developer própria, senha efêmera, TLS com CA de teste e porta loopback efêmera |
| `test/support/tds-load.ts` | Processo de carga com heap limitado e entrada somente por stdin da fixture |
| `package.json`, `package-lock.json` | Tedious direto fixado; sem alteração dos scripts operacionais |
| `README.md`, este relatório e `phase3d-real-load-results.json` | Orientação e evidências sem credenciais ou dados pessoais |

`IsolatedTdsSource` e `ExactConnection` recusam servidor diferente de `127.0.0.1` e banco diferente de `phase3d_fixture`. Não há URL, host ou credencial de ambiente consumida por esse caminho. O produtor exige CA explícita e configura encrypt=true, trustServerCertificate=false, serverName=localhost e TLS mínimo 1.2. Nenhum parâmetro libera ExportDataV3 ou o host corporativo.

O coordenador aceita o novo transporte somente se sua instância estiver registrada internamente por esse construtor restritivo (WeakSet). Um objeto com kind=`isolated-sqlserver` por si só não libera o caminho. O kind continua identificado como conexão SQL Server efetiva; não é rotulado `synthetic`. Nenhuma parte é instanciada pelo HTTP, probes ou scheduler.

Não foram criadas migrations nem alteradas 001–005, permissões, outbox, controles transacionais ou os arquivos anteriores de testes/suporte. A aplicação de SQL de teste ocorre explicitamente apenas nas fixtures criadas pelos helpers.

## 4. Fidelidade dos valores e metadados

| Tipo | Aquisição testada | Representação entregue ao pipeline |
|---|---|---|
| INT / INTN(4) | Corpo signed 32 bits, sem number vindo do parser | Bytes; codec existente produz string decimal exata |
| BIGINT / INTN(8) | Oito bytes signed, incluindo extremos int64 | Bytes; BigInt/string no codec, nunca number |
| DECIMALN / NUMERICN | Comprimento, precisão, escala, sinal e magnitude binária | Bytes; codec exato existente. Teste efetivo DECIMAL(38,18) |
| DATETIME / DATETIMEN(8) | Dias desde 1900 e ticks de 1/300 s originais | Oito bytes; `sql-datetime/v1:<rawHex>` no envelope |
| NVARCHAR | UTF-16LE original com unidades/bytes distintos | Bytes; decoder estrito preserva Unicode ou rejeita entrada inválida |
| VARCHAR / CHAR | Bytes originais, inclusive 0x80 e espaços finais | Bytes; encoding e collation explicitamente autorizados antes da decodificação |
| DATE | Corpo de três bytes, dias desde 0001 | Bytes no transporte de investigação; não é Date de resposta |
| NULL | Prefixos próprios, PLP NULL e bitmap NBCROW | null, distinto de corpo vazio |

Não são reconstruídos valores de Date/number arredondados. Um tick DATETIME foi comprovado como ticks300=1 após aquisição de SQL Server e como token exato após persistência no PostgreSQL da carga. DATETIME não recebe fuso/offset nem arredondamento para microssegundos PostgreSQL. DECIMAL testado: `12345678901234567890.123456789012345678`. BIGINT testados: mínimo e máximo signed int64.

Metadados são validados **antes das linhas**: quantidade, ordem, nome exato, duplicatas sem distinção de case, tipo, largura, precisão/escala, flag de nullability e ausência de criptografia de coluna. Não se reutiliza NO_METADATA de um dataset anterior. Texto exige comprimento SQL declarado (`sqlLength`, em unidades da declaração), orçamento local (`maxBytes`) e bytes de collation aprovados (`collationHex`). VARCHAR aceita os encodings previstos no codec anterior; codepage incompatível é recusada. Não se infere charset pela configuração do host.

Os campos/tipos reais da instalação Olos continuam pendentes. A fixture mede seus próprios metadados/flags/collation e aprova somente seus registros gerados. Não se transforma isso em aprovação dos seis contratos documentais. Tipos não implementados neste transporte, incluindo DATETIME2/DATETIMEOFFSET, FLOAT/MONEY, XML/UDT e outros tipos não contratados, falham explicitamente. DATE de investigação não foi conectado a um novo layout de persistência operacional.

Password continua excluído pelo pipeline anterior à gravação. O transporte pode receber bytes de coluna sensível se uma procedure os enviar; isso não significa que senha seja escrita em spool, envelope ou log. A revisão de coleta mínima e possibilidade de exclusão na origem permanecem obrigatórias.

## 5. RPC, datasets, OUTPUT e término

`prepareTdsRequest` reutiliza `prepareCall` e compara a assinatura recebida com o catálogo informado por sys.parameters da Fase 3B. Não executa SQL livre, query de descoberta, EXEC concatenado, wrapper ou INSERT EXEC. O nome RPC vem da allowlist fixa dbo.

| Procedure preparada | Parâmetros principais | Evidência nesta fase |
|---|---|---|
| ReadData | TableName VARCHAR(128), outputs VARCHAR(MAX) | RPC efetivo na fixture com bytes exatos e outputs |
| ReadDataCurrentDay | TableName VARCHAR(128), outputs VARCHAR(4000) | RPC efetivo na fixture; resposta de 200.000 linhas |
| ReadDataPreviousDays | Mesma assinatura anterior | RPC efetivo com sucesso/erro/timeout/datasets extras/perda de conexão na fixture |
| LogReadData | tablename NVARCHAR(128), DateExport DATE, outputs VARCHAR(4000) | RPC efetivo, data civil e retorno DATE binário na fixture |
| ReReadData | TableName VARCHAR(128), DateExport DATE, PKIDStart/End BIGINT, outputs VARCHAR(4000) | RPC efetivo, extremos BIGINT e data conferidos pelo corpo SQL sintético |
| ResetReadData / DataAvailable | Catálogo anterior | Permanecem recusadas antes da conexão |

Parâmetros BIGINT são strings validadas convertidas para BigInt no binding Tedious; nunca para number. DATE de **entrada** usa a adaptação civil UTC já existente, com useUTC=true; isso não permite converter DATETIME de resposta em Date. Tamanhos de NVARCHAR preservam unidades UTF-16, sem confundir os bytes de sys.parameters. Outputs MAX têm orçamento de célula independente do tamanho SQL.

OUTPUTs Message/ErrorMessage são capturados com nome, tipo textual e tamanho verificados, sem persistir seus conteúdos nos controles ou logs. O parser limita PLP de outputs antes de materializar a string. Mensagens INFO têm contador; seu conteúdo não é registrado. ERROR, flags SQL/serverError/attention e falhas de execução impedem sucesso. Nenhum returnValue é interpretado como sucesso sem a allowlist explícita do plano.

Uma resposta só produz `complete` depois de o callback nativo de conclusão ser chamado, o iterador de tokens chegar ao fim e existirem dataset único, outputs exigidos, RETURNSTATUS e DONEPROC final sem MORE/erro. Dados ou tokens posteriores a esse terminal invalidam a execução. Dataset extra/inesperado não é descartado. O endpoint de investigação `call` entrega erro OUTPUT/status para avaliação do chamador; o coordenador de ingestão rejeita ErrorMessage não vazio ou status fora do plano.

Não existe paginação, cursor remoto, parâmetro de page size ou retry automático. O limite local de 200.000 não muda o comportamento da procedure. Uma resposta maior, estática ou incremental, falha conservadoramente e exige recuperação, sem confirmar prefixos.

## 6. Streaming, backpressure e cancelamento

O parser usa Readable.from com highWaterMark=1 e o request usa pause/resume nativos. Ao receber uma linha, pausar antes de disponibilizá-la ao consumidor. Há um único slot de entrega; recebimento inesperado de outra linha falha explicitamente. O consumidor solicita o próximo evento somente depois de o `append` da Fase 3C terminar. A ponte pode ter prefetch limitado de token/chunk, além dos buffers de socket/TLS/pacote; não se afirma memória de apenas uma linha no processo inteiro.

PLP conhecido/desconhecido valida comprimento total/chunks/terminador, NULL e limites antes de alocar conteúdo desproporcional. O acumulador cresce com teto, sem guardar uma lista ilimitada de chunks de um byte. Defaults: célula 1 MiB, linha binária 1 MiB e buffer de parser 2 MiB. Esses limites se somam aos limites JSON/disco/parcelas SQL da Fase 3C. Células maiores são recusadas; não há streaming de uma única célula arbitrariamente grande até o PostgreSQL.

Timeout cobre a execução inteira, mesmo depois do primeiro pacote, e não apenas o requestTimeout nativo que o driver encerra ao receber dados. Abort/erro no consumidor solicita atenção/cancelamento; a execução não é liberada por Promise.race. O bloco finally fecha a conexão dedicada e aguarda seu evento end antes de liberar a exclusão local. Cada chamada explícita usa conexão nova, sem pool ou replay oculto.

O fechamento local/ACK de atenção não prova rollback de efeitos já ocorridos no servidor. Falha após dispatch continua `unknown`; não é convertida para `not_sent`. A falha de TLS/conexão anterior ao envio permanece `not_sent`. Os controles duráveis e o spool anterior impedem outra leitura após ingestão interrompida; eles não fornecem fencing dentro da Olos.

## 7. Spool, PostgreSQL e recuperação

O novo produtor implementa a porta de bytes da Fase 3C. Intent PostgreSQL e manifesto de recepção precedem a chamada. Linhas vão para o arquivo parcial com backpressure. O coordenador exige `complete`, fim efetivo do produtor e selo fsync/hash antes de chamar persistência. As parcelas SQL, recibo e checkpoint continuam em uma transação da resposta inteira.

- Rede/TLS interrompida, erro após linhas, timeout, metadados incompatíveis ou dataset extra: não há terminal válido, selo nem checkpoint; preservar intent/claim e conciliar.
- Falha de disco: a exceção do sink encerra o iterador de origem, cancela/fecha a conexão e mantém o protocolo conservador de recuperação da Fase 3C.
- Spool selado antes do PostgreSQL: retomar somente entrega local da mesma identidade, com lease/recibo válidos. Não executar outra procedure.
- Commit sem ACK: recibo idempotente da Fase 3C, mesmo UUID/digest, sem novo consumo remoto. Ausência de recibo não prova que a Olos deixou de avançar.
- Lease vencida sem recibo, lote parcial, volume perdido ou claim abandonado: conciliação e intervenção previstas na Fase 3C; sem adoção/reset automático.

LogReadData/ReReadData estão preparados no transporte de ensaio; o planejador anterior não passou a executar recuperação corporativa. Permanecem pendentes autoria, retenção, escopo de intervalos/exportDate e compartilhamento do controle incremental. Não há transação distribuída nem garantia demonstrada de ausência de perda na janela consumo remoto → spool durável.

## 8. Ambiente e testes

A fixture foi criada a partir da imagem Microsoft SQL Server Developer 2022 fixada pelo digest `sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718`. Requer Docker x86_64, imagem disponível, OpenSSL e recursos para a instância de teste. O helper não aceita conexão externa, URL de ambiente ou fallback. Falha de disponibilidade é falha explícita, nunca substituição por simulação rotulada como certificação real.

O container tem label exclusivo `worker-olos-export-data.test=phase3d`, porta efêmera publicada somente em 127.0.0.1, limite de 3 GiB/2 CPUs e senha efêmera. O processo SQL Server roda como usuário mssql; o estágio inicial prepara somente permissões dos arquivos da CA/certificado da fixture. TLS 1.2 e forceencryption são habilitados na instância de teste; CA própria e certificado localhost são validados pelos clientes, sem trustServerCertificate=true ou redução da política TLS. [Referência Microsoft sobre containers SQL Server](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker).

Os testes de persistência usam exclusivamente o PostgreSQL descartável anterior, em loopback/porta efêmera, sem configuração corporativa. O produtor da carga lê endpoint, plano e conexão apenas do stdin da fixture pai; valida os nomes de banco/host e não imprime segredos.

Para reproduzir em ambiente de desenvolvimento isolado (a imagem deve estar disponível antes da suíte):

```bash
docker pull mcr.microsoft.com/mssql/server@sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718
npx vitest run test/tds-parser.test.ts test/tds-integration.test.ts
npm run typecheck
npm run build
npm test
docker build -t worker-olos-export-data:phase3d-local .
```

Nenhum desses comandos aponta para banco corporativo. As permissões de administrador da fixture servem apenas para criá-la; autenticação/permissões EXECUTE do OEC e a cadeia TLS da instalação corporativa precisam de validação própria antes da ativação.

Cobertura separada:

- **Frames sintéticos:** fragmentação byte a byte, ROW/NBCROW, null bitmap, PLP conhecido/desconhecido/vazio/null, comprimento total incorreto, truncamento, limite de célula/linha, falha de stream, metadata drift, collation/charset, assinatura, BIGINT/DATE de entrada e gates.
- **SQL Server efetivo isolado:** fidelidade dos tipos, cinco RPCs, outputs/status, resposta vazia, datas e extremos numéricos, pausa/retomada e exclusão, cancelamento, timeout, erro após linhas, dataset extra, metadata drift, TLS sem CA válida, nova conexão explícita e perda efetiva de conexão ao parar somente a fixture.
- **Ponta a ponta de carga:** 200.000 registros gerados com os 18 campos documentados de AttemptsRawData; uma chamada SQL Server, TLS/TDS/binário → spool → PostgreSQL; token DATETIME exato no envelope e checkpoint único. Contrato/nulabilidade/collation aprovados somente na fixture.

O teste de rede inicialmente não recebeu uma linha antes do WAITFOR porque o servidor não havia enviado um pacote completo; a fixture foi corrigida para produzir linhas suficientes e flush explícito antes da interrupção. Isso não mudou o transporte nem relaxou sua confirmação. A preparação inicial de permissões/ambiente da fixture também foi ajustada para executar como mssql, mantendo TLS verificado.

Resultados finais e métricas da carga são registrados abaixo e no artefato [phase3d-real-load-results.json](phase3d-real-load-results.json). Memória é pico amostrado; o limite V8 não limita todo RSS/buffers nativos. Tempo e capacidade não são benchmark da Olos/VPN nem garantia para todos os payloads.

### Resultados finais — código após a revisão de limites e OUTPUTs

| Comando/verificação | Resultado |
|---|---|
| `npm install --save-exact tedious@18.6.2` | Exit 0; dependência direta fixada e lockfile atualizado; sem edição manual de pacotes |
| `npm run typecheck` | Exit 0 |
| `npm run build` | Exit 0; import do transporte compilado em Node também validado sem conexão |
| `npm test` | **351 testes / 25 arquivos aprovados**, exit 0; 333 anteriores + 18 novos (9 frames/segurança e 9 integrações efetivas) |
| Duração da suíte final | 114,35 s |
| `docker build -t worker-olos-export-data:phase3d-local .` | Exit 0; build local, sem publicação/implantação |
| Integridade SHA-256 contra baseline da tarefa | Testes e suportes anteriores, contratos/adaptadores Olos anteriores, cinco migrations, permissões, persistência, projeção, boot e configuração preservados |

| Carga SQL Server efetiva até PostgreSQL | Resultado |
|---|---:|
| Versão SQL Server Developer | 16.0.4305.5 |
| Registros recebidos / persistidos | 200.000 / 200.000 |
| Duplicados / rejeitados | 0 / 0 |
| Chamadas SQL Server / checkpoint final | 1 / 1 |
| Tempo do pipeline da carga | 78.935 s |
| Heap inicial / pico amostrado | 23.11 / 38.67 MiB |
| Limite total V8 (`old-space=128`, `semi-space=4`) | 140 MiB |
| Pico RSS amostrado | 125.83 MiB |
| Bytes do spool selado | 86,088,895 (82.10 MiB) |
| Buffer máximo de escrita / teto configurado | 65,512 / 65.536 bytes |
| Linhas no slot de entrega, máximo | 1 |
| Pausas / retomadas | 200.000 / 200.000 |

A contagem final foi consultada no PostgreSQL da fixture. Não houve avanço por parcela. O artefato contém somente versão, métricas e contagens, sem endpoint, credenciais, payloads ou mensagens do servidor. Após a suíte final, os filtros Docker pelos labels exclusivos das fixtures SQL Server e PostgreSQL não listaram containers remanescentes.

## 9. Limitações e primeira extração corporativa controlada

1. A extensão depende de API privada fixada. Antes de alterar Tedious/Node ou adotar uma instalação distinta, repetir framing, tipos, TLS, outputs, cancelamento, falhas e carga. Uma API pública futura com aquisição anterior a readValue seria preferível após validação.
2. Não foi acessado ExportDataV3. Confirmar versão/TDS/TLS/certificado e metadados atuais mediante diagnóstico de catálogo separadamente autorizado, sem consumir procedimentos para descobrir layout.
3. Validar e aprovar contratos de colunas/nulabilidade/collation e escopo de PK entre dias/instâncias; não reutilizar a aprovação sintética. CHAR/VARCHAR com outras codepages precisam codec/evidência próprios. Datasets múltiplos/novos tipos precisam contrato explícito.
4. Comprovar semântica/retorno/status de sucesso, término de lote, partição PreviousDays, retenção, interação com outros consumidores e recuperação após desconexão. Cinco assinaturas reproduzidas não comprovam os corpos da Olos. DataAvailable/Reset continuam proibidas.
5. Revisar volume POSIX persistente privado, fsync, proteção dos dados pessoais, monitoramento/retenção, quotas, lease para a latência real, WAL e transação PostgreSQL única. Spool local não resolve atomicidade entre bancos nem perda do volume.
6. Preparar autorização operacional específica, escopo de uma chamada e plano ensaiado de recuperação/conciliação. Considerar primeiro uma referência pequena como Disposition/ReadData após validar seu layout/tamanho integral; não inventar TOP/page size que a assinatura não oferece.
7. Remover o gate de endpoint corporativo e ligar composição operacional somente em entrega revisada posterior. Isso inclui segurança TLS existente, credenciais protegidas, readiness, shutdown, estado incerto, reconexão e exclusão global. Nenhuma variável desta entrega habilita essa transição.

Nenhuma procedure real da Olos, consulta de dados pessoais reais, migration/INSERT em banco corporativo, alteração de VPN/WireGuard/discadora/serviços existentes, commit, push ou deploy foi executado. Somente servidores descartáveis de teste receberam SQL/fixtures e somente uma imagem local da aplicação foi construída. SYNC_ENABLED continua false.
