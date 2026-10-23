# Auditoria do diagnóstico TLS/TDS corporativo

Escopo: transporte de `ReadDataCurrentDay` e `ReadDataPreviousDays`, com
`tedious@18.6.2`. Auditoria local sobre `87bd7c6`, sem conexão à Olos, execução
remota, credenciais reais, alteração de evidências operacionais ou publicação.

## Conclusão e limite da evidência

Foi reproduzida uma incompatibilidade no `PinnedMessageIO`: o handshake original
aceitava somente mensagens TDS de tipo `0x04`. Um peer sintético com envelope
`0x12` concluiu TLS verificado no `MessageIO` original do Tedious 18.6.2 e falhou
com `INVALID_TDS` no transporte com pin, antes da correção. O teste inicial teve
5 aprovações e essa falha; não enviou LOGIN7, SQL ou RPC.

O transporte agora aceita `0x04` e `0x12` **durante a negociação TLS**. Outros
tipos continuam rejeitados. A biblioteca TLS ainda precisa autenticar o
certificado aprovado, conferir o SHA-256 exato e negociar TLS 1.2. Nenhum tipo de
pacote, isoladamente, autoriza LOGIN7 ou RPC.

No caminho corporativo auditado, a restrição de tipo era a única criação
explícita de `INVALID_TDS`. O contexto informado de produção é compatível com
essa restrição. Entretanto, o diagnóstico antigo não preservou o tipo recebido:
**não está comprovado que a Olos respondeu `0x12`**. A reprodução confirma o
defeito local, não a causa remota definitiva. TCP conectado não comprova TLS,
login ou execução de procedure.

## Inventário das origens e classificações

| Camada | Condição auditada | Resultado e distinção |
| --- | --- | --- |
| `PinnedMessageIO.startTls` | Tipo da mensagem de handshake diferente de `0x04` no código anterior | Produzia `INVALID_TDS`, inclusive para `0x12`. Agora somente tipos fora de `0x04`/`0x12` geram esse código, com `TLS_RESPONSE_TYPE_UNEXPECTED` e tipo numérico recebido. |
| `IncomingMessageStream` herdado | Comprimento do pacote menor que o cabeçalho de 8 bytes | Tedious emite `ConnectionError` sem código próprio. O novo motivo é `TDS_PACKET_STREAM_ERROR`; não se inventa um código `INVALID_TDS` para um erro sem esse código. |
| Leitura de mensagem | Iterator encerra antes de entregar mensagem, ou rejeita | `TDS_MESSAGE_READ_FAILED`; preserva código reconhecido quando existente. EOF sem código usa a classificação operacional segura. |
| Corpo da mensagem | Erro propagado pelo stream durante PRELOGIN ou handshake | `TDS_MESSAGE_STREAM_ERROR`, com tipo conhecido e código permitido. Fragmentação TCP e vários pacotes TDS, por si só, não são erros. |
| Decoder PRELOGIN do Tedious | Offsets ou comprimentos causam erro de leitura de Buffer | `PRELOGIN_DECODE_FAILED`, com classe/código sanitizados. O decoder original era chamado fora do `try/catch` de leitura em `SENT_PRELOGIN`. A primeira resposta agora é drenada e decodificada antes de retornar por `readMessage`, cujo erro o driver trata. O conteúdo retornado ao driver é o mesmo. |
| TLS/OpenSSL | Registro inválido, incompatibilidade de versão, certificado rejeitado ou pin diferente | `TLS_HANDSHAKE_FAILED` e código técnico reconhecido, sem mensagem TLS. Configuração/pin inválidos antes da negociação usam `TLS_CONFIGURATION_REJECTED`. |
| Socket durante TLS | `error`, `end` ou `close` enquanto a negociação está pendente | Motivos separados `TLS_SOCKET_ERROR`, `TLS_SOCKET_ENDED`, `TLS_SOCKET_CLOSED`; término sem código é representado por `ECONNRESET`. A promessa termina uma vez. |
| TLS estabelecido | Erro posterior no stream TLS | `TLS_STREAM_FAILED`; continua encerrando o socket. |
| Parser da resposta RPC | Erro do parser exato, stream, sink ou handler de tokens | `TOKEN_PARSER_FAILED`; preserva a falha antes de converter a operação para `INVALID_RESPONSE`. O parser exato usa `INVALID_RESPONSE`, não cria `INVALID_TDS`. |

Também foram auditadas as ocorrências do diagnóstico independente
`src/diagnostics/tds-tls.ts`, que **não é a chamada corporativa consumptiva**:

- `encryptionFromPrelogin`: tabela truncada ou com mais de 32 opções; terminador
  ausente; tokens duplicados; offsets sobre o cabeçalho ou fora do corpo; opção
  ENCRYPTION ausente, tamanho diferente de 1 ou valor maior que 3.
- Erro de `incomingMessageStream` e resposta inicial com tipo diferente de `0x04`.
- `classifyTlsError`: fallback de erros desconhecidos para `INVALID_TDS`.
- Conversão de `TLS_INCOMPATIBLE` em `INVALID_TDS` quando a fase ainda é PRELOGIN.

Essas classificações do diagnóstico independente não foram alteradas nem usadas
para deduzir o que aconteceu no transporte corporativo. No Tedious instalado,
`MessageIO`, `IncomingMessageStream` e o decoder PRELOGIN não criam diretamente
um erro com código `INVALID_TDS`.

## Comparação com o MessageIO original

A comparação usou o código instalado de `node_modules/tedious/lib/message-io.js`,
`incoming-message-stream.js`, `prelogin-payload.js` e `connection.js`.

Ambos enviam dados do handshake dentro de PRELOGIN (`0x12`), leem a resposta pelo
iterator de mensagens do Tedious e trocam os pipes para TLS após a negociação.
O original não restringe o tipo do envelope da resposta de handshake. O código
com pin mantém uma lista explícita de dois tipos, além das suas proteções:
certificado folha autoassinado previamente aprovado, pin SHA-256, TLS 1.2,
`rejectUnauthorized: true`, uso único e renegociação desabilitada. Nenhuma dessas
proteções foi removida.

O original passa `servername` e não encaminha um `checkServerIdentity` customizado
para `tls.connect`. A implementação com pin mantém a identidade vinculada à
aprovação e seu verificador específico. A mudança não introduz alteração global
em protótipos do driver.

## Diagnóstico e preservação do erro

`InspectionDiagnostics` conserva o primeiro erro e acrescenta `transport`, com
até 32 entradas. O primeiro erro e seu motivo continuam preservados mesmo se o
orçamento de entradas tiver sido preenchido por eventos anteriores. Eventos de
limpeza não substituem o erro original nem avançam as etapas após a falha.

Cada entrada contém somente motivo fixo, etapa/fase, código permitido, tipo de
pacote inteiro entre 0 e 255 quando conhecido e classe de erro permitida. Classes
e códigos desconhecidos recebem classificação segura. Não são serializados
mensagens, stack, causas, certificados, registros TLS, payloads ou parâmetros.
Nenhum debug de bytes ou tokens foi habilitado.

`OlosError.code` permanece operacional, por exemplo `CONNECTION_FAILED`.
`OlosError.technicalCode` conserva o código técnico sanitizado. A extração técnica
percorre uma cadeia limitada de causas, preferindo o erro original aos wrappers
`ESOCKET`, `CONNECTION_FAILED` e `EXECUTION_FAILED`. O snapshot conserva seu
`errorCode` técnico; a estrutura anterior permanece com campos adicionais.

O teste das duas procedures força `ESOCKET` com causa `INVALID_TDS`, confirma a
saída operacional `CONNECTION_FAILED` e técnica `INVALID_TDS`, ausência de RPC,
`not-dispatched.json` com digest válido e bloqueio de uma segunda captura. Nenhuma
evidência anterior é preenchida retrospectivamente ou modificada.

## Arquivos alterados

| Arquivo | Alteração |
| --- | --- |
| `src/diagnostics/pinned-message-io.ts` | Diagnóstico de leitura/PRELOGIN/TLS, envelopes de handshake e término concorrente. |
| `src/olos/tds/inspection-diagnostics.ts` | Vocabulário seguro, causas limitadas, detalhes estruturados e orçamento de eventos. |
| `src/olos/tds/connection.ts` | Etapa PRELOGIN e preservação das falhas do parser/handler. |
| `src/olos/tds/source.ts`, `src/olos/types.ts` | Código técnico sanitizado junto do erro operacional, sem causa ou mensagem original. |
| `test/pinned-message-io.test.ts` | Nova bateria isolada de protocolo, TLS, comparação com upstream e concorrência. |
| `test/support/tls-diagnostic-fixture.ts` | Peer sintético configurável para tipo, fragmentação, vários pacotes e falhas controladas. |
| `test/attempts-corporate-pinning.test.ts` | Callback real de conexão em PRELOGIN inválido. |
| `test/attempts-activation.test.ts` | Erro técnico, ausência de RPC, digest e pendência nas duas procedures. |
| `test/tds-response-observation.test.ts` | Falhas do stream e do handler do parser, com sanitização. |
| `docs/corporate-tls-tds-diagnostics-audit.md` | Este relatório. |

## Testes e validação

A bateria direcionada passou com 113 testes. Ela cobre:

- Comparação com o driver original para envelopes `0x04` e `0x12`, com TLS
  verificado e zero bytes de aplicação.
- Tipos inesperados, cabeçalho TDS inválido, PRELOGIN com offset inválido e erro
  do decoder chegando ao callback real de conexão antes de TLS/login/RPC.
- Fragmentação TCP e mensagem TDS dividida em vários pacotes.
- Registro TLS inválido, incompatibilidade de versão, certificado substituído e
  desconexão durante o handshake.
- Erro TLS, término do socket e evento secure tardio concorrentes, sem falso
  sucesso e sem substituição do primeiro erro.
- Sanitização, limite do diagnóstico, falhas de stream/handler do parser,
  preservação do código original e dos controles de recuperação consumptiva.

Comandos de validação final:

```bash
npm test -- --maxWorkers=2 --testTimeout=30000 --reporter=json --outputFile=/tmp/worker-olos-tls-tds-tests.json
npm run typecheck
npm run build
git diff --check
```

Resultado final: **693 testes aprovados, zero falhas e 3 ignorados**, em 54
arquivos de teste. Os três ignorados são históricos opcionais, pois
`OLOS_HISTORICAL_AUDIT_DIR` não foi fornecido. Typecheck, build e
`git diff --check` passaram. O relatório JSON está em
`/tmp/worker-olos-tls-tds-tests.json`. Os testes de banco usam containers locais
descartáveis e credenciais sintéticas.

## Riscos residuais e limites operacionais

O tipo recebido no incidente antigo permanece desconhecido; não será inferido
nem acrescentado à evidência existente. Resposta diferente de TDS, comportamento
de intermediário, envelope inesperado ou falha TLS continuam hipóteses remotas,
sem validação corporativa nesta auditoria. Os testes não comprovam configuração,
certificado ou comportamento da Olos em produção.

A dependência de APIs internas exige manter a versão exata 18.6.2 e repetir os
testes antes de qualquer atualização do driver. A classificação não publica
códigos arbitrários: um código nativo fora da lista pode aparecer somente como
fallback seguro, acompanhado do motivo e da classe conhecidos.

A primeira resposta PRELOGIN é temporariamente copiada para permitir que falhas
do decoder cheguem ao tratamento do driver; o driver já acumulava essa resposta
antes de decodificá-la. Isso não toca no spool nem no stream de dados consumptivos.
Não foram alterados limites de lote, checkpoints, deduplicação, fencing, recibos,
autorizações, recuperação ou configuração de retries.

O `remote-pending.json` e o `not-dispatched.json` do incidente real permanecem
intactos. Esta correção não libera nem reexecuta a tentativa. Não houve chamada à
Olos, commit, push, deploy, alteração de configuração de produção ou uso de
credenciais reais. Os testes continuam sem constituir autorização operacional.
