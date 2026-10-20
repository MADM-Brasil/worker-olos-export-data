# Homologação local offline de AttemptsRawData

Evolução de agentes: consultar [catálogo global autorizado](global-agent-catalog.md). O catálogo sanitizado aceita somente AgentId/UserName; campos extras, inclusive Password/Login/Email/UserId, são rejeitados. Com a migration específica instalada, nomes são enriquecidos pelo AgentId, com revisão e atualização histórica independentes do checkpoint de ingestão. Não existe leitura automática de Users nem atualização automática dos ambientes antigos.

Implementação em 16/10/2026. Este caminho processa **arquivos históricos privados**, sem consultar SQL Server. Não libera aquisição incremental nem remove `ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED`. O boot HTTP e a integração Disposition continuam inalterados. `SYNC_ENABLED=false` é obrigatório também na CLI offline.

## Evidência disponível e limite desta entrega

O operador informou um lote real de 570 registros, com 218 CPFs, 264 e-mails e 21 TMAs provisórios, anteriormente recuperado na VPS. Esse arquivo, seus catálogos e a lista física vigente de 16 colunas **não foram fornecidos nesta sessão**. Não houve transferência, busca de arquivos privados, conexão à VPS ou conferência desses totais reais.

Os ensaios desta entrega usam 570 registros **sintéticos**, CPFs gerados por algoritmo e endereços `example.invalid`, em PostgreSQL 16 descartável. Contagens iguais às informadas foram construídas para testar o pipeline; não constituem validação do lote real. A homologação real depende da transferência manual autorizada, aprovação do perfil e execução dos comandos abaixo.

## Arquivos e arquitetura

| Arquivo | Responsabilidade |
| --- | --- |
| `adapters/offline-homologation.ts` | CLI isolada; arquivos privados; criação explícita de PostgreSQL próprio; identidade Docker; arquivo original; spool; exclusão mútua; ingestão/outbox; recuperação e ciclo local |
| `src/offline/model.ts` | Perfil versionado das 16 colunas locais; parser JSON sem arredondamento; validações; extração XML e transformação pura |
| `src/offline/store.ts` | Writer exclusivamente de homologação, UPSERT parametrizado e recibo/contador na transação de negócio; reutilização do dispatcher/outbox |
| `homologation/offline-control.sql` | Controles somente do PostgreSQL novo de homologação; não é migration corporativa |
| `test/offline-homologation.test.ts` | Precisão, XML, mapeamento, 570 sintéticos, replay, concorrência, checkpoint, falhas e agendamento |
| `package.json`, `package-lock.json` | CLI `homologation:offline`; dependências exatas `lossless-json@4.3.2` e `saxes@6.0.0` |
| `README.md`, este documento | Execução e limitações |
| `docs/offline-homologation-results.json` | Resumos agregados do ensaio real de CLI, exclusivamente sintético |

Fluxo: arquivo original → parse lossless/validação → archive SHA-256 imutável → spool existente selado e validado até EOF → `PostgresPersistence` existente → envelopes + checkpoint + snapshot/outbox no mesmo commit → dispatcher existente → `raw.olos_calls` + recibo + contador de entrega na mesma transação local.

O checkpoint usa tipo `investigation` e a data explicitamente informada como rótulo do artefato. Seu cursor é o SHA-256 do arquivo. **Não é cursor Olos nem comprovação da partição consumida.** Controles e entregas têm namespace exclusivo de homologação. Uma execução concluída não cria outro lote/checkpoint ao reencontrar o mesmo arquivo, catálogos, perfil e data.

O plano armazenado no spool é evidência offline não executável, com assinatura `offline-json/non-executable`, sem parâmetros e sem evidências de aquisição TDS. A referência histórica a ReReadData não autoriza nem invoca essa procedure. O JSON já foi serializado: não passa novamente pelo parser TDS e não recupera ticks/bytes que tenham sido perdidos na extração anterior. Transporte e parser corporativos permanecem preservados para aquisição futura; não há outro cliente SQL Server neste modo.

O destino é criado pelo próprio comando `prepare`, com banco fixo `olos_offline_homolog`, imagem `postgres:16-bookworm`, ID/label de instância e porta efêmera publicada somente em `127.0.0.1`. Cada ciclo verifica essa identidade. Não existem argumentos URL/host/credencial de destino, nem fallback para `PG_*`/`DATABASE_URL`. O runtime assume role restrita aos controles/envelopes/entrega e às colunas locais. O administrador do container é usado somente no prepare explícito.

O ambiente `/home/madm/olos-phase4-test/run-001` não é reutilizado, recriado nem alterado. Não há integração com discadora, Eaglle, Kommo, HubSpot ou banco corporativo.

## Perfil local de 16 colunas

Os documentos anteriores apresentam 41 nomes distintos e não fixam o DDL vigente de 16 colunas. Para não tratá-los como confirmação física, a CLI exige um perfil externo aprovado antes de criar seu PostgreSQL. O exemplo cria **somente um contrato local de homologação**:

| Coluna no exemplo | Origem/regra | Tipo escolhido somente no banco local |
| --- | --- | --- |
| internal_id_olos | AttemptsRawDataID exato | BIGINT, PK local |
| olos_call_id | CallId, sem equiparar à PK | TEXT |
| data_ligacao | StartDate, política temporal explícita | TIMESTAMP sem timezone |
| telefone | OriginalPhoneNumber original | TEXT |
| telefone_normalizado | Política explícita de normalização | TEXT |
| nome_agente | NULL, ou catálogo local autorizado AgentId → UserName | TEXT |
| id_campanha_olos | CampaignId original | TEXT |
| nome_campanha_olos | CampaignId → Description no catálogo local | TEXT |
| id_tabulacao_olos | DispositionId original | TEXT |
| nome_tabulacao_olos | DispositionId → Description no catálogo local | TEXT |
| cpf | Campo XML explicitamente selecionado, checksum válido | TEXT |
| email | Campo XML explicitamente selecionado, sintaxe válida | TEXT |
| lead_id_origem | Sempre NULL | TEXT |
| tma_origem | NULL por padrão; fórmula provisória opt-in | INTEGER |
| razao_desligamento | Sempre NULL | TEXT |
| data_carga | Instante UTC civil da primeira carga | TIMESTAMP sem timezone |

A allowlist admite, como alternativas locais, `cpf_normalizado`, `telefone_match_key` e `tma_segundos`. O perfil precisa selecionar exatamente 16 nomes únicos e incluir `internal_id_olos`. Não admite nomes arbitrários, `kommo_*`, vínculos HubSpot ou SQL livre. `raw_olos_call_id` não é gerado por equivalência presumida.

**Antes de rodar os dados reais, conferir essas 16 escolhas com o operador.** Se o contrato vigente diferir além dessa allowlist, parar e ajustar o contrato mediante evidência; não apontar a CLI a uma tabela preexistente da empresa.

Regras:

- O parser de [lossless-json](https://github.com/josdejong/lossless-json) conserva lexemas numéricos como strings, incluindo BIGINT/DECIMAL; rejeita chaves JSON duplicadas. Não usa `JSON.parse` nos registros. Texto projetado com NUL ou surrogate isolado e tipo inesperado em campo mapeado interrompem o lote, evitando substituição pelo encoder PostgreSQL. IDs de extração devem estar no intervalo signed BIGINT, em forma canônica; `CallId` permanece texto. Null e ausência continuam distintos no arquivo e no snapshot bruto. O envelope guarda a linha como JSON canônico textual, com lexemas numéricos em strings, e o hash do arquivo; os tipos numéricos originais do JSON permanecem no archive, os bytes UTF-8 originais são arquivados separadamente.
- Um ID repetido com payload idêntico é contado e deduplicado; dois payloads diferentes para a mesma PK no arquivo impedem a ingestão. Não se deduplica por telefone, CPF ou CallId. Não se filtra operador/campanha.
- XML é lido com [saxes](https://github.com/lddubeau/saxes), sem DTD/entidades externas, limitado a 256 KiB, profundidade 32 e 20.000 nós. Tags/atributos CPF e Email são explicitamente configurados. Valores inválidos/ambíguos ficam NULL, com contadores agregados. Nome de campo desconhecido não gera busca especulativa no XML. Checksum CPF não comprova titularidade e sintaxe de e-mail não comprova entrega.
- Por padrão, telefone normalizado remove apenas separadores, preservando zeros; não acrescenta país/DDD. `br-e164` é uma regra opt-in que requer confirmação do formato de origem. `telefone_match_key` permanece não resolvido salvo autorização explícita de equivalência com o normalizado.
- `timestampPolicy=reject-offset` é o padrão: conserva horário civil e até seis casas sem arredondar. Valor preenchido com offset, formato inválido ou fração não representável interrompe a projeção antes da ingestão. Para JSON comprovadamente em UTC/offset, `utc-civil` converte explicitamente o instante para horário UTC sem timezone, conservando a fração representável. Não declarar que isso identifica o fuso original da Olos. NULL/ausente continuam NULL. O original nunca é reconstituído a partir de Date/number previamente arredondado.
- TMA fica NULL por padrão. Somente aprovação de `EndCall-minus-StartDate-provisional` calcula duração não negativa de até 24h, truncada para segundos inteiros. Isso é duração provisória, não definição de TMA homologada. A fórmula usada anteriormente nos 21 registros reais não foi fornecida; não há promessa de reproduzir esse total. A extração dos campos temporais originais é preservada independentemente dessa opção.
- `--users`, opcional, aceita somente arquivo local sanitizado com AgentId/UserName. A cardinalidade AgentId → UserName precisa estar autorizada; conflitos interrompem. Sem correspondência no catálogo, novas chamadas ficam com nome_agente NULL; não se usa nome avulso nem UserId como fallback. Não há consulta Users nem join remoto. No catálogo persistido, mudanças exigem `agents-update` explícito; preferir o ciclo sem `--users` após sua configuração.

## Instalação e transferência manual no WSL2

Pré-condições: Node compatível com `package.json` (Node 20.19+ neste ambiente), npm, Docker local acessível e espaço livre. Os comandos não necessitam VPN e não se conectam à Olos. Não usar diretório público/montado com permissões abertas. Manter o material fora do checkout.

```bash
cd /home/madm/worker-olos-export-data
npm install
npm run typecheck
npm run build

umask 077
OFFLINE_PRIVATE="$HOME/olos-offline-homologation"
mkdir -m 700 "$OFFLINE_PRIVATE"
```

Se o lote está somente na VPS: um operador autorizado deve selecionar os caminhos reais e transferir **apenas arquivos já existentes**, com sua chave SSH e host key previamente verificada. Substituir os três caminhos de exemplo abaixo. Nenhum comando deve executar ReReadData/LogReadData nem gerar outra extração.

```bash
# Exemplos para execução MANUAL pelo operador. Não foram executados nesta entrega.
scp -p -- usuario@vps-conhecida:/caminho/privado/attempts.json "$OFFLINE_PRIVATE/attempts.json"
scp -p -- usuario@vps-conhecida:/caminho/privado/campaigns.json "$OFFLINE_PRIVATE/campaigns.json"
scp -p -- usuario@vps-conhecida:/caminho/privado/dispositions.json "$OFFLINE_PRIVATE/dispositions.json"
chmod 600 "$OFFLINE_PRIVATE/attempts.json" "$OFFLINE_PRIVATE/campaigns.json" "$OFFLINE_PRIVATE/dispositions.json"
sha256sum "$OFFLINE_PRIVATE/attempts.json" "$OFFLINE_PRIVATE/campaigns.json" "$OFFLINE_PRIVATE/dispositions.json"
```

Comparar hashes com os fornecidos pelo operador da VPS. Não desativar verificação de host SSH, não colocar senhas na linha de comando e não imprimir o conteúdo dos arquivos. Não copiar logs/dumps para Git. `scp -p` pode preservar permissões de origem; o chmod posterior é obrigatório. A ferramenta recusa arquivos com bits de acesso para grupo/outros, symlinks, caminhos dentro do checkout, arquivos de outro usuário e UTF-8 inválido.

Formatos aceitos: array de objetos, ou um objeto com `recordset`, `records` ou `data`; wrapper `recordsets` exige exatamente um dataset. Se `recordset` e `recordsets` coexistirem, devem coincidir. JSON bruto de linhas planas com strings/números lexicais/boolean/null; estrutura aninhada desconhecida é recusada, não descartada. Campaign/Disposition precisam conter seus IDs e `Description`. Catálogos podem ser vazios; faltas são contadas e não inventam descrições.

Gerar, revisar e aprovar o perfil privado:

```bash
SYNC_ENABLED=false npm run --silent homologation:offline -- profile-example > "$OFFLINE_PRIVATE/profile.json"
chmod 600 "$OFFLINE_PRIVATE/profile.json"
# Editar em editor local seguro: confirmar as 16 colunas, evidência de identidade,
# nomes/caminhos XML e política de tempo/telefone. Não anexar dados pessoais ao perfil.
# Se o XML usa <field name="CPF" value="..."/>, acrescentar:
# "xmlFieldAttributes": {"name":"name","value":"value"}
# Para <field name="Email">...</field>, usar value="$text".
# utc-civil somente se comprovada a representação temporal do JSON.
```

O literal `APPROVE_ISOLATED_OFFLINE_MAPPING_V1`, a evidência de identidade no perfil e a confirmação do prepare registram a escolha local. Não equivalem a aprovação do contrato corporativo. Não editar o perfil/state do banco já preparado: a configuração é vinculada por digest; mudanças pedem outro ambiente isolado revisado.

Criar um ambiente **novo**, independente de run-001:

```bash
SYNC_ENABLED=false npm run homologation:offline -- prepare \
  --work-dir "$OFFLINE_PRIVATE/run-001" \
  --profile "$OFFLINE_PRIVATE/profile.json" \
  --confirm CREATE_ISOLATED_OFFLINE_DATABASE

# Informar a data comprovada do ARTEFATO; não inferi-la de StartDate ou do relógio.
read -r -p 'Data documentada do lote (AAAA-MM-DD): ' OFFLINE_EXPORT_DATE
```

Prepare aplica 001–004 e as permissões existentes **somente no container que acaba de criar**; cria os dois controles offline e as 16 colunas locais do perfil. Não aplica migrations em ambientes existentes ou no runtime HTTP.

Primeira importação e replay idempotente (a mesma linha pode ser executada duas vezes):

```bash
SYNC_ENABLED=false npm run homologation:offline -- import \
  --work-dir "$OFFLINE_PRIVATE/run-001" \
  --input "$OFFLINE_PRIVATE/attempts.json" \
  --campaigns "$OFFLINE_PRIVATE/campaigns.json" \
  --dispositions "$OFFLINE_PRIVATE/dispositions.json" \
  --export-date "$OFFLINE_EXPORT_DATE" \
  --expect-count 570 --expect-cpfs 218 --expect-emails 264 \
  --confirm IMPORT_HISTORICAL_DATA
```

Os três totais são **asserções a conferir**, não valores preenchidos pelo programa. Divergência impede nova ingestão. Se tags/regras não corresponderem ao arquivo, revisar a evidência antes de aprovar outro perfil; não remover as asserções apenas para declarar êxito. Nenhum CPF/e-mail/telefone é impresso.

Esperado no resumo agregado: `inputRows=570`, `uniqueInputRows=570`, `rows="570"`, `unique_rows="570"`, `confirmed=570`, `complete=true`, `metrics.cpf=218`, `metrics.email=264`, `freshness="historical-offline"`, `sqlServerQueries=0`, `sourceObservedNow=false`. A segunda execução mantém a versão do checkpoint e não reescreve linhas idênticas nem cria novos recibos. Se houver duplicatas idênticas no arquivo, `duplicatesInFile` as explicita e o total único é conferido separadamente.

## Ciclo de 60 segundos e parada

Executar no terminal em primeiro plano, depois da conferência da importação:

```bash
SYNC_ENABLED=false npm run homologation:offline -- start \
  --work-dir "$OFFLINE_PRIVATE/run-001" \
  --input "$OFFLINE_PRIVATE/attempts.json" \
  --campaigns "$OFFLINE_PRIVATE/campaigns.json" \
  --dispositions "$OFFLINE_PRIVATE/dispositions.json" \
  --export-date "$OFFLINE_EXPORT_DATE" \
  --expect-count 570 --expect-cpfs 218 --expect-emails 264 \
  --confirm PROCESS_HISTORICAL_EVERY_60_SECONDS
```

Há uma execução imediata; depois, 60 segundos **a partir do término** da anterior, reutilizando `Scheduler`. Execução lenta não acumula intervalos. Arquivo inalterado é replay local idempotente. Novo conteúdo no mesmo caminho só é aceito após concluir o lote anterior; preferir substituição atômica e aprovação explícita do novo artefato. Isso não torna os dados atuais: todo resumo conserva a marca histórica.

Em outro terminal (redefinir OFFLINE_PRIVATE se necessário):

```bash
cd /home/madm/worker-olos-export-data
OFFLINE_PRIVATE="$HOME/olos-offline-homologation"
SYNC_ENABLED=false npm run homologation:offline -- status --work-dir "$OFFLINE_PRIVATE/run-001"
SYNC_ENABLED=false npm run homologation:offline -- stop --work-dir "$OFFLINE_PRIVATE/run-001"
```

Ctrl+C/SIGTERM também pedem parada; a tarefa em andamento é drenada. O stop escreve pedido com nonce do runner, sem matar PID reutilizado. Claim de runner exclusivo e advisory lock PostgreSQL impedem sobreposição; perda do lock aborta as próximas etapas e os controles/recibos transacionais continuam protegendo a entrega. Não remover runner/claims enquanto um processo anterior estiver ativo. Depois de crash abrupto, confirmar encerramento do processo e revisar evidência antes de remover exclusivamente o runner obsoleto; nunca apagar spool, controles ou recibos para repetir a origem.

## Falhas, recuperação e limites

- Antes do selo: conservar arquivo/arquivo original, job e spool parcial; não há checkpoint. Ausência de manifesto selado exige revisão; não há promoção automática de prefixo nem nova leitura Olos.
- Após selo/antes da ingestão: replay verifica hash e EOF e usa o mesmo run/lote/checkpoint, enquanto a lease original de cinco minutos for válida. Esse ensaio simula interrupção no limite anterior à observação, não um kill físico do WSL2. Falha auditada antes de existir batch pode deixar rejeição no run; a conclusão exige revisão, sem apagar erros ou contornar o controle anterior.
- Lease expirada, run `needs_reconciliation`, erro de checkpoint ou job sem manifesto: parada conservadora. Não há takeover automático neste caminho. O artefato privado permanece reaproveitável após procedimento de recuperação revisado.
- Depois da ingestão/antes de raw: checkpoint de arquivo pode estar confirmado, mas `confirmed<expected` e `complete=false` indicam entrega pendente. O arquivo bruto e outbox permanecem duráveis. Outra entrada/perfil/catálogo não começa enquanto houver job incompleto.
- Falha do destino: rollback de negócio/recibo/contador; import manual retorna código não zero se houver pendência. No ciclo, repetir somente a entrega local. Não se lê SQL Server. Conservar exatamente arquivos e perfil utilizados.
- COMMIT sem ACK: estado incerto, consultar recibo local antes de qualquer reenvio. Recibo confirmado reconcilia sem reescrever. Ausência comprovada permite retry do mesmo UUID/snapshot **apenas neste writer que grava recibo e negócio na mesma transação do mesmo PostgreSQL**, sob exclusão local. A regra genérica da Fase 2B e sua semântica entre bancos não mudam. Indisponibilidade da consulta de recibo conserva a incerteza.
- UPSERT por AttemptsRawDataID: NULL novo não substitui valor válido; `data_carga` preserva a primeira carga. Valor não nulo diferente pode atualizar a projeção local. Não existe precedência histórica entre arquivos arbitrários: importar revisões antigas exige decisão operacional. Não há DELETE/TRUNCATE automático.
- Limites desta CLI: até 10.000 linhas e 32 MiB por arquivo, até 4 MiB por registro de spool e 64 MiB por lote PostgreSQL; não é o caminho streaming TDS de 200.000 registros. Parsing/deduplicação mantêm o arquivo limitado em memória; não se promete RSS constante. Catálogos também têm limites. Spool usa as quotas/reserva de disco da arquitetura existente (512 MiB por diretório, reserva 64 MiB); archive de múltiplos lotes requer monitoramento/retenção manual. ENOSPC mantém pendência e não confirma batch/checkpoint parcial. Não há limpeza automática.
- Container usa armazenamento Docker persistente, sem tmpfs e sem `--rm`. Reiniciar o worker conserva o banco; perder o volume Docker/WSL2 ou o diretório privado exige recuperação a partir de backup. Proteger volumes e arquivo state (contém segredo local 0600); não publicá-los. Este modo usa PostgreSQL loopback sem TLS somente no container local identificado; não é configuração para destino remoto.

## Aquisição incremental corporativa — ainda bloqueada

`ReadDataCurrentDay` continua indisponível, com `ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED`. Não há combinação de flags offline, perfil, SYNC_ENABLED ou catálogo que altere esse gate. `SYNC_ENABLED=true` é recusado. Nenhuma chamada a ReadData, CurrentDay, PreviousDays, LogReadData, ReReadData, ResetReadData ou DataAvailable foi executada nesta entrega.

Antes da liberação:

1. Aprovar metadados físicos/contrato atual de Attempts, PK e escopo por tabela/data, sucesso/outputs, timezone, volume e retenção. Não inferir granularidade de CallId nem data de exportação a partir da data da chamada.
2. Coordenar consumidores: estabelecer quem pode avançar o controle compartilhado Olos e como atribuir intervalos consumidos a cada execução. LogReadData sozinho não comprova autoria/exclusividade.
3. Corrigir certificado TLS/CA e identidade DNS da Olos; validar o transporte TDS exato, criptografia e metadados na instalação. A exceção de certificado Disposition não é herdada por Attempts. Não reduzir TLS nem usar trust global para viabilizar a homologação.
4. Ensaiar janela leitura→spool→PostgreSQL, falha parcial, VPN, timeout, COMMIT sem ACK e reinício com lease expirada/hard crash. Preservar claim; nunca repetir incremental automaticamente diante de resultado remoto incerto.
5. Aprovar separadamente LogReadData e ReReadData, com data/faixa/atribuição/completude comprovadas, retenção disponível e entrega idempotente. ResetReadData permanece proibida. Não presumir transação distribuída ou ausência de perda.
6. Implementar capacidade corporativa revisada no mesmo transporte e realizar primeiro teste **manual, autorizado, de tentativa única** com destino isolado. Somente após evidências de recuperação deliberar scheduler incremental. O scheduler offline desta entrega não é autorização para esse consumo.

## Validação executada

| Verificação | Resultado |
| --- | --- |
| `npm install` (dependências exatas adicionadas) | Concluído |
| `npm run typecheck` | Aprovado |
| `npm run build` | Aprovado |
| `npm test -- test/offline-homologation.test.ts test/no-automatic-migrations.test.ts` | 17 aprovados: 16 novos e 1 proteção anterior; reexecutado após os ajustes finais |
| `npm test` | 458 aprovados, 31 arquivos, 157,86 s; inclui os 442 anteriores |
| CLI `prepare → import → import → start → stop` | Executada em PostgreSQL próprio com 570 sintéticos; saída 0 |
| Registros/identidades no ensaio CLI | 570 linhas, 570 PKs distintas, zero duplicatas; segunda importação mantém checkpoint `1` |
| Enriquecimento sintético | 218 CPFs, 264 e-mails, 21 durações provisórias construídas na fixture; lead/razão NULL |
| Ciclo CLI em relógio real | Duas execuções, intervalo entre resumos de 60,165 s, sem nova ingestão/duplicação; parada explícita e remoção do runner |
| Logs CLI | Apenas agregados, sem valores de CPF/e-mail/telefone das fixtures |

Os totais de negócio acima são sintéticos. A evidência agregada encontra-se em [offline-homologation-results.json](offline-homologation-results.json), com `realBatchValidated=false` e `freshness=historical-offline`. O script temporário de ensaio gerou arquivos privados fora do checkout, executou a própria CLI, conferiu resumos e removeu somente seu próprio container/volume/diretório ao terminar. Não é uma nova ferramenta de diagnóstico de origem.

A suíte completa foi executada durante a implementação; as últimas validações conservadoras de texto/identidade/tipo e tratamento de erro do pool foram verificadas novamente por typecheck, build e suíte focada. Os testes anteriores não foram editados. Testes de falha usam exclusivamente pools, triggers e containers criados pelo próprio ensaio; nenhum SQL de teste é aplicado em banco externo. As fixtures SQL Server Developer dos testes anteriores continuam isoladas.

Hashes dos arquivos anteriores conferidos ao final: mudanças limitadas a README, package.json e package-lock.json, além dos novos arquivos desta entrega. Migrations 001–006, transporte/parser TDS, gates, integrações Disposition/Attempts, configuração geral e testes anteriores preservados. Nenhum procedimento corporativo, migration corporativa, alteração de WireGuard/discadora, commit, push ou deploy foi realizado. O lote privado real e run-001 anterior não foram acessados.
