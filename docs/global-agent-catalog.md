# Catálogo global autorizado de agentes Olos

Implementação local em 16/10/2026. Escopo: `Users.AgentId → Users.UserName → AttemptsRawData.AgentId → raw.olos_calls.nome_agente`. Sem dependência de discadora, SDK de agente, monitor, campanhas, organizações, Eaglle, HubSpot ou Kommo. Não há lista fixa de agentes nem filtro de ativo/supervisor/campanha. Global significa todos os agentes válidos do snapshot autorizado da mesma instalação ExportDataV3, não mistura entre instalações.

O operador informou a homologação real de 570 chamadas, 218 CPFs, 264 e-mails, checkpoint 1 e replay/ciclos aprovados. **O ambiente `/home/madm/olos-offline-homologation/run-001` e seus arquivos reais não foram abertos, migrados, reinicializados ou modificados nesta tarefa.** As evidências desta implementação são sintéticas em outros PostgreSQL criados e removidos pelos testes. Não se afirma que os 570 registros reais já receberam nomes.

## Evidência documental e bloqueios da origem

Referência local: [DBA-MN002 — ExportData V3.0 Cliente, revisão 03/06/2020](reference/olos-exportdata-v3.pdf), páginas revisadas 2, 3, 4, 16 e 30.

| Informação | Evidência | Confirmação atual |
| --- | --- | --- |
| Users é estática e pode mudar por merge | p.2 e p.30 | Documentado, sem consulta atual ao servidor |
| ReadData retorna todos os registros e colunas da tabela estática | p.4 | Sem parâmetro de projeção de colunas, operador ou paginação |
| AttemptsRawData.AgentId correlaciona Users.AgentId | p.16 | Relação documentada; validação dos valores ocorre no catálogo |
| Users.UserId é a PK distinta | p.30 | Nunca usado como substituto de AgentId |
| Users.AgentId INT; UserName NVARCHAR(510) | p.30 | Tipos documentados; metadados físicos atuais não consultados |
| Users.Password NVARCHAR(100), Email e Login no retorno | p.30 | Proíbe armazenar o retorno bruto; catálogo admite só duas colunas |
| Acesso por EXECUTE em procedures | p.3; permissões OEC informadas pelo operador | Sem SELECT/VIEW DEFINITION; nenhuma tentativa de contornar |

`ReadData('Users')` **não foi habilitada nem executada**. Sua assinatura não permite solicitar duas colunas desde a origem. O parser TDS precisa receber e enquadrar a resposta completa na rede/memória; filtrar no cliente não evita o recebimento de Password. O cliente Olos genérico remove Password, mas isso não é sanitização suficiente: ainda mantém Email/Login. `decodeWireRow` também não constitui sozinho uma allowlist de duas colunas. O transporte corporativo disponível continua restrito à capacidade Disposition; não foi acrescentada capacidade Users.

`sanitizeUsersInMemory` é apenas uma fronteira pura preparada para aquisição futura: lê AgentId/UserName, descarta os demais atributos sem copiá-los e retorna somente entradas validadas. Não executa SQL, não autentica, não autoriza a leitura e não escreve arquivo/spool/log. Um futuro adaptador deverá aplicá-la **antes** de qualquer persistência, observabilidade ou spool de Users, depois de validar completude, sucesso/outputs e metadados. Não pode gravar primeiro a resposta completa e sanitizar depois. Strings JavaScript imutáveis não permitem prometer apagamento seguro da memória; controlar acesso ao processo e dumps também será necessário.

TLS corporativo continua pendente de confiança e identidade. Não há relaxamento de TLS, uso de trustServerCertificate, mudança nas permissões OEC, habilitação de SYNC_ENABLED ou alteração de `ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED`.

## Arquivos

| Arquivo | Alteração |
| --- | --- |
| `src/offline/agent-identity.ts` | Validação conservadora da identidade AgentId |
| `src/offline/agents.ts` | Snapshot sanitizado, conflitos, digest canônico e sanitizador em memória |
| `src/offline/agent-store.ts` | Catálogo persistido, atualização transacional, histórico e cercas de identidade/posse do nome |
| `src/offline/model.ts` | Reutiliza projeção existente; nome exclusivamente pelo AgentId, sem fallback UserId/nome avulso |
| `src/offline/store.ts` | Vínculo técnico da chamada ao AgentId na mesma transação de negócio/recibo |
| `adapters/offline-homologation.ts` | `agents-check`, `agents-update`, uso do catálogo persistido e proteção contra snapshot antigo |
| `homologation/001_global_agents.sql` | Migration aditiva versionada, exclusivamente de homologação isolada |
| `test/global-agents.test.ts` | Oito testes objetivos com respostas/arquivos sintéticos e PostgreSQL próprio |
| `README.md`, `docs/offline-homologation.md`, este documento | Orientação e compatibilidade |

Parser XML de fragmentos com múltiplas raízes, transporte/parser TDS, catálogos Campaign/Disposition, scheduler, migrations anteriores e testes anteriores preservados. Não há mudança nas 16 colunas do destino nem nos perfis/arquivos históricos existentes. `agentNameField` permanece aceito para não invalidar perfis antigos, mas deixa de autorizar associação por nome solto: nesta projeção o relacionamento exige AgentId válido e correspondência no catálogo.

## Contrato e persistência mínima

Arquivo autorizado: array com **somente** `AgentId` e `UserName`. Wrappers de dados já suportados podem conter apenas suas chaves de dataset, sem outputs/metadata adicionais. Não fornecer JSON bruto de Users. Qualquer campo adicional, inclusive UserId, Password, Email ou Login, é rejeitado no catálogo sanitizado, mesmo nulo. O helper futuro em memória, diferentemente da entrada sanitizada, aceita a resposta completa apenas para descartar os campos extras antes de continuar.

Regras:

- AgentId é string decimal canônica ou INT seguro recebido em memória, dentro do INT documentado. Zero, negativos, espaços, zeros à esquerda, frações, overflow e ausência/null ficam sem associação. A restrição a positivos é uma **política local conservadora**: semântica de sentinelas/IDs não positivos não foi comprovada; não se declara que todo INT não positivo seja inválido na plataforma. Esses casos precisam de revisão antes de ampliar o relacionamento.
- UserName deve ser string não vazia, até 510 unidades UTF-16, sem controles/NUL/surrogates isolados. Conteúdo válido é preservado, sem inventar nomes, normalizar grafia ou usar Login/Email.
- Mesmo AgentId com dois nomes diferentes rejeita o snapshot inteiro antes de gravar. Duplicatas idênticas são consolidadas. Nome ausente/inválido bloqueia o AgentId desse snapshot, inclusive quando outra linha contém um nome válido para ele. IDs inválidos são ignorados e contados, sem fallback UserId.
- SHA-256 do conjunto ordenado das duas colunas identifica a versão lógica. Ordem e duplicatas idênticas não geram nova versão. Catálogo atualizado é uma substituição do snapshot de referência, não uma lista fixa e não uma remoção de chamadas.
- Limite local: 10.000 linhas por snapshot, além do limite de arquivo privado da CLI. Excesso é recusado, nunca truncado/paginado por suposição. A futura aquisição precisa validar a volumetria completa da instalação.

Migration `homologation/001_global_agents.sql` cria:

1. `olos_export.offline_agent_state`: versão do controle, revisão, digest, contagens e instante técnico da última mudança lógica.
2. `olos_export.offline_agents`: somente AgentId/UserName do snapshot atual.
3. `olos_export.offline_agent_links`: ID de extração da chamada, AgentId e hash do último nome aplicado, para impedir troca silenciosa de associação e sobrescrita de nome não pertencente ao catálogo.

Não utiliza o envelope genérico `olos_export.users`, não armazena o dataset bruto, UserId, Password, Login, Email, flags ou outros atributos. DELETE existe somente para entradas de referência ausentes no novo snapshot, nunca para chamadas, envelopes, lotes, recibos ou checkpoints. Os ledgers anteriores não mudam.

A migration é aplicada por `prepare` apenas no PostgreSQL novo que esse comando acaba de criar. **Não existe atualização automática de bancos já preparados.** Na instância existente, ausência da migration mantém o fluxo offline anterior, com sanitização estrita de `--users`; atualização histórica durável por `agents-update` exige instalação manual revisada. Não aplicar o SQL diretamente no run-001 durante esta entrega. Uma adoção futura nesse ambiente exige autorização separada, parada/drenagem e backup, além da verificação da identidade Docker isolada; nunca executar em PostgreSQL corporativo. A migration não deve ser reaplicada se já instalada.

## Atualização de nomes e enriquecimento histórico

`agents-update` exige caminho privado e `APPLY_SANITIZED_GLOBAL_AGENTS`. O pool é o mesmo identificado por container/label/banco/porta loopback da homologação, sem URL externa ou fallback. A transação obtém o mesmo advisory lock da ingestão `(740210,1)`. Concorrência retorna BUSY; não abre outro canal SQL Server.

Uma mudança de catálogo incrementa sua revisão e reconcilia as chamadas, em páginas **locais PostgreSQL** de 500, dentro da mesma transação. A consulta extrai do envelope já persistido somente a identidade AttemptsRawDataID e AgentId; não traz telefone/XML/CPF/e-mail. O ID original precisa corresponder à PK da chamada. UserId e CallId não são identidades alternativas. O vínculo já existente não pode trocar de agente silenciosamente.

Nome NULL pode ser preenchido quando o relacionamento for verificado. Nome anteriormente aplicado pelo catálogo pode acompanhar a renomeação. Nome diferente alterado por outro caminho é preservado e contado em `unverified`; não é sobrescrito automaticamente. UPDATE usa diferença de valor; replay não reescreve linhas idênticas nem timestamps de negócio. Origem sem AgentId válido, identidade divergente ou associação conflitante fica sem atualização.

Agente sem correspondência não exclui a chamada: novas chamadas ficam NULL; um nome histórico já verificado não é apagado quando o agente deixa de aparecer num snapshot posterior. Quando chega referência nova, chamadas ainda NULL podem ser enriquecidas. A projeção representa o **nome atual autorizado**, não garante o nome histórico vigente na data da chamada; effective dating não foi fornecido.

Enriquecimento e estado do catálogo são confirmados juntos. Falha provoca rollback de ambos. COMMIT sem confirmação mantém resultado incerto; reaplicar exatamente o mesmo snapshot reconcilia pelo digest e pelas atualizações idempotentes locais. Não há nova leitura remota. Revisões seguem a ordem das aplicações autorizadas, não uma versão temporal da Olos: selecionar arquivo antigo em `agents-update` pode regredir nomes e exige revisão operacional.

Checkpoints de ingestão, recibos e lotes existentes não são avançados ou reescritos por `agents-update`. Com a migration instalada, `--users` pode inicializar o catálogo ou reapresentar seu mesmo digest; substituição exige `agents-update` explícito. Assim, um arquivo antigo no scheduler não reverte uma atualização autorizada mais nova. Uma mudança de catálogo é recusada enquanto houver ingestão/entrega incompleta; recuperar o lote antes. O mesmo snapshot pode acompanhar replay de recuperação.

Após a configuração, recomenda-se executar o ciclo normal **sem `--users`**, carregando o catálogo global persistido. Atualizá-lo por operação manual separada. Não existe polling de Users. No modo legado sem migration, o catálogo sanitizado em memória permanece disponível pelo `--users`, mas não oferece o controle histórico global: o operador deve instalar a capacidade antes de adotar esse fluxo durável.

## Próximo comando seguro e homologação futura

Sem catálogo autorizado disponível, o ensaio seguro executável agora é exclusivamente sintético e cria seu próprio PostgreSQL descartável:

```bash
cd /home/madm/worker-olos-export-data
npm test -- test/global-agents.test.ts
```

Se já houver um arquivo **sanitizado e autorizado** fora do repositório, o próximo passo é somente validá-lo, sem abrir PostgreSQL/Docker ou SQL Server:

```bash
cd /home/madm/worker-olos-export-data
SYNC_ENABLED=false npm run homologation:offline -- agents-check \
  --users /caminho/privado/agents-sanitized.json
```

Substituir o caminho; arquivo 0600, diretório privado, sem symlinks. Não criar o arquivo por uma nova chamada corporativa nesta etapa. A saída contém apenas contagens e `sqlServerQueries=0`, sem IDs ou nomes.

Para ensaio sintético, preparar **outro** diretório PostgreSQL pelo `prepare` já documentado, sem reaproveitar/remover run-001. Esse prepare explícito instala o controle novo. Depois, uma atualização local autorizada tem a forma:

```bash
SYNC_ENABLED=false npm run homologation:offline -- agents-update \
  --work-dir /caminho/privado/OUTRO-AMBIENTE-ISOLADO \
  --users /caminho/privado/agents-sanitized.json \
  --confirm APPLY_SANITIZED_GLOBAL_AGENTS
```

Repetir esse comando deve retornar `changed=false`, `updated=0` e a mesma revisão, salvo chamadas novas ainda elegíveis. Não executá-lo no run-001 sem a autorização futura de adoção da migration. Os testes já ensaiaram as duas CLIs no ambiente sintético próprio; não foram executados esses exemplos nos dados reais.

## Pendências para ReadData('Users') oficial

- Autorização operacional específica e confirmação de que receber temporariamente todas as colunas, inclusive Password, é permitido. Preferir mudança oficial na origem para uma interface mínima se essa coleta transitória não puder ser autorizada; a conta OEC não permite construir SELECT/view por conta própria.
- Resolver confiança/identidade TLS sem herdar a exceção Disposition nem mudar defaults globais.
- Validar metadados reais de Users, nulabilidade, cardinalidade de AgentId, IDs sentinelas, completude/volume, ReturnValue/outputs e semântica de snapshot consistente na instalação atual.
- Implementar capacidade Users no transporte existente com allowlist de aquisição, sanitização antes de spool e controle de tentativa única/erro sem exposição da resposta. A função pura preparada não equivale a essa autorização/capacidade.
- Definir retenção/atualização do catálogo e precedência temporal dos snapshots. Recusar resposta parcial ou erro após parte do dataset; nenhum snapshot parcial pode substituir o atual.
- Manter liberação incremental Attempts como assunto separado: `ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED` continua intacto, assim como as pendências de controle compartilhado, TLS e recuperação remota.

## Resultados dos testes

- `npm run typecheck`: aprovado.
- `npm run build`: aprovado.
- `npm test -- test/global-agents.test.ts test/offline-homologation.test.ts test/no-automatic-migrations.test.ts test/projection-delivery.test.ts test/projection-transform.test.ts test/persistence.test.ts test/scheduler.test.ts`: **116 testes aprovados, 7 arquivos**, incluindo os oito novos casos e regressões diretamente relacionadas; execução final de 31,86 s.

Demonstrado: dois agentes em campanhas distintas preenchem 380 chamadas sintéticas e 190 desconhecidas permanecem NULL; UserId distinto não faz associação; conflito e campos sensíveis são recusados; atualização posterior adiciona outro agente e renomeia apenas campos de posse verificada; replay não reescreve linhas nem avança checkpoint/recibos. Falha provoca rollback do catálogo e nomes; concorrência é recusada pelo lock compartilhado. As CLIs agents-check/agents-update foram executadas com arquivos sintéticos privados e saídas agregadas. Nova ingestão usa o catálogo persistido e preserva nome não verificado editado externamente. O ensaio de 570 sintéticos conserva todas as outras colunas, contagem de identidades e controles; as 570 chamadas reais não foram usadas nos testes.

Não foi necessário executar a suíte completa de TDS/SQL Server, pois esses componentes não foram alterados. Hashes anteriores conferidos: somente model/store offline, adaptador offline e os dois documentos existentes/README mudaram; migrations anteriores, driver, XML (mantida a implementação de fragmentos), configuração de segurança, scheduler e testes anteriores preservados. Não há conexão SQL Server neste novo ensaio: apenas respostas sintéticas em memória e PostgreSQL descartável próprio. Não houve operação corporativa, mudança de WireGuard/serviços VPS/discadora, commit, push ou deploy.
