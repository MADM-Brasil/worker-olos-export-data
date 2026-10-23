# Diagnóstico manual de TLS da Olos — PRELOGIN, sem login SQL

## Escopo

A ferramenta separada `npm run diagnose:olos-tls -- ...` identifica o certificado público apresentado em `10.196.234.11:1433`. Não foi executada contra a Olos nesta implementação. O banco ExportDataV3 é contexto operacional fornecido pela empresa: nenhum nome de banco, usuário ou senha é enviado, pois não há LOGIN7.

O diagnóstico encerra após PRELOGIN e negociação TLS. Não importa o runtime, não instancia `Connection`/`Request`, não envia autenticação, SQL, RPC ou ATTENTION, não inicia extração e não usa PostgreSQL/spool. Não existe opção para consulta/procedure, credencial, host alternativo, versão TLS legada ou cipher permissivo. ResetReadData, DataAvailable e demais procedures continuam sem acesso por esta ferramenta. Os gates e as aprovações Disposition permanecem independentes e inalterados.

O diretório `/home/madm/olos-phase4-test/run-001` e seu PostgreSQL não foram usados, preparados novamente ou removidos. Os testes utilizam instâncias descartáveis próprias, com IDs novos.

## Estratégia de transporte

No TDS 7.x, TLS não começa diretamente no socket TCP: primeiro ocorre uma troca PRELOGIN; o handshake é encapsulado em pacotes TDS. Isso é definido na especificação [MS-TDS, PRELOGIN](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/60f56408-0188-4cd5-8b90-25c6f2423868). Portanto, `openssl s_client -connect host:1433` sem suporte a TDS não é equivalente a este diagnóstico.

A API pública de conexão do Tedious continua do handshake para LOGIN7. Para impedir essa continuação, o módulo usa somente `MessageIO`, `PreloginPayload` e `Debug` internos, lidos do **Tedious 18.6.2 fixado**. Não há alteração de node_modules, substituição de imports ou monkey patch de protótipos. `Debug` não tem listeners nem captura de payload. Esses módulos não constituem API pública estável: atualização do driver exige revisão e repetição dos testes.

Sequência de uma execução:

1. Validar configuração, versão do driver, CA pública opcional e ausência de `NODE_TLS_REJECT_UNAUTHORIZED=0`.
2. Abrir **uma** conexão TCP no endereço fixo, sem resolver outro endpoint e sem retries.
3. Enviar PRELOGIN com criptografia solicitada; validar tipo, limites e offsets da resposta. Apenas ENCRYPT_ON/ENCRYPT_REQ permitem prosseguir. OFF/NOT_SUP não causam fallback para login em claro.
4. Negociar TLS pelos encapsulamentos do próprio Tedious, exclusivamente TLS 1.2, com as cifras padrão do Node/OpenSSL. Não reduzir o nível de segurança para acomodar servidores antigos. O teto TLS 1.2 corresponde à implementação TDS 7.x desse driver, não a uma conclusão sobre protocolos disponíveis na Olos.
5. Ler o certificado público da sessão e fechar todos os streams/socket, antes de qualquer LOGIN7.

Orçamento total de 10 segundos, 256 KiB recebidos e 16 KiB para a resposta inicial PRELOGIN. Respostas excessivas, truncadas ou malformadas falham; timeout não reinicia a conexão. Os testes injetam outros limites temporais apenas em loopback. Não há redirecionamento, pooling, login retry ou segunda tentativa de inspeção depois de uma falha estrita.

## Modos e resultado

### Diagnóstico com a aprovação corporativa existente

`verify-corporate-pinned` aceita a aprovação `OLOS_ATTEMPTS_TDS_ACQUISITION` já usada pelo V2, carregada por `loadOlosPinApproval()`. O modo anterior `verify-pinned` continua restrito à aprovação de metadados. Não criar ou editar aprovações para executar este diagnóstico.

O novo modo instancia o próprio `PinnedMessageIO` corporativo: TLS 1.2, `rejectUnauthorized: true`, `ca` igual ao PEM aprovado, `allowPartialTrustChain: true` e verificação do pin SHA-256. Executa uma conexão PRELOGIN/TLS com timeout total de 10 segundos. Não instancia `Connection`/`Request`, não lê credenciais SQL e não envia LOGIN7, consultas ou RPC. Não usa scheduler, PostgreSQL, spool ou recuperação. Não exporta certificados nem escreve arquivos.

No checkout, apontar `OLOS_EXISTING_PIN_APPROVAL_FILE` para o mesmo arquivo indicado em `pinApprovalFile` na configuração V2. A variável abaixo é apenas um caminho local; nenhuma aprovação é criada:

```bash
SYNC_ENABLED=false npm run --silent diagnose:olos-tls -- \
  --authorize TDS_TLS_METADATA_ONLY \
  --mode verify-corporate-pinned \
  --server-name 10.196.234.11 \
  --pin-approval-file "${OLOS_EXISTING_PIN_APPROVAL_FILE:?Defina o caminho da aprovacao existente}"
```

Na imagem de produção que contenha esta alteração, o equivalente é `node dist/cli/adapters/olos-tls-diagnostic.js` seguido dos mesmos argumentos, com `SYNC_ENABLED=false`. Usar o caminho da aprovação **dentro do container**, já montado para o worker; não alterar mounts ou permissões. Nenhum comando contra a Olos foi executado durante esta implementação.

A saída desse modo contém exclusivamente `code`, `stage` e `fingerprintSha256` do certificado **remoto**, ou `null` quando indisponível. Exemplo de falha, com o hash omitido neste exemplo:

```json
{"code":"DEPTH_ZERO_SELF_SIGNED_CERT","stage":"TLS_HANDSHAKE_STARTED","fingerprintSha256":null}
```

O hash é observado antes de o [handler TLS do Node 22.23.4](https://github.com/nodejs/node/blob/v22.23.4/lib/internal/tls/wrap.js#L1663-L1711) encerrar a sessão rejeitada. A observação usa o evento interno `secure`, sem substituir a validação da cadeia ou do pin; essa dependência requer regressão ao atualizar Node. Não imprime PEM, subject, issuer, mensagens de erro, stacks ou bytes TLS/TDS. Códigos técnicos conhecidos são preservados, incluindo `DEPTH_ZERO_SELF_SIGNED_CERT`, `CERT_HAS_EXPIRED` e `PIN_MISMATCH`; erros desconhecidos recebem código fixo sanitizado.

Comparar externamente o hash retornado com o aprovado `9F:48:BD:96:77:FE:38:C9:76:68:44:C8:50:EE:4F:07:22:D1:AD:5F:E6:83:F7:8F:2F:88:D4:B9:CA:93:1E:37`:

- Hash diferente: o certificado observado nessa conexão difere do aprovado. Não renovar a aprovação automaticamente; investigar endpoint/certificado apresentado.
- Hash igual com erro: a hipótese de substituição do certificado não explica essa sessão; a falha de validação continua vigente e requer investigação do contexto TLS efetivo.
- `null`: certificado indisponível; não concluir igualdade ou divergência.
- `PIN_VERIFIED` / `TLS_VALIDATED`: autenticação concluída nessa conexão. Não autoriza aquisição, não encerra tentativas pendentes e não altera recovery.

Exit 0 somente em `PIN_VERIFIED`; exit 1 em falha de transporte/TLS; exit 64 em rejeição local de argumentos/aprovação. Não há retry ou fallback para outro modo.

Validação desta alteração: 147 testes direcionados em oito arquivos, incluindo 13 novos casos em `test/corporate-tls-diagnostic.test.ts`, typecheck e build aprovados em Node.js 22.23.4 / OpenSSL 3.5.9 / Tedious 18.6.2. O novo peer sintético comprova uma conexão, apenas pacotes PRELOGIN e zero bytes de aplicação após TLS. `DEPTH_ZERO_SELF_SIGNED_CERT` foi reproduzido com certificado remoto diferente do aprovado, preservando seu hash; isso não demonstra divergência do certificado na Olos real. A CLI compilada também recusou aprovação ausente com exit 64 e somente os três campos previstos, antes de conectar.

### Modos anteriores

| Modo | Validação TLS | Resultado |
|---|---|---|
| `verify` (padrão) | `rejectUnauthorized=true`; cadeia, validade e nome esperado verificados pelo Node; checagem explícita adicional do nome | `VERIFIED`, `verified=true`, exit 0 somente em sucesso |
| `inspect-unverified` | Exceção local exclusivamente para receber o certificado durante o handshake; nunca reutilizada pelo worker | `INSPECTED_UNVERIFIED`, sempre `verified=false`, exit 2, mesmo se a cadeia for conhecida |

A API [TLS do Node.js](https://nodejs.org/docs/latest-v20.x/api/tls.html) documenta `rejectUnauthorized`, `checkServerIdentity` e `getPeerCertificate`. No modo estrito, falha de verificação pode impedir a obtenção dos metadados; não há fallback automático para inspeção. No modo informativo, o campo `name` informa separadamente se o nome fornecido corresponde ao certificado. `chain=unverified` significa que a verificação TLS não foi aprovada, e não prova, isoladamente, que o emissor seja desconhecido: uma falha de nome também impede a autorização do socket.

A CLI exibe do certificado folha somente `subject`, `issuer`, `validFrom`, `validTo` e `fingerprintSha256`. O núcleo preserva SAN na API de metadados para compatibilidade, mas a CLI não o imprime. Nenhum certificado bruto/PEM, chave privada, chave de sessão ou conteúdo TDS é impresso. Os campos de estado são códigos fixos, sem mensagens/stack do driver. A saída é JSON para escapar caracteres do certificado. A exportação PEM para arquivo privado é opcional e explícita, conforme abaixo.

| Código | Interpretação / ação |
|---|---|
| `CERT_UNTRUSTED` | Cadeia autoassinada ou emissor não confiável/incompleto; solicitar CA/cadeia à equipe responsável |
| `CERT_NAME_MISMATCH` | Nome esperado não corresponde ao certificado; verificar SAN e nome aprovado, sem contornar a validação |
| `CERT_EXPIRED` | Certificado fora da validade, inclusive ainda não válido; conferir também relógio do cliente |
| `CERT_INVALID` | Outra falha de certificado |
| `TLS_INCOMPATIBLE` | Falha de negociação TLS, protocolo, cifras ou política criptográfica; código não determina sozinho a causa exata |
| `TLS_NOT_SUPPORTED` | Servidor não negociou criptografia; parar sem autenticar |
| `NETWORK_FAILURE` / `TIMEOUT` | Conexão recusada, interrompida, inalcançável ou sem progresso dentro do orçamento; não prova qual componente da rede/VPN falhou |
| `INVALID_TDS` / `LIMIT_EXCEEDED` | Resposta inesperada/malformada ou orçamento excedido; parar |
| `UNSUPPORTED_DRIVER` / `INVALID_CONFIGURATION` | Incompatibilidade local; nenhuma conexão é iniciada na validação inicial |

Exit 1 indica falha do diagnóstico. Exit 64 indica rejeição de argumentos, autorização, ambiente ou leitura do arquivo CA. CA inválida detectada pelo núcleo retorna `INVALID_CONFIGURATION`, exit 1. Não inferir autorização de produção a partir de um exit code.

## Uma execução manual futura, explicitamente autorizada

**Nenhum comando desta seção foi executado contra a Olos durante o desenvolvimento.** Obter aprovação operacional para uma conexão PRELOGIN/TLS no endpoint acima, com modo e janela definidos. A flag de autorização é confirmação local do operador; não é assinatura nem mecanismo de autorização corporativa independente. A ferramenta não registra uma aprovação durável nem impede uma segunda invocação manual: cada repetição exige nova autorização.

Pré-condições: Node.js 20 compatível, dependências instaladas do lockfile (Tedious 18.6.2), VPN previamente disponível e `SYNC_ENABLED=false`. Não carregar o arquivo protegido de credenciais da integração: esta CLI não precisa dele. Não alterar WireGuard ou serviços. Não reutilizar comandos `prepare`, `approve` ou `read` da integração Disposition.

Para **uma única inspeção informativa** quando ainda não existe CA/nome de certificado confirmados, executar manualmente, após aprovação:

```bash
cd /home/madm/worker-olos-export-data
SYNC_ENABLED=false npm run diagnose:olos-tls -- \
  --authorize TDS_TLS_METADATA_ONLY \
  --mode inspect-unverified \
  --server-name OLOSMADSPODTB01
```

`OLOSMADSPODTB01` é apenas o nome de servidor anteriormente informado, **não um SAN previamente confirmado**. Nesse exemplo ele serve como candidato para a checagem de nome; qualquer metadado recebido permanece sem autenticação de identidade. Exit 2 é o resultado informativo esperado, não uma validação. Não anexar `|| retry`, loops ou um comando de extração depois dele. Se a sessão falhar antes de receber o certificado, pode não haver metadados.

Se a empresa já tiver fornecido a CA pública e o nome DNS esperado por um canal confiável, escolher **em vez da inspeção**, sob aprovação para essa única execução, o modo estrito:

```bash
SYNC_ENABLED=false npm run diagnose:olos-tls -- \
  --authorize TDS_TLS_METADATA_ONLY \
  --mode verify \
  --server-name NOME_DNS_APROVADO_NO_CERTIFICADO \
  --ca-file /caminho/protegido/ca-publica-aprovada.pem
```

Substituir os dois placeholders previamente. A CA pode ser omitida quando a cadeia já pertence à trust store do Node. O arquivo deve conter apenas certificados PEM públicos, ser arquivo regular (sem symlink), com até 128 KiB. A CLI nunca pede chave privada nem altera a trust store. Um arquivo PEM recebido da sessão não deve ser automaticamente promovido a CA confiável.

**Critérios de parada:** qualquer falha, timeout, divergência de nome, cadeia/validade não aprovada ou modo informativo exige revisão humana. Não executar automaticamente um segundo modo, não repetir a conexão, não ativar sync, não mudar `trustServerCertificate`, TLS mínimo, cifras ou `NODE_TLS_REJECT_UNAUTHORIZED`. Comparar fingerprint, SAN, emissor e validade com a equipe responsável por canal independente. Identificar o certificado em uma sessão não prova sua legitimidade nem valida a configuração futura da integração. Uma validação estrita posterior, se necessária, exige nova autorização e fica fora da primeira execução.

## Exportação explícita do certificado público no container

A opção `--export-certificate-pem /caminho/absoluto/arquivo.pem` salva somente o certificado público folha apresentado no mesmo handshake. Não envia LOGIN7/SQL e não lê credenciais ou chamadas. Não gera aprovação de pin. Em `inspect-unverified`, o certificado permanece **observado e não autenticado**, mesmo com exportação concluída.

O diretório pai deve existir, pertencer ao usuário do processo e não permitir acesso de grupo/outros (usar 0700), sem symlinks no caminho. O destino precisa ser novo; arquivo/symlink existente é recusado antes de conectar. Gravação usa staging privado 0600, fsync e publicação sem sobrescrita; disputa pelo nome retorna `CERT_EXPORT_FAILED`, sem reconexão. Falha de TLS não publica PEM; staging é removido ao terminar normalmente. Queda abrupta pode deixar um `.tmp` privado que não deve ser confundido com certificado exportado.

Na imagem de produção, `diagnose:olos-tls` aponta para `node dist/cli/adapters/olos-tls-diagnostic.js`; o checkout de desenvolvimento mantém `tsx`. **Os comandos abaixo exigem a imagem atualizada por um fluxo de redeploy autorizado separado.** Nenhum deploy ou conexão corporativa foi executado nesta implementação.

O mount `/home/node/.config/olos-worker` é readonly; usar o volume de estado já existente para a exportação, sem alterar Compose ou mounts. No host da VPS, depois da atualização autorizada da imagem:

```bash
WORKER=olos-export-data-workerolosexportdata-ack5ed-worker-1

sudo docker exec "$WORKER" node -e 'const fs=require("node:fs"); const p="/home/node/.local/state/olos-worker/tls-observed"; fs.mkdirSync(p,{recursive:true,mode:0o700}); fs.chmodSync(p,0o700);'

sudo docker exec -e SYNC_ENABLED=false "$WORKER" \
  npm run diagnose:olos-tls -- \
  --authorize TDS_TLS_METADATA_ONLY \
  --mode inspect-unverified \
  --server-name 10.196.234.11 \
  --export-certificate-pem /home/node/.local/state/olos-worker/tls-observed/olos-public.pem

echo "exit_code=$?"
```

Exit 2 com `INSPECTED_UNVERIFIED` é o resultado informativo esperado; arquivo exportado não muda `verified:false`. Exit 64 indica configuração/autorização/destino recusado antes da conexão; exit 1 é falha de diagnóstico/exportação. Não sobrescrever o arquivo nem repetir automaticamente a conexão em falha. Se `CERT_EXPORT_FAILED` ocorrer após publicação, o arquivo pode existir: conferir estado local antes de qualquer outra tentativa.

Conferir 0600 e os dois hashes sem imprimir o PEM:

```bash
sudo docker exec "$WORKER" node -e 'const fs=require("node:fs"), crypto=require("node:crypto"); const p="/home/node/.local/state/olos-worker/tls-observed/olos-public.pem", s=fs.lstatSync(p); if(!s.isFile()||s.isSymbolicLink()||s.uid!==process.getuid()||(s.mode&0o777)!==0o600)throw Error("PRIVATE_PEM_REQUIRED"); const pem=fs.readFileSync(p), cert=new crypto.X509Certificate(pem); console.log(JSON.stringify({fingerprintSha256:cert.fingerprint256,pemFileSha256:crypto.createHash("sha256").update(pem).digest("hex")}));'
```

`fingerprintSha256` é o SHA-256 do certificado DER e deve coincidir com o diagnóstico e com a confirmação administrativa independente. `pemFileSha256` é o hash dos bytes do arquivo PEM, útil para conferir transferência; os dois hashes são diferentes por representarem formatos distintos. Nenhum hash observado autoriza a aquisição por si só. Não solicitar nem copiar a chave privada do servidor.

Validação da exportação em 20/10/2026: typecheck/build aprovados; **74 testes direcionados em cinco arquivos** (`tls-certificate-export`, `tls-diagnostic`, `olos-certificate-pin`, `attempts-corporate-pinning`, `attempts-corporate-diagnostics`) passaram. A CLI compilada executou `--help` sem loader TypeScript. Os testes novos usam apenas peer TDS/TLS sintético em loopback e comprovam PEM/fingerprint corretos, modo 0600, ausência de PEM no stdout, nenhuma escrita sem opção explícita, recusa de diretório inseguro/symlink/sobrescrita, colisão durante o handshake e zero bytes de aplicação/LOGIN7/SQL. Nenhuma conexão corporativa, alteração de aquisição/scheduler/Compose/PostgreSQL/WireGuard, commit, push ou deploy foi realizada nesta atualização.

## Arquivos e testes

- `src/diagnostics/tds-tls.ts`: transporte PRELOGIN/TLS limitado, validações e metadados públicos.
- `adapters/olos-tls-diagnostic.ts`: CLI manual independente, endpoint fixo, autorização local e leitura restrita de CA pública.
- `test/support/tls-diagnostic-fixture.ts`: certificados sintéticos temporários e peer TDS/TLS sintético com observação dos bytes de aplicação descriptografados.
- `test/tls-diagnostic.test.ts`: confiança, nome, validade, modo informativo, ausência de LOGIN7/RPC/SQL, fragmentação, timeout no PRELOGIN/TLS, interrupções, limites e isolamento de configuração.
- `test/tls-diagnostic-sqlserver.test.ts`: handshake com SQL Server Developer 2022 real, descartável, somente em loopback; recusa de cadeia/nome e inspeção informativa.
- `package.json`: comando npm explícito, sem hooks automáticos. README: link para este procedimento.

A fixture SQL Server usa o helper existente, que autentica e cria um banco **apenas para preparar seu próprio container**. Essa preparação é distinta da conexão sob teste, que recebe somente host/porta/nome/CA e não possui credenciais ou API de SQL. A evidência de ausência de LOGIN7/SQL inclui o peer sintético: todos os pacotes do cliente anteriores à mudança para TLS são PRELOGIN e zero bytes de aplicação são recebidos após o handshake. O teste com SQL Server real comprova compatibilidade do handshake nesta fixture, sem afirmar certificação da instalação Olos.

Validação executada em Node.js 20.20.2, Tedious 18.6.2 e Docker disponível:

| Comando | Resultado |
|---|---|
| `npm run typecheck` | Aprovado, exit 0 |
| `npm run build` | Aprovado, exit 0 |
| `npm test` | **395 testes aprovados, 28 arquivos**, 101,91 s; 359 testes anteriores preservados e 36 novos |
| `SYNC_ENABLED=false npm run diagnose:olos-tls -- --help` | Aprovado; somente ajuda local, sem conexão |

Os 36 novos testes compreendem 33 casos sintéticos/configuração/isolamento e três casos em SQL Server Developer 2022 efetivo (16.x), com TLS 1.2 e CA gerada exclusivamente para a fixture. Nenhum servidor sintético foi apresentado como evidência de SQL Server real. Certificados e chaves sintéticos são temporários e removidos ao terminar os testes. Os testes anteriores de PostgreSQL e carga continuaram usando seus próprios containers descartáveis.

Hashes SHA-256 dos arquivos preexistentes foram comparados antes/depois: todos os arquivos anteriores de `src/olos/`, `src/integration/`, `src/config/`, migrations e testes permaneceram idênticos. As únicas alterações em arquivos anteriores são o comando npm em `package.json` e o link documental em README; os demais arquivos desta entrega são novos. Nenhuma configuração de credencial ou arquivo de `run-001` foi lido para implementar/validar este diagnóstico.

Nenhuma procedure/consulta Olos, migration corporativa, modificação da integração Disposition, commit, push ou deploy foi executado. Não há certificação nova da instalação Olos, da extração binária, da persistência PostgreSQL ou de uso em produção.
