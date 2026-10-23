# Pacote local para preparar observe na VPS

Preparado em 20/10/2026, sem acesso à VPS/Olos/PostgreSQL. **Não autoriza aquisição.** Reutiliza os arquivos referenciados em `~/.config/olos-worker/attempts-corporate.preparation.json`; esse arquivo original NÃO foi copiado, pois contém credenciais. `run-002/state.json` também contém senha e não faz parte do pacote.

## Origens e conteúdo exatos

| Origem local | Nome no pacote | Verificação |
| --- | --- | --- |
| `/home/madm/olos-offline-homologation/profile-16-columns.json` | `profile-16-columns.json` | 16 colunas na ordem de APPROVED_COLUMNS, igual ao perfil salvo de run-002, phoneMatchKey=normalized, tmaPolicy=disabled |
| `/home/madm/olos-offline-homologation/run-002/archive/3b0f69440ba5b62d6df6269b2f2aabe6480c68dc2bff33aab721936ce061da60.json` | `campaign.json` | 51 IDs únicos, hash original preservado |
| `/home/madm/olos-offline-homologation/run-002/archive/97ee3d262adf8811c4a3fe61111c2714ef36fbfc09d3ac0433668a186c8f77ec.json` | `disposition.json` | 126 IDs únicos, hash original preservado |

São os snapshots já referenciados pela preparação local, não catálogos recém-consultados nem nova aprovação corporativa. O perfil contém aprovação do mapeamento de homologação, não aprovação do contrato TDS/partições da Olos. Os três arquivos foram copiados sem modificar seus bytes; os catálogos passaram pelos parsers existentes e não incluem registros/arquivos de chamadas ou Users.

Diretório preparado fora do Git e do contexto Docker:

```text
/home/madm/.local/share/olos-worker/observe-transfer-20261020/
  profile-16-columns.json
  campaign.json
  disposition.json
  attempts-corporate.observe.template.json
  SHA256SUMS
```

Arquivo para transferência, permissão 0600:

```text
/home/madm/.local/share/olos-worker/observe-transfer-20261020.tar.gz
SHA-256: ebf0cf68a6dd2d286bbc7876c5438abbff938a776b837f5d66c8ed1dcd305246
```

Não contém senha Olos/PostgreSQL, tokens, certificado/aprovação TLS, preparação original, estado de homologação, spool, backups ou os 570 históricos. Não copiar diretórios inteiros `archive`, `run-002` ou `~/.config/olos-worker`.

O modelo usa diretamente `version`, `contractMode: observe`, endpoint sem usuário/senha e os três caminhos absolutos em `/home/node/.config/olos-worker/`. Dispensa `attempts-corporate.preparation.json`. Não inclui `current` ou `previous`. `endpoint.user`, `endpoint.password`, `pinApprovalFile` e `authorization` estão deliberadamente ausentes: o modelo é **incompleto e não executável**. Não criar valores fictícios para passar o schema.

## Transferir e instalar manualmente

Os comandos abaixo são instruções futuras, não foram executados. Usar o login SSH administrativo real e a chave já autorizada. Confirmar a chave do host por canal confiável antes de usar `StrictHostKeyChecking=yes`; não usar aceitação automática ou desabilitar a checagem.

No computador local:

```bash
cd /home/madm/.local/share/olos-worker/observe-transfer-20261020
sha256sum -c SHA256SUMS
scp -p -o StrictHostKeyChecking=yes /home/madm/.local/share/olos-worker/observe-transfer-20261020.tar.gz <USUARIO_SSH>@179.198.127.41:~/observe-transfer-20261020.tar.gz
ssh -o StrictHostKeyChecking=yes <USUARIO_SSH>@179.198.127.41
```

Na sessão SSH, conferir/extrair em staging privado:

```bash
set -eu
umask 077
ARCHIVE="$HOME/observe-transfer-20261020.tar.gz"
printf '%s  %s\n' 'ebf0cf68a6dd2d286bbc7876c5438abbff938a776b837f5d66c8ed1dcd305246' "$ARCHIVE" | sha256sum -c -
STAGE=$(mktemp -d "$HOME/olos-observe-install.XXXXXX")
tar -xzf "$ARCHIVE" --no-same-owner -C "$STAGE"
(cd "$STAGE" && sha256sum -c SHA256SUMS)
```

Identificar o **Source real do bind** cujo Destination é `/home/node/.config/olos-worker`, no worker já implantado. Não supor o caminho de instalação do Dokploy:

```bash
docker inspect <ID_REAL_DO_WORKER> --format '{{range .Mounts}}{{if eq .Destination "/home/node/.config/olos-worker"}}{{println .Source}}{{end}}{{end}}'
```

Esse Source corresponde ao `../files/olos-private` do serviço. Preencher `PRIVATE_DIR` com o caminho absoluto obtido e instalar somente os cinco arquivos novos. O bloco recusa sobrescrita; não altera `worker-local.env`, `postgres-credentials.json`, tokens ou aprovações existentes:

```bash
PRIVATE_DIR='<SOURCE_ABSOLUTO_CONFIRMADO_DO_BIND>'
sudo test -d "$PRIVATE_DIR"
sudo test ! -L "$PRIVATE_DIR"
for file in profile-16-columns.json campaign.json disposition.json attempts-corporate.observe.template.json SHA256SUMS; do
  if sudo test -e "$PRIVATE_DIR/$file" || sudo test -L "$PRIVATE_DIR/$file"; then
    printf 'Arquivo ja existe; revisar antes de instalar: %s\n' "$file" >&2
    exit 1
  fi
done
sudo chown 1000:1000 "$PRIVATE_DIR"
sudo chmod 700 "$PRIVATE_DIR"
for file in profile-16-columns.json campaign.json disposition.json attempts-corporate.observe.template.json SHA256SUMS; do
  sudo install -o 1000 -g 1000 -m 600 "$STAGE/$file" "$PRIVATE_DIR/$file"
done
sudo sh -c 'cd "$1" && sha256sum -c SHA256SUMS' sh "$PRIVATE_DIR"
sudo stat -c '%u:%g %a %n' "$PRIVATE_DIR" "$PRIVATE_DIR/profile-16-columns.json" "$PRIVATE_DIR/campaign.json" "$PRIVATE_DIR/disposition.json" "$PRIVATE_DIR/attempts-corporate.observe.template.json" "$PRIVATE_DIR/SHA256SUMS"
```

Esperado: diretório `1000:1000 700`, cinco arquivos `1000:1000 600`. Não usar chown/chmod recursivo sobre arquivos privados existentes. Se a instalação for interrompida, revisar hashes dos arquivos já instalados antes de continuar; não contornar o bloqueio de sobrescrita.

O bind é de diretório: os novos arquivos ficam visíveis imediatamente no container, mesmo com mount readonly. **Não precisa de build, restart ou redeploy.** No terminal do worker do Dokploy:

```bash
node --input-type=module -e 'import fs from "node:fs/promises"; const dir="/home/node/.config/olos-worker/"; for(const name of ["profile-16-columns.json","campaign.json","disposition.json","attempts-corporate.observe.template.json","SHA256SUMS"]){const s=await fs.lstat(dir+name); if(!s.isFile()||s.isSymbolicLink()||s.uid!==1000||s.gid!==1000||(s.mode&511)!==384)throw Error("PRIVATE_FILE_REJECTED"); console.log(name,"1000:1000 600");}'
npm run worker:attempts -- status --config /home/node/.config/olos-worker/attempts-corporate.observe.template.json
```

O status esperado tem `configuration.valid=false`, `CORPORATE_CONFIGURATION_INVALID`, `acquisitionAuthorized=false` e `networkAccessed=false`. Isso confirma que o modelo não fornece aprovações; não é sinal para executar `once`.

## Pendências reais antes da configuração definitiva

- Fornecer usuário/senha SQL Olos reais em `endpoint.user` e `endpoint.password` por canal privado, fora deste pacote. Os logins PostgreSQL de runtime não servem como credencial Olos.
- Obter PEM/SHA-256 confirmado independentemente e aprovação vigente para `OLOS_ATTEMPTS_TDS_ACQUISITION`; somente então adicionar `pinApprovalFile` apontando ao arquivo privado real. TCP funcionando não autentica TLS.
- Registrar autorização operacional real, responsável/referência e emissão/expiração (máximo 12h), `sourceTimeZone` confirmado e evidências reais de coordenação de consumo e recuperação. A recuperação deve cobrir tentativa interrompida sem PK recebida, atribuição à faixa de LogReadData e retenção/completude de ReReadData.
- Somente após essas evidências, revisar o modelo e criar separadamente `/home/node/.config/olos-worker/attempts-corporate.json`, sem `current`/`previous` no modo observe. Não renomear o modelo incompleto para esse nome como se fosse autorização.

Não executar procedures Olos, `once`, `recover` ou `start` nesta instalação. Nenhum arquivo de banco, scheduler, migration ou código de runtime foi alterado. O pacote e seus hashes foram verificados localmente; instalação/permissões da VPS continuam a cargo do procedimento manual.
