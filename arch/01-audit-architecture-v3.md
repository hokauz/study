# Arquitetura de auditoria — Clients e Usuários (IDP/OAuth2) — v2

> Changelog v2: seção 4.2 (activity log) reescrita com arquitetura de 3 camadas para controlar
> crescimento em Postgres puro — estado atual, evento bruto particionado e agregado diário.
>
> Changelog v3: adicionadas as seções 4.2.1 (por que não reter evento bruto a longo prazo,
> mesmo particionado), 4.2.2 (mecanismo de export automatizado pré-drop, S3 + Parquet)
> e 7 (referências usadas para os padrões de audit trail e logs deste documento).

## 1. Objetivo

Fornecer um histórico confiável de mudanças de estado (audit trail) e de uso operacional (activity log) para dois domínios:

- **Clients** (aplicações registradas no IDP)
- **Usuários** (usuários finais), com dois vieses:
  - **Administrativo**: quem criou/alterou o registro do usuário e o quê mudou
  - **Operacional**: como o usuário interage com o sistema (login, provider, MFA, client acessado)

## 2. Por que separar audit trail de activity log

São dois modelos de dado com características opostas. Tratá-los na mesma tabela leva a problemas de retenção, volume e performance de query.

| Característica | Audit trail | Activity log |
|---|---|---|
| Volume | Baixo (eventos de mudança de estado) | Alto (todo login, toda troca de token) |
| Retenção | Longa (anos, requisito de compliance) | Curta/média (semanas a poucos meses no banco quente) |
| Mutabilidade | Nunca — imutável por definição | Imutável, mas pode ser sumarizado/agregado com o tempo |
| Consulta típica | "quem alterou X e quando" | "quantos logins, de qual provider, em que client" |
| Growth pattern | Linear com nº de mudanças administrativas | Linear com nº de requisições/logins |

Misturar as duas na mesma tabela normalmente resulta em: (a) a tabela de auditoria "administrativa" virando gigante e lenta de consultar, e (b) políticas de retenção conflitantes aplicadas ao mesmo lugar.

## 3. Arquitetura de alto nível

```
API atual (IDP/OAuth2)
   │  grava evento na mesma transação local (outbox pattern)
   ▼
outbox_events (tabela na própria API atual)
   │  consumidor assíncrono (poller ou CDC/Debezium)
   ▼
Serviço/consumidor de auditoria
   │
   ├──► audit_events        (banco de auditoria — baixo volume, retenção longa)
   └──► activity log        (banco de auditoria — 3 camadas, ver seção 4.2)
              │
              ▼
     API de consulta de auditoria (read-only, cacheável)
```

### Por que outbox pattern em vez de gravação síncrona direta

Se a API atual gravar diretamente no banco de auditoria de forma síncrona:

- Cria acoplamento forte: uma indisponibilidade do banco de auditoria derruba login/CRUD de client.
- Dificulta a integração gradual mencionada no requisito (você teria que tocar todos os fluxos de uma vez).

Com outbox pattern:

1. A API grava a mudança de negócio + o evento de auditoria na **mesma transação local** (ambos no banco da API atual).
2. Um processo separado (poller simples ou CDC como Debezium) lê a tabela de outbox e publica/grava no banco de auditoria.
3. Cada fluxo pode ser migrado um de cada vez — você adiciona o insert no outbox naquele fluxo específico, sem afetar os demais.

**Nota sobre fire-and-forget puro (sem outbox):** resolve o problema de latência (o fluxo principal não espera o banco de auditoria responder), mas não resolve perda silenciosa de eventos — se o banco de auditoria estiver indisponível no momento exato da chamada, o evento desaparece sem retry. Para `audit_events` (baixo volume, alto valor de compliance) isso é inaceitável, por isso o outbox é a escolha para essa tabela. Para o `activity_log` de login, o trade-off é mais aceitável dado o volume — ver seção 4.2.

## 4. Modelo de dados

### 4.1 Audit trail (client e usuário administrativo)

Uma única tabela genérica cobre ambos os domínios (client e user), usando `target_type` para diferenciar. Isso evita duplicar schema para cada entidade nova que precisar de auditoria no futuro (provider, MFA policy, etc).

```sql
CREATE TABLE audit_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- quem realizou a ação
    actor_id        UUID,                 -- null se for o próprio sistema
    actor_type      VARCHAR(20) NOT NULL, -- 'system' | 'admin' | 'user' | 'client_credential'
    actor_label     VARCHAR(255),         -- snapshot do nome/email no momento (evita join p/ registro deletado)

    -- sobre o que a ação foi realizada
    target_type     VARCHAR(30) NOT NULL, -- 'client' | 'user' | 'mfa_factor' | 'provider' | ...
    target_id       UUID NOT NULL,
    target_label    VARCHAR(255),         -- snapshot (ex: client_name, user_email)

    action          VARCHAR(50) NOT NULL, -- 'created' | 'updated' | 'deleted' | 'mfa_added' |
                                           -- 'password_reset_forced' | 'secret_rotated' | ...

    changes         JSONB,                -- diff: { "field": { "old": ..., "new": ... } }
    metadata        JSONB,                -- contexto extra: ip, request_id, reason, ticket_id

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_target ON audit_events (target_type, target_id, occurred_at DESC);
CREATE INDEX idx_audit_actor  ON audit_events (actor_id, occurred_at DESC);
CREATE INDEX idx_audit_action ON audit_events (action, occurred_at DESC);
```

**Por que `changes` em JSONB e não uma tabela por campo alterado:** permite adicionar novos campos auditáveis em qualquer entidade sem migração de schema, e a maioria das consultas de auditoria já busca por `target_id` (traz o registro inteiro e itera no JSON na aplicação).

**Por que snapshot (`actor_label`, `target_label`):** se o usuário ou client for deletado depois, o log de auditoria não pode depender de um JOIN que retorna vazio — o registro tem que ser autocontido.

Equivalente em TypeScript (para o SDK/service mencionado):

```typescript
type ActorType = 'system' | 'admin' | 'user' | 'client_credential';

type TargetType = 'client' | 'user' | 'mfa_factor' | 'provider';

type AuditAction =
  | 'created'
  | 'updated'
  | 'deleted'
  | 'mfa_added'
  | 'mfa_removed'
  | 'password_reset_forced'
  | 'secret_rotated'
  | 'status_changed';

interface FieldChange<T = unknown> {
  old: T;
  new: T;
}

interface AuditEvent {
  id: string; // uuid
  occurredAt: string; // ISO timestamp
  actorId: string | null;
  actorType: ActorType;
  actorLabel: string | null;
  targetType: TargetType;
  targetId: string;
  targetLabel: string | null;
  action: AuditAction;
  changes: Record<string, FieldChange> | null;
  metadata: {
    ip?: string;
    requestId?: string;
    reason?: string;
    ticketId?: string;
    [key: string]: unknown;
  } | null;
}

// Contrato do SDK que a API atual vai chamar/emitir
interface AuditEventInput {
  actorId?: string;
  actorType: ActorType;
  targetType: TargetType;
  targetId: string;
  action: AuditAction;
  changes?: Record<string, FieldChange>;
  metadata?: Record<string, unknown>;
}

interface AuditSDK {
  record(event: AuditEventInput): Promise<void>; // grava no outbox local, não bloqueia
}
```

### 4.2 Activity log (uso operacional do usuário) — arquitetura de 3 camadas

Login é um evento de **alta cardinalidade temporal**: todo usuário ativo gera múltiplas linhas por dia. Modelar isso como "1 linha por evento" numa tabela única, em Postgres comum, degrada de forma previsível: o índice em `(user_id, occurred_at)` cresce sem parar e o autovacuum passa a competir com o tráfego de escrita.

A comunidade (Auth0, WorkOS, Okta, Ory Kratos) resolve isso separando **estado atual**, **evento bruto** e **agregado histórico** em três tabelas com responsabilidades distintas — não uma tabela só tentando servir as três consultas.

```
┌─────────────────────┐     ┌──────────────────────┐     ┌───────────────────────┐
│ user_login_state     │     │ login_events           │     │ login_stats_daily       │
│ 1 linha por usuário   │     │ particionada por tempo │     │ rollup diário           │
│ sempre UPDATE         │     │ retenção curta (7-30d) │     │ retenção longa           │
│ responde: "último     │     │ responde: "investigar  │     │ responde: "tendência de │
│ login? qual client?"  │     │ incidente recente"     │     │ uso por método/client"   │
└─────────────────────┘     └──────────────────────┘     └───────────────────────┘
        ▲                            ▲                              ▲
        │                            │                              │
        └──────────── login bem-sucedido dispara escrita nas 3 ──────┘
                       (state = upsert síncrono leve; events = fire-and-forget/fila;
                        stats = job noturno de agregação, não escrita direta)
```

#### Camada 1 — Estado atual (`user_login_state`)

Resolve a pergunta mais comum ("qual foi o último login desse usuário, com qual client, qual método") sem varrer histórico nenhum. É uma tabela de **estado**, não de evento — sempre `UPSERT`, nunca cresce além do número de usuários.

```sql
CREATE TABLE user_login_state (
    user_id             UUID PRIMARY KEY REFERENCES users(id),

    last_login_at       TIMESTAMPTZ,
    last_client_id      UUID,

    last_auth_method    VARCHAR(30),  -- 'password' | 'mfa_totp' | 'mfa_sms' | 'mfa_webauthn' |
                                       -- 'external:google' | 'external:azure_ad' |
                                       -- 'passwordless_email' | 'biometric'

    last_ip             INET,
    last_user_agent     TEXT,

    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Upsert idempotente, chamado de forma síncrona (é leve — 1 linha, chave primária, sem scan):

```sql
INSERT INTO user_login_state (user_id, last_login_at, last_client_id, last_auth_method, last_ip, last_user_agent, updated_at)
VALUES ($1, now(), $2, $3, $4, $5, now())
ON CONFLICT (user_id) DO UPDATE SET
    last_login_at    = EXCLUDED.last_login_at,
    last_client_id   = EXCLUDED.last_client_id,
    last_auth_method = EXCLUDED.last_auth_method,
    last_ip          = EXCLUDED.last_ip,
    last_user_agent  = EXCLUDED.last_user_agent,
    updated_at       = now();
```

Essa gravação pode ser feita de forma síncrona no fluxo de login (é um UPDATE de 1 linha por PK, custo desprezível) — diferente do evento bruto abaixo, que deve ser assíncrono.

#### Camada 2 — Evento bruto particionado (`login_events`)

Guarda o histórico detalhado, mas com **janela curta de retenção via particionamento**, para permitir investigação de incidente recente sem acumular indefinidamente.

```sql
CREATE TABLE login_events (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY,
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    user_id             UUID NOT NULL,
    client_id           UUID NOT NULL,

    auth_method         VARCHAR(30) NOT NULL,  -- mesmo domínio de user_login_state.last_auth_method
    mfa_method          VARCHAR(20),            -- 'totp' | 'sms' | 'webauthn' — null se não envolveu mfa
    external_provider   VARCHAR(30),            -- 'google' | 'azure_ad' | 'saml:idp_x' — null se local

    ip_address          INET,
    user_agent          TEXT,
    success             BOOLEAN NOT NULL DEFAULT true,

    metadata            JSONB,                  -- detalhes extras específicos do método usado

    PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE INDEX idx_login_events_user   ON login_events (user_id, occurred_at DESC);
CREATE INDEX idx_login_events_client ON login_events (client_id, occurred_at DESC);
```

**Particionamento — automatizado com `pg_partman`, não cron manual:**

Criar e derrubar partições manualmente é a parte que mais falha em produção (alguém esquece de criar a partição do mês seguinte, inserts começam a falhar). A extensão `pg_partman` resolve isso:

```sql
CREATE EXTENSION IF NOT EXISTS pg_partman;

SELECT partman.create_parent(
    p_parent_table      => 'public.login_events',
    p_control           => 'occurred_at',
    p_type              => 'range',
    p_interval          => '1 week',   -- semanal se volume alto; 'monthly' se volume moderado
    p_premake           => 4            -- cria 4 partições futuras com antecedência
);

-- Política de retenção: derruba partições com mais de 30 dias automaticamente
UPDATE partman.part_config
SET retention = '30 days',
    retention_keep_table = false  -- false = DROP de fato; true = apenas desanexa (para mover a cold storage antes)
WHERE parent_table = 'public.login_events';
```

Com `retention_keep_table = true`, a partição é desanexada (não apagada) antes do drop — dá uma janela para rodar um export para S3/Parquet antes de remover de fato, se precisar de retenção regulatória maior que 30 dias.

**Por que `DROP PARTITION` em vez de `DELETE`:** `DELETE` em massa gera dead tuples que o autovacuum precisa limpar depois, competindo com o tráfego de escrita normal. `DROP` de uma partição é instantâneo e não deixa bloat.

TypeScript equivalente:

```typescript
type AuthMethod =
  | 'password'
  | 'mfa_totp'
  | 'mfa_sms'
  | 'mfa_webauthn'
  | 'external_google'
  | 'external_azure_ad'
  | 'passwordless_email'
  | 'biometric';

interface LoginEvent {
  id: number;
  occurredAt: string;
  userId: string;
  clientId: string;
  authMethod: AuthMethod;
  mfaMethod: 'totp' | 'sms' | 'webauthn' | null;
  externalProvider: string | null; // 'google' | 'azure_ad' | 'saml:idp_x' | null
  ipAddress: string | null;
  userAgent: string | null;
  success: boolean;
  metadata: Record<string, unknown> | null;
}

interface LoginState {
  userId: string;
  lastLoginAt: string | null;
  lastClientId: string | null;
  lastAuthMethod: AuthMethod | null;
  lastIp: string | null;
  lastUserAgent: string | null;
  updatedAt: string;
}

interface LoginActivitySDK {
  // chamada síncrona leve — upsert de 1 linha por PK
  recordState(entry: Omit<LoginState, 'updatedAt'>): Promise<void>;

  // chamada assíncrona — vai para fila/outbox, não bloqueia o fluxo de login
  recordEvent(entry: Omit<LoginEvent, 'id' | 'occurredAt'>): Promise<void>;
}
```

#### Camada 3 — Agregado diário (`login_stats_daily`)

Para perguntas de tendência ("uso de MFA por client ao longo do tempo", "métodos de login mais usados por usuário") você não precisa do evento individual depois de alguns dias — um rollup diário já responde, e cresce muito mais devagar que o evento bruto.

```sql
CREATE TABLE login_stats_daily (
    usage_date      DATE NOT NULL,
    user_id         UUID NOT NULL,
    client_id       UUID NOT NULL,
    auth_method     VARCHAR(30) NOT NULL,

    login_count     INT NOT NULL DEFAULT 1,
    success_count   INT NOT NULL DEFAULT 0,
    failure_count   INT NOT NULL DEFAULT 0,
    last_login_at   TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (usage_date, user_id, client_id, auth_method)
);
```

**Job de agregação (roda diariamente, lê a partição do dia anterior):**

```sql
INSERT INTO login_stats_daily (usage_date, user_id, client_id, auth_method, login_count, success_count, failure_count, last_login_at)
SELECT
    occurred_at::date                          AS usage_date,
    user_id,
    client_id,
    auth_method,
    COUNT(*)                                    AS login_count,
    COUNT(*) FILTER (WHERE success)             AS success_count,
    COUNT(*) FILTER (WHERE NOT success)         AS failure_count,
    MAX(occurred_at)                            AS last_login_at
FROM login_events
WHERE occurred_at >= (now() - interval '1 day')::date
  AND occurred_at <  now()::date
GROUP BY 1, 2, 3, 4
ON CONFLICT (usage_date, user_id, client_id, auth_method) DO UPDATE SET
    login_count   = EXCLUDED.login_count,
    success_count = EXCLUDED.success_count,
    failure_count = EXCLUDED.failure_count,
    last_login_at = EXCLUDED.last_login_at;
```

Rodar esse job **antes** da partição do dia ser desanexada/derrubada pelo `pg_partman` (agendar o job de agregação algumas horas antes da rotina de retenção, ou usar `retention_keep_table = true` como margem de segurança).

TypeScript equivalente:

```typescript
interface LoginStatsDaily {
  usageDate: string; // YYYY-MM-DD
  userId: string;
  clientId: string;
  authMethod: AuthMethod;
  loginCount: number;
  successCount: number;
  failureCount: number;
  lastLoginAt: string;
}

interface LoginStatsQueryAPI {
  getMethodTrend(
    userId: string,
    range: { from: string; to: string }
  ): Promise<Array<{ date: string; authMethod: AuthMethod; count: number }>>;

  getMfaAdoptionByClient(
    clientId: string,
    range: { from: string; to: string }
  ): Promise<Array<{ date: string; mfaLogins: number; nonMfaLogins: number }>>;
}
```

#### Resumo das 3 camadas

| Camada | Tabela | Volume | Escrita | Retenção |
|---|---|---|---|---|
| Estado atual | `user_login_state` | 1 linha por usuário | Síncrona (upsert leve) | Permanente |
| Evento bruto | `login_events` (particionada) | Alto | Assíncrona (fila/fire-and-forget) | Curta (7–30 dias via `pg_partman`) |
| Agregado diário | `login_stats_daily` | Baixo, cresce devagar | Job noturno em batch | Longa |

**Sobre fire-and-forget para a camada de evento bruto:** aqui o trade-off é aceitável — diferente do `audit_events` administrativo, a perda ocasional de um evento de login individual não compromete investigação (a camada de estado e o agregado diário continuam consistentes) e o volume não justifica a complexidade extra de outbox+CDC. Uma fila simples (SQS/RabbitMQ) como buffer já reduz bastante o risco de perda em relação a um `.catch()` que só loga.

#### Quando isso deixa de ser suficiente

Se o volume crescer a ponto de particionamento semanal não bastar (cenário de milhões de eventos por hora), o próximo passo natural é migrar `login_events` para **Timescale** (extensão do próprio Postgres — migração incremental, menos disruptiva) ou **ClickHouse** (se o volume justificar um motor colunar dedicado). Isso não deveria ser decisão de dia 1 — a arquitetura de 3 camadas em Postgres puro com `pg_partman` atende a maioria dos IDPs até uma escala considerável.

#### 4.2.1 Reter o evento bruto a longo prazo, mesmo particionado? Não — mas a capacidade de investigação não deve se perder

Uma dúvida natural nesse ponto: já que o evento está particionado, não seria mais simples só aumentar a retenção da partição em vez de derrubar?

A resposta consolidada entre os provedores de IDP (ver seção de referências) é **não manter o evento bruto por longo prazo dentro do banco operacional, independente de estar particionado**, por dois motivos:

1. **Custo/benefício cai rápido depois de uma janela curta.** Na prática, a esmagadora maioria das consultas sobre eventos brutos de login acontece nos primeiros dias após o evento (investigação de incidente recente). Depois disso, o uso real é agregado ("tendência de uso de MFA") — que já é coberto pela camada 3 (`login_stats_daily`) — ou forense pontual e raro ("quem acessou X há 8 meses"), que não justifica manter todo o volume bruto indexado e sob vacuum permanente em Postgres.

2. **A pergunta de segurança ("quem acessou o quê, por onde, quando") não desaparece — ela migra de lugar.** O padrão da indústria não é "guardar tudo para sempre na mesma tabela transacional", é: reter bruto por uma janela curta/média no sistema operacional, e **exportar automaticamente** para um destino especializado (data lake em Parquet + engine de query analítica, ou um SIEM) antes do drop. Isso é literalmente o que Auth0 (Log Streaming), Okta (System Log + Log Streaming) e a própria AWS (S3 + Parquet + Athena para logs particionados) fazem — ver seção 7.

Ou seja: a resposta à sua pergunta de segurança ("quem, o quê, por onde, quando") continua existindo depois dos 30 dias — só que ela passa a ser respondida por uma camada diferente, otimizada para esse tipo de consulta (scan analítico sobre grande volume histórico), em vez de pela tabela transacional que serve o app no dia a dia.

#### 4.2.2 Export automatizado antes do drop (nível "morno/frio")

Mecanismo concreto, acoplado à rotina de retenção do `pg_partman`, usando `retention_keep_table = true` como margem de segurança (a partição é desanexada, não apagada, dando a janela para o export):

```sql
-- pg_partman já configurado para desanexar (não dropar direto) partições vencidas
UPDATE partman.part_config
SET retention = '30 days',
    retention_keep_table = true  -- desanexa; export job cuida do drop depois
WHERE parent_table = 'public.login_events';
```

Job de export (roda diariamente, depois do `pg_partman` rodar sua manutenção, antes da rotina de limpeza final):

```sql
-- 1. Encontrar partições desanexadas prontas para export
--    (pg_partman renomeia partições desanexadas com sufixo, ex: login_events_p2026_07_20)
SELECT schemaname, tablename
FROM pg_tables
WHERE tablename LIKE 'login_events_p%'
  AND tablename NOT IN (SELECT child_table FROM partman.show_partitions('public.login_events'));
```

```bash
# 2. Export via COPY para Parquet (usando extensão pg_parquet, ou COPY para CSV
#    intermediário + conversão com duckdb/pyarrow — ambas abordagens comuns)
psql -c "COPY login_events_p2026_07_20 TO STDOUT WITH (FORMAT csv, HEADER true)" \
  | duckdb -c "COPY (SELECT * FROM read_csv('/dev/stdin', AUTO_DETECT=true)) \
               TO 's3://idp-audit-archive/login_events/year=2026/month=07/day=20/part.parquet' \
               (FORMAT parquet)"

# 3. Só após confirmação de sucesso do upload, dropar a partição desanexada
psql -c "DROP TABLE login_events_p2026_07_20;"
```

**Estrutura de partição no S3 (Hive-style, `year=/month=/day=`)** é o padrão recomendado pela AWS para permitir partition pruning eficiente no Athena/Glue — evita que toda query analítica precise escanear o histórico inteiro. Parquet é preferido a CSV/JSON no destino frio pelo mesmo motivo: formato colunar permite ao engine de query pular colunas e partições irrelevantes, reduzindo custo e tempo de scan.

TypeScript equivalente do contrato do job:

```typescript
interface ArchiveExportJob {
  /**
   * Localiza partições de login_events desanexadas (retention_keep_table=true)
   * e ainda não exportadas, exporta para S3 em Parquet particionado por data,
   * e só então autoriza o drop da tabela desanexada.
   */
  exportDetachedPartitions(): Promise<{
    exported: Array<{ partitionTable: string; s3Path: string; rowCount: number }>;
    droppedAfterConfirmation: string[];
  }>;
}
```

#### 4.2.3 Padrão de 3 níveis (resumo)

| Nível | Onde | Retenção | Para quê |
|---|---|---|---|
| Quente | `login_events` particionado (Postgres) | 7–30 dias | Investigação imediata, o app consulta direto |
| Morno/frio | S3 (Parquet, particionado por data) + Athena/Glue, ou SIEM | 1–7 anos (conforme necessidade/compliance) | "Quem acessou o quê, por onde, quando" — investigação forense retroativa |
| Agregado | `login_stats_daily` (Postgres) | Indefinida | Tendência, dashboards de produto — não é a camada de segurança |

## 5. Estratégia de integração gradual

1. Criar o SDK (`AuditSDK` / `LoginActivitySDK`) como um pacote isolado.
   - `AuditSDK.record()` grava no outbox local (transacional).
   - `LoginActivitySDK.recordState()` é upsert síncrono leve.
   - `LoginActivitySDK.recordEvent()` vai para fila assíncrona.
2. Priorizar os fluxos de maior valor primeiro: criação/alteração de client, criação de usuário administrativa, MFA e reset de senha forçado (audit trail) — esses têm baixo volume e alto valor de compliance.
3. Login entra em duas etapas: primeiro `recordState` (simples, baixo risco, alto valor imediato — já responde "último login"), depois `recordEvent` (histórico detalhado) e por último o job de agregação diária.
4. Configurar `pg_partman` com política de retenção antes de habilitar o volume completo de `recordEvent`, não depois.
5. Só então construir a API de consulta, já com os três domínios (audit, estado, agregado) populados.

## 6. Retenção sugerida

| Tabela | Retenção banco "quente" | Destino após |
|---|---|---|
| `audit_events` | Indefinida (baixo volume, geralmente < alguns GB/ano) | — |
| `user_login_state` | Permanente (1 linha por usuário) | — |
| `login_events` | 7–30 dias em partições (`pg_partman`) | Export automatizado para S3 (Parquet particionado) antes do drop — ver seção 4.2.2. Retenção fria: 1–7 anos conforme necessidade |
| `login_stats_daily` | Longa (cresce devagar, agregado) | — |

## 7. Referências

Padrões e decisões deste documento foram baseados em como provedores de identidade e arquiteturas de referência tratam publicamente auditoria e logs operacionais:

- **Auth0 — Logs & Log Streaming.** Confirma o padrão de streaming de eventos de auditoria/login para destinos externos (SIEM, Datadog, Elastic Security) em vez de retenção ilimitada no próprio sistema; retenção do log no Auth0 varia por plano de assinatura.
  https://auth0.com/docs/deploy-monitor/logs
  https://auth0.com/docs/logs/export-log-events-with-rules

- **Auth0 — Datadog integration (log streaming).** Exemplo concreto de export de log de autenticação em tempo real para uma ferramenta de análise externa.
  https://docs.datadoghq.com/integrations/auth0/

- **Okta — System Log & Customer Data Retention Policy.** Confirma retenção quente padrão de 90 dias no próprio produto, com recomendação explícita de export via API/Log Streaming para SIEM (Splunk, AWS) caso seja necessária retenção maior — é a referência mais direta para a decisão de "não reter bruto indefinidamente, exportar para fora".
  https://support.okta.com/help/s/article/Customer-Data-Retention-Policy?language=en_US
  https://support.okta.com/help/Documentation/Knowledge_Article/Exporting-Okta-Log-Data
  https://developer.okta.com/docs/reference/system-log-query/

- **AWS — Partitioning e Parquet para logs em S3/Athena.** Base para a estrutura de partição Hive-style (`year=/month=/day=`) e a escolha de Parquet como formato de destino frio, reduzindo custo e tempo de scan analítico.
  https://repost.aws/questions/QUjQHGAO_nQd-PsIKsvfH3HA/partitioning-s3-access-logs-to-optimize-athena-queries
  https://aws.amazon.com/blogs/apn/empirical-approach-to-improving-performance-and-reducing-costs-with-amazon-athena/

- **`pg_partman` (extensão PostgreSQL).** Base para a automação de criação/desanexação/drop de partições e política de retenção usada nas seções 4.2 e 4.2.2, evitando gestão manual de partições em produção.
  https://github.com/pgpartman/pg_partman

Padrões gerais de arquitetura (outbox pattern, separação de audit trail vs. activity log, event sourcing simplificado com diff em JSONB) refletem práticas amplamente documentadas na comunidade de engenharia de software para sistemas distribuídos e não foram atribuídos a uma fonte única — são consideradas conhecimento consolidado de arquitetura de sistemas (ex.: microservices.io para outbox pattern, martinfowler.com para event sourcing).
