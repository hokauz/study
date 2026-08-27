# Arquitetura de rastreio de obsolescência (Deprecation Tracking)

## 1. Objetivo

Permitir marcar um recurso (versão de endpoint, propriedade, filtro, provider, etc) como obsoleto/depreciado, e responder de forma barata:

- Quem ainda está consumindo esse recurso
- Se o consumo está caindo ao longo do tempo
- Quando é seguro removê-lo de fato (sunset)

Sem que isso vire uma tabela de log bruto (1 linha por requisição).

## 2. Decisão: banco próprio agregado vs. consultar Datadog sob demanda

Não é uma escolha exclusiva — são responsabilidades diferentes:

| Responsabilidade | Onde deve morar |
|---|---|
| Contagem bruta de chamadas por endpoint/tag | Datadog (APM/Metrics) — ele já faz isso, não reimplementar |
| "Este recurso está deprecado, tem substituto X, sunset em tal data" | Seu domínio de negócio — Datadog não tem esse conceito |
| Agregação diária por (recurso, consumidor) para consulta rápida | Sua tabela — porque correlacionar com status de negócio dentro do Datadog é difícil/caro |
| Histórico de longo prazo (anos) do consumo agregado | Sua tabela — retenção no Datadog é cara e curta |

**Recomendação:** job periódico (hourly ou daily) consulta a Metrics/Logs API do Datadog filtrando por tags relevantes (endpoint, client_id) e faz **upsert** na tabela agregada abaixo. O Datadog é a fonte da contagem bruta; sua tabela é a camada de negócio que sabe o que é obsoleto e serve consultas rápidas via API própria.

Isso evita:
- Duplicar um pipeline de agregação que a ferramenta de APM já resolve.
- Persistir volume alto de eventos crus (seu receio original).
- Perder a correlação com `status` do recurso, que só existe no seu domínio.

## 3. Modelo de dados

### 3.1 Tabela de recursos (dimensão)

```sql
CREATE TABLE resources (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    resource_type     VARCHAR(30) NOT NULL, -- 'endpoint' | 'endpoint_version' | 'property' |
                                             -- 'filter' | 'provider' | 'grant_type'
    identifier        VARCHAR(255) NOT NULL, -- ex: 'GET /v1/users', 'oauth.grant_type.implicit'

    status            VARCHAR(20) NOT NULL DEFAULT 'active',
                      -- 'active' | 'deprecated' | 'sunset_scheduled' | 'removed'

    deprecated_at     TIMESTAMPTZ,
    sunset_at         TIMESTAMPTZ,           -- data planejada de remoção definitiva
    removed_at        TIMESTAMPTZ,

    replacement_id    UUID REFERENCES resources(id), -- aponta para o recurso substituto, se houver
    owner_team        VARCHAR(100),
    notes             TEXT,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (resource_type, identifier)
);
```

### 3.2 Tabela de uso agregado (fato, granularidade diária)

```sql
CREATE TABLE resource_usage_daily (
    resource_id     UUID NOT NULL REFERENCES resources(id),
    usage_date      DATE NOT NULL,

    consumer_type   VARCHAR(20) NOT NULL, -- 'client' | 'user' | 'ip' | 'unknown'
    consumer_id     VARCHAR(255) NOT NULL, -- client_id, user_id, ou hash de IP

    request_count   BIGINT NOT NULL DEFAULT 0,
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (resource_id, usage_date, consumer_type, consumer_id)
);

CREATE INDEX idx_usage_by_date ON resource_usage_daily (usage_date);
CREATE INDEX idx_usage_by_resource_trend ON resource_usage_daily (resource_id, usage_date);
```

**Upsert idempotente do job de sync:**

```sql
INSERT INTO resource_usage_daily (
    resource_id, usage_date, consumer_type, consumer_id,
    request_count, first_seen_at, last_seen_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (resource_id, usage_date, consumer_type, consumer_id)
DO UPDATE SET
    request_count = resource_usage_daily.request_count + EXCLUDED.request_count,
    last_seen_at  = EXCLUDED.last_seen_at,
    updated_at    = now();
```

Isso garante que rodar o job múltiplas vezes no mesmo dia (ex: sync a cada hora) apenas incrementa a contagem existente, sem duplicar linhas — crescimento é `recursos × consumidores × dias`, não `requisições`.

### 3.3 Modelo TypeScript equivalente

```typescript
type ResourceType =
  | 'endpoint'
  | 'endpoint_version'
  | 'property'
  | 'filter'
  | 'provider'
  | 'grant_type';

type ResourceStatus = 'active' | 'deprecated' | 'sunset_scheduled' | 'removed';

interface Resource {
  id: string;
  resourceType: ResourceType;
  identifier: string; // ex: "GET /v1/users", "oauth.grant_type.implicit"
  status: ResourceStatus;
  deprecatedAt: string | null;
  sunsetAt: string | null;
  removedAt: string | null;
  replacementId: string | null;
  ownerTeam: string | null;
  notes: string | null;
  createdAt: string;
  updatedAt: string;
}

type ConsumerType = 'client' | 'user' | 'ip' | 'unknown';

interface ResourceUsageDaily {
  resourceId: string;
  usageDate: string; // YYYY-MM-DD
  consumerType: ConsumerType;
  consumerId: string;
  requestCount: number;
  firstSeenAt: string;
  lastSeenAt: string;
  updatedAt: string;
}

// Contrato do job de sincronização com Datadog
interface DatadogSyncJob {
  /**
   * Consulta a Metrics/Logs API do Datadog filtrando por tags de recurso
   * e faz upsert incremental em resource_usage_daily.
   */
  syncUsage(params: {
    resourceType: ResourceType;
    since: string; // ISO timestamp do último sync bem-sucedido
  }): Promise<{ resourcesUpdated: number; rowsUpserted: number }>;
}

// Contrato de consulta (API própria de deprecação)
interface DeprecationQueryAPI {
  getResourceUsageTrend(
    resourceId: string,
    range: { from: string; to: string }
  ): Promise<Array<{ date: string; totalRequests: number; distinctConsumers: number }>>;

  getActiveConsumers(
    resourceId: string,
    sinceDate: string
  ): Promise<Array<{ consumerId: string; consumerType: ConsumerType; lastSeenAt: string }>>;

  listSunsetCandidates(): Promise<
    Array<{ resource: Resource; last30DaysRequests: number }>
  >;
}
```

## 4. Perguntas que o modelo responde

| Pergunta | Query |
|---|---|
| Quem ainda consome o recurso X? | `SELECT DISTINCT consumer_id, consumer_type FROM resource_usage_daily WHERE resource_id = $1 AND usage_date > now() - interval '30 days'` |
| Houve queda de consumo nos últimos 90 dias? | `SELECT usage_date, SUM(request_count) FROM resource_usage_daily WHERE resource_id = $1 GROUP BY usage_date ORDER BY usage_date` (série temporal, calcular tendência na aplicação) |
| Quais recursos deprecados já têm zero consumo (candidatos a remoção)? | Join `resources` (status = 'deprecated') com `resource_usage_daily` filtrando ausência de linhas recentes |

## 5. Job de sincronização — fluxo sugerido

1. Job roda periodicamente (ex: a cada hora, ou diário se o volume de mudança for baixo).
2. Para cada `resource_type` monitorado, consulta o Datadog (Metrics API para contagem agregada por tag, ou Logs API se precisar de granularidade de consumidor).
3. Agrupa o resultado por `(resource_id, usage_date, consumer_id)` no próprio job.
4. Faz upsert incremental na tabela `resource_usage_daily`.
5. Loga apenas o resumo do sync (quantos recursos atualizados) — não precisa de auditoria própria para isso, é um job idempotente e reprocessável.

## 6. Retenção

Como a granularidade já é diária e agregada, a tabela cresce devagar. É viável reter por anos sem custo relevante — diferente de um log bruto, que precisaria de TTL agressivo.
