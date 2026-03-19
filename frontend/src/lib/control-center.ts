"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import { customFetch } from "@/api/mutator";

export type ControlModuleCategory =
  | "finance"
  | "network_marketing"
  | "newsletters"
  | "podcasts"
  | "paperclip"
  | "custom";

export type FinanceKind =
  | "stock"
  | "property"
  | "email"
  | "budget_txn"
  | "budget_statement"
  | "budget_rule";
export type NetworkKind = "conversation" | "client" | "team_member" | "huddle_play" | "cold_contact";
export type NetworkMarketingViewMode = "pipeline" | "team_tree" | "cold_contact";

export type ControlModule = {
  id: string;
  slug: string;
  title: string;
  description: string;
  category: ControlModuleCategory;
  enabled: boolean;
  order: number;
};

export type FinanceRecord = {
  id: string;
  kind: FinanceKind;
  title: string;
  summary: string;
  stage: string;
  amount: number | null;
  transactionDate?: string;
  category?: string;
  subcategory?: string;
  statementId?: string;
  uncategorized?: boolean;
  linkedTaskId?: string | null;
  updatedAt: string;
};

export type NetworkRecord = {
  id: string;
  kind: NetworkKind;
  title: string;
  summary: string;
  stage: string;
  nextStep: string;
  personName?: string;
  huddlePlay?: string;
  huddleDay?: string;
  coldContactScore?: number;
  coldContactPlatform?: string;
  coldContactProfileUrl?: string;
  coldContactWhyFit?: string;
  coldContactWhyNow?: string;
  coldContactSignals?: string[];
  coldContactConfidence?: string;
  coldContactAngle?: string;
  coldContactResearch?: string;
  followUpDate?: string;
  followUpCompleted?: boolean;
  followUpCompletedAt?: string;
  parentMemberId?: string | null;
  displayName?: string;
  statusSummary?: string;
  memberFolderPath?: string;
  createdAt?: string;
  linkedTaskId?: string | null;
  updatedAt: string;
  [key: string]: unknown;
};

export type NewsletterRecord = {
  id: string;
  source: string;
  title: string;
  summary: string;
  action: string;
  priority: "low" | "medium" | "high";
  linkedTaskId?: string | null;
  updatedAt: string;
};

export type PodcastRecord = {
  id: string;
  source_filename: string;
  source_format: string;
  ingest_status: "uploaded" | "pending" | "processing" | "completed" | "failed";
  transcript_status: "uploaded" | "pending" | "processing" | "completed" | "failed";
  summary_status: "uploaded" | "pending" | "processing" | "completed" | "failed";
  task_extraction_status: "uploaded" | "pending" | "processing" | "completed" | "failed";
  category: string;
  transcript_path: string;
  summary_path: string;
  summary_generated_at?: string;
  summary_sections?: string[];
  extracted_actions_count: number;
  title: string;
  summary: string;
  stage: string;
  linkedTaskId?: string | null;
  updatedAt: string;
};

export type CustomRecord = {
  id: string;
  title: string;
  summary: string;
  stage: string;
  data?: Record<string, unknown>;
  linkedTaskId?: string | null;
  updatedAt: string;
  [key: string]: unknown;
};

export type ControlRecordsState = {
  finance: FinanceRecord[];
  network_marketing: NetworkRecord[];
  newsletters: NewsletterRecord[];
  podcasts: PodcastRecord[];
  custom: Record<string, CustomRecord[]>;
};

export type ControlCenterState = {
  modules: ControlModule[];
  records: ControlRecordsState;
  version: number;
  networkMarketingViewMode: NetworkMarketingViewMode;
};

export type PodcastSummaryResult = {
  record_id: string;
  summary_status: "processing" | "completed" | "failed";
  summary_path?: string | null;
  error?: string | null;
};

export type PodcastTranscriptionResult = {
  record_id: string;
  transcript_status: "uploaded" | "processing" | "completed" | "failed";
  audio_path: string;
  transcript_path?: string | null;
  error?: string | null;
};

export type PodcastIngestResult = {
  record: BackendRecord;
};

export type BudgetImportResult = {
  statement_id: string;
  source_filename: string;
  imported_count: number;
  categorized_count: number;
  uncategorized_count: number;
};

export type EventScanResponse = {
  imported: number;
  created: number;
  skipped: number;
  week_start?: string;
  week_end?: string;
  imported_count?: number;
  skipped_duplicates?: number;
  message?: string;
  events?: Array<Record<string, unknown>>;
  [key: string]: unknown;
};

export type ColdContactPipelineRunResult = {
  success: boolean;
  status: string;
  processed: number;
  imported: number;
  total_candidates?: number;
  exported_records?: number;
  imported_records?: number;
  errors?: string[];
  [key: string]: unknown;
};

export type ProspectQueueResult = {
  items: Array<{
    prospect_id: string;
    name: string;
    band: string;
    score_total: number;
    platform: string;
    stage: string;
    [key: string]: unknown;
  }>;
  total: number;
  [key: string]: unknown;
};

export type FollowUpTaskResult = {
  items: Array<{
    task_id: string;
    prospect_name?: string;
    task_type?: string;
    stage?: string;
    recommendation?: string;
    [key: string]: unknown;
  }>;
  total: number;
  [key: string]: unknown;
};

export type FollowUpRecomputeResult = {
  created: number;
  updated: number;
  skipped: number;
  [key: string]: unknown;
};

export type PodcastDriveSyncResult = {
  scanned: number;
  synced: number;
  imported: number;
  skipped: number;
  failed: number;
  [key: string]: unknown;
};

export type PodcastRecordView = {
  record_id: string;
  title?: string;
  summary?: string;
  category?: string;
  transcript_path?: string;
  summary_path?: string;
  transcript_text?: string;
  transcript_words?: Array<{ text?: unknown; start?: unknown; end?: unknown }>;
  transcript_vtt_text?: string;
  summary_text?: string;
  action_points?: string[];
  key_points?: string[];
  decisions?: string[];
  risks?: string[];
  [key: string]: unknown;
};

type BackendRecord = {
  id: string;
  module_id: string;
  module_slug: string;
  module_category: ControlModuleCategory;
  title: string;
  summary?: string | null;
  stage?: string | null;
  data?: Record<string, unknown>;
  linked_task_id?: string | null;
  updated_at: string;
};

export type ControlRecordInput =
  | {
      module: ControlModule;
      kind: "finance";
      title: string;
      summary: string;
      stage: string;
      data: {
        kind: FinanceKind;
        amount?: number | null;
        category?: string;
        subcategory?: string;
        uncategorized?: boolean;
        keyword?: string;
      };
    }
  | {
      module: ControlModule;
      kind: "network_marketing";
      title: string;
      summary: string;
      stage: string;
      data: {
        kind: NetworkKind;
        nextStep: string;
        parent_member_id?: string | null;
        direct_to_me?: boolean;
        display_name?: string;
        status_summary?: string;
        created_at?: string;
        [key: string]: unknown;
      };
    }
  | {
      module: ControlModule;
      kind: "newsletters";
      title: string;
      summary: string;
      stage: string;
      data: {
        source: string;
        action: string;
        priority: NewsletterRecord["priority"];
      };
    }
  | {
      module: ControlModule;
      kind: "podcasts";
      title: string;
      summary: string;
      stage: string;
      data: {
        source_filename: string;
        source_format: string;
        ingest_status:
          | "uploaded"
          | "pending"
          | "processing"
          | "completed"
          | "failed";
        transcript_status:
          | "uploaded"
          | "pending"
          | "processing"
          | "completed"
          | "failed";
        summary_status:
          | "uploaded"
          | "pending"
          | "processing"
          | "completed"
          | "failed";
        task_extraction_status:
          | "uploaded"
          | "pending"
          | "processing"
          | "completed"
          | "failed";
        category: string;
        transcript_path: string;
        summary_path: string;
        extracted_actions_count: number;
      };
    }
  | {
      module: ControlModule;
      kind: "custom";
      title: string;
      summary: string;
      stage: string;
      data?: Record<string, unknown>;
    };

const STORAGE_KEY = "openclaw-control-center-v1";
const VERSION = 1;

const networkMarketingStages = [
  "contact_made",
  "made_aware",
  "door_opened",
  "meet_and_greet",
  "meet_and_greet_2",
  "follow_up",
  "follow_up_2",
  "financial_blueprint",
  "offer_call",
  "launched",
] as const;

type NetworkMarketingStage = (typeof networkMarketingStages)[number];

function normalizeNetworkMarketingStage(stage: string | null | undefined): NetworkMarketingStage {
  const candidate = (stage ?? "").trim().toLowerCase().replace(/[\s-]+/g, "_");
  if ((networkMarketingStages as readonly string[]).includes(candidate)) {
    return candidate as NetworkMarketingStage;
  }

  const legacyMap: Record<string, NetworkMarketingStage> = {
    inbox: "contact_made",
    new_lead: "contact_made",
    contacted: "contact_made",
    call_booked: "meet_and_greet",
    proposal: "financial_blueprint",
    won: "launched",
    lost: "follow_up_2",
  };

  return legacyMap[candidate] ?? "contact_made";
}

const DEFAULT_MODULES: ControlModule[] = [
  {
    id: "finance",
    slug: "budget",
    title: "Budget",
    description: "Upload bank statements, categorize expenses, and track budget history.",
    category: "finance",
    enabled: true,
    order: 1,
  },
  {
    id: "network_marketing",
    slug: "network-marketing",
    title: "Network Marketing",
    description: "Conversations, clients, and team operations.",
    category: "network_marketing",
    enabled: true,
    order: 2,
  },
  {
    id: "newsletters",
    slug: "newsletters",
    title: "Newsletters",
    description: "News digests, key updates, and follow-up actions.",
    category: "newsletters",
    enabled: true,
    order: 3,
  },
  {
    id: "podcasts",
    slug: "podcasts",
    title: "Podcasts",
    description: "Audio ingest, transcription, summarization, and action extraction jobs.",
    category: "podcasts",
    enabled: true,
    order: 4,
  },
  {
    id: "events",
    slug: "events",
    title: "Events",
    description: "Track, review, and plan upcoming events.",
    category: "custom",
    enabled: true,
    order: 5,
  },
];

const DEFAULT_STATE: ControlCenterState = {
  version: VERSION,
  modules: DEFAULT_MODULES,
  networkMarketingViewMode: "pipeline",
  records: {
    finance: [],
    network_marketing: [],
    newsletters: [],
    podcasts: [],
    custom: {},
  },
};

function parseState(raw: string | null): ControlCenterState {
  if (!raw) return DEFAULT_STATE;
  try {
    const parsed = JSON.parse(raw) as Partial<ControlCenterState>;
    if (!parsed || !Array.isArray(parsed.modules) || !parsed.records) {
      return DEFAULT_STATE;
    }
    return {
      version: VERSION,
      modules: ensureSystemModules(parsed.modules as ControlModule[]),
      networkMarketingViewMode:
        parsed.networkMarketingViewMode === "team_tree" ? "team_tree" : "pipeline",
      records: {
        finance: Array.isArray(parsed.records.finance)
          ? (parsed.records.finance as FinanceRecord[])
          : [],
        network_marketing: Array.isArray(parsed.records.network_marketing)
          ? (parsed.records.network_marketing as NetworkRecord[])
          : [],
        newsletters: Array.isArray(parsed.records.newsletters)
          ? (parsed.records.newsletters as NewsletterRecord[])
          : [],
        podcasts: Array.isArray(parsed.records.podcasts)
          ? (parsed.records.podcasts as PodcastRecord[])
          : [],
        custom:
          parsed.records.custom && typeof parsed.records.custom === "object"
            ? (parsed.records.custom as Record<string, CustomRecord[]>)
            : {},
      },
    };
  } catch {
    return DEFAULT_STATE;
  }
}

function normalizeModule(module: ControlModule): ControlModule {
  if (module.id === "finance") {
    return {
      ...module,
      slug: "budget",
      title: "Budget",
      description: "Upload bank statements, categorize expenses, and track budget history.",
    };
  }
  if (module.id === "events") {
    return {
      ...module,
      slug: "events",
      title: "Events",
      description: "Track, review, and plan upcoming events.",
      category: "custom",
    };
  }
  return module;
}

function ensureSystemModules(modules: ControlModule[]): ControlModule[] {
  const normalized = modules.map(normalizeModule);
  const presentIds = new Set(normalized.map((module) => module.id));
  const appended = [...normalized];
  let nextOrder =
    normalized.reduce(
      (max, module) => Math.max(max, Number.isFinite(module.order) ? module.order : 0),
      0,
    ) + 1;

  for (const defaultModule of DEFAULT_MODULES.map(normalizeModule)) {
    if (presentIds.has(defaultModule.id)) continue;
    appended.push({ ...defaultModule, order: nextOrder++ });
  }

  return appended
    .sort((a, b) => a.order - b.order)
    .map((module, index) => ({ ...module, order: index + 1 }));
}

function toStateFromBackend(
  modules: ControlModule[],
  records: BackendRecord[],
  networkMarketingViewMode: NetworkMarketingViewMode,
): ControlCenterState {
  const state: ControlCenterState = {
    version: VERSION,
    modules: ensureSystemModules(modules),
    networkMarketingViewMode,
    records: {
      finance: [],
      network_marketing: [],
      newsletters: [],
      podcasts: [],
      custom: {},
    },
  };

  for (const item of records) {
    const data = item.data ?? {};
    if (item.module_category === "finance") {
      const parsedKind = String(data.kind ?? "stock");
      state.records.finance.push({
        id: item.id,
        kind: (parsedKind as FinanceKind) ?? "stock",
        title: item.title,
        summary: item.summary ?? "",
        stage: item.stage ?? "inbox",
        amount:
          typeof data.amount === "number" && Number.isFinite(data.amount)
            ? data.amount
            : null,
        transactionDate:
          typeof data.transaction_date === "string" ? data.transaction_date : undefined,
        category: typeof data.category === "string" ? data.category : undefined,
        subcategory: typeof data.subcategory === "string" ? data.subcategory : undefined,
        statementId: typeof data.statement_id === "string" ? data.statement_id : undefined,
        uncategorized: Boolean(data.uncategorized),
        linkedTaskId: item.linked_task_id ?? null,
        updatedAt: item.updated_at,
      });
      continue;
    }
    if (item.module_category === "network_marketing") {
      state.records.network_marketing.push({
        id: item.id,
        kind: (data.kind as NetworkKind) ?? "conversation",
        title: item.title,
        summary: item.summary ?? "",
        stage: normalizeNetworkMarketingStage(item.stage),
        nextStep:
          typeof data.nextStep === "string"
            ? data.nextStep
            : typeof data.next_step === "string"
              ? data.next_step
              : "",
        personName: typeof data.person_name === "string" ? data.person_name : undefined,
        huddlePlay: typeof data.huddle_play === "string" ? data.huddle_play : undefined,
        huddleDay:
          typeof data.huddle_day === "string"
            ? data.huddle_day
            : typeof data.logged_on_date === "string"
              ? data.logged_on_date
              : undefined,
        parentMemberId:
          (() => {
            const rawParentId =
              typeof data.parent_member_id === "string"
                ? data.parent_member_id
                : typeof data.reports_to_member_id === "string"
                  ? data.reports_to_member_id
                  : data.direct_to_me === true
                    ? null
                    : data.parent_member_id === null || data.reports_to_member_id === null
                      ? null
                      : undefined;
            if (rawParentId === item.id) return null;
            return rawParentId;
          })(),
        displayName:
          typeof data.display_name === "string"
            ? data.display_name
            : typeof data.member_display_name === "string"
              ? data.member_display_name
              : undefined,
        statusSummary:
          typeof data.status_summary === "string"
            ? data.status_summary
            : typeof data.lifecycle_status === "string"
              ? data.lifecycle_status
              : undefined,
        memberFolderPath:
          typeof data.member_folder_path === "string"
            ? data.member_folder_path
            : undefined,
        createdAt:
          typeof data.created_at === "string"
            ? data.created_at
            : typeof data.createdAt === "string"
              ? data.createdAt
              : undefined,
        linkedTaskId: item.linked_task_id ?? null,
        updatedAt: item.updated_at,
      });
      continue;
    }
    if (item.module_category === "newsletters") {
      state.records.newsletters.push({
        id: item.id,
        source: String(data.source ?? ""),
        title: item.title,
        summary: item.summary ?? "",
        action: String(data.action ?? ""),
        priority: (data.priority as NewsletterRecord["priority"]) ?? "medium",
        linkedTaskId: item.linked_task_id ?? null,
        updatedAt: item.updated_at,
      });
      continue;
    }
    if (item.module_category === "podcasts") {
      state.records.podcasts.push({
        id: item.id,
        source_filename: String(data.source_filename ?? ""),
        source_format: String(data.source_format ?? ""),
        ingest_status:
          (data.ingest_status as PodcastRecord["ingest_status"]) ?? "pending",
        transcript_status:
          (data.transcript_status as PodcastRecord["transcript_status"]) ?? "pending",
        summary_status:
          (data.summary_status as PodcastRecord["summary_status"]) ?? "pending",
        task_extraction_status:
          (data.task_extraction_status as PodcastRecord["task_extraction_status"]) ??
          "pending",
        category: String(data.category ?? ""),
        transcript_path: String(data.transcript_path ?? ""),
        summary_path: String(data.summary_path ?? ""),
        summary_generated_at:
          typeof data.summary_generated_at === "string"
            ? data.summary_generated_at
            : undefined,
        summary_sections:
          data.summary_format &&
          typeof data.summary_format === "object" &&
          Array.isArray((data.summary_format as { sections?: unknown[] }).sections)
            ? ((data.summary_format as { sections: unknown[] }).sections.filter(
                (item): item is string => typeof item === "string",
              ) as string[])
            : undefined,
        extracted_actions_count:
          typeof data.extracted_actions_count === "number" &&
          Number.isFinite(data.extracted_actions_count)
            ? data.extracted_actions_count
            : 0,
        title: item.title,
        summary: item.summary ?? "",
        stage: item.stage ?? "inbox",
        linkedTaskId: item.linked_task_id ?? null,
        updatedAt: item.updated_at,
      });
      continue;
    }
    const customList = state.records.custom[item.module_id] ?? [];
    customList.push({
      id: item.id,
      title: item.title,
      summary: item.summary ?? "",
      stage: item.stage ?? "inbox",
      linkedTaskId: item.linked_task_id ?? null,
      updatedAt: item.updated_at,
    });
    state.records.custom[item.module_id] = customList;
  }

  return state;
}

function removeRecordById(state: ControlCenterState, recordId: string): ControlCenterState {
  return {
    ...state,
    records: {
      finance: state.records.finance.filter((record) => record.id !== recordId),
      network_marketing: state.records.network_marketing.filter(
        (record) => record.id !== recordId,
      ),
      newsletters: state.records.newsletters.filter((record) => record.id !== recordId),
      podcasts: state.records.podcasts.filter((record) => record.id !== recordId),
      custom: Object.fromEntries(
        Object.entries(state.records.custom).map(([moduleId, records]) => [
          moduleId,
          records.filter((record) => record.id !== recordId),
        ]),
      ),
    },
  };
}

function updateRecordStageById(
  state: ControlCenterState,
  recordId: string,
  stage: string,
): ControlCenterState {
  const nextStage = stage.trim() || "inbox";
  return {
    ...state,
    records: {
      finance: state.records.finance.map((record) =>
        record.id === recordId ? { ...record, stage: nextStage } : record,
      ),
      network_marketing: state.records.network_marketing.map((record) =>
        record.id === recordId ? { ...record, stage: nextStage } : record,
      ),
      newsletters: state.records.newsletters,
      podcasts: state.records.podcasts.map((record) =>
        record.id === recordId ? { ...record, stage: nextStage } : record,
      ),
      custom: Object.fromEntries(
        Object.entries(state.records.custom).map(([moduleId, records]) => [
          moduleId,
          records.map((record) =>
            record.id === recordId ? { ...record, stage: nextStage } : record,
          ),
        ]),
      ),
    },
  };
}

export function slugifyLabel(label: string): string {
  return label
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
}

export function makeId(prefix: string): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
}

export function useControlCenterState() {
  const [state, setState] = useState<ControlCenterState>(DEFAULT_STATE);
  const [ready, setReady] = useState(false);

  const persistLocal = useCallback((next: ControlCenterState) => {
    setState(next);
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  }, []);

  useEffect(() => {
    const localState = parseState(window.localStorage.getItem(STORAGE_KEY));
    setState(localState);
    const loadRemote = async () => {
      try {
        const [configResponse, recordsResponse] = await Promise.all([
          customFetch<{
            data: {
              version: number;
              modules: ControlModule[];
              network_marketing_view_mode: NetworkMarketingViewMode;
            };
          }>("/api/v1/control-center", {
            method: "GET",
          }),
          customFetch<{ data: { items: BackendRecord[] } }>(
            "/api/v1/control-center/records",
            { method: "GET" },
          ),
        ]);
        const merged = toStateFromBackend(
          ensureSystemModules(configResponse.data.modules),
          recordsResponse.data.items,
          configResponse.data.network_marketing_view_mode === "team_tree"
            ? "team_tree"
            : "pipeline",
        );
        persistLocal(merged);
      } catch {
        setState(localState);
      } finally {
        setReady(true);
      }
    };

    void loadRemote();
  }, [persistLocal]);

  const saveModules = useCallback(
    async (next: ControlCenterState) => {
      const normalizedNext = {
        ...next,
        modules: ensureSystemModules(next.modules),
      };
      persistLocal(normalizedNext);
      try {
        await customFetch<{ data: unknown }>(
          "/api/v1/control-center",
          {
            method: "PUT",
            body: JSON.stringify({
              version: normalizedNext.version,
              modules: normalizedNext.modules,
              network_marketing_view_mode: normalizedNext.networkMarketingViewMode,
            }),
          },
        );
      } catch {
        // Keep local data even when backend is temporarily unavailable.
      }
    },
    [persistLocal],
  );

  const refreshRecords = useCallback(async () => {
    const recordsResponse = await customFetch<{ data: { items: BackendRecord[] } }>(
      "/api/v1/control-center/records",
      { method: "GET" },
    );
    const merged = toStateFromBackend(
      state.modules,
      recordsResponse.data.items,
      state.networkMarketingViewMode,
    );
    persistLocal(merged);
    return merged;
  }, [persistLocal, state.modules, state.networkMarketingViewMode]);

  const addRecord = useCallback(
    async (input: ControlRecordInput): Promise<void> => {
      const response = await customFetch<{ data: BackendRecord }>(
        "/api/v1/control-center/records",
        {
          method: "POST",
          body: JSON.stringify({
            module_id: input.module.id,
            module_slug: input.module.slug,
            module_category: input.module.category,
            title: input.title,
            summary: input.summary,
            stage: input.stage,
            data: input.data ?? {},
          }),
        },
      );
      const added = toStateFromBackend(
        state.modules,
        [response.data],
        state.networkMarketingViewMode,
      ).records;
      const next: ControlCenterState = {
        ...state,
        records: {
          finance: [...added.finance, ...state.records.finance],
          network_marketing: [
            ...added.network_marketing,
            ...state.records.network_marketing,
          ],
          newsletters: [...added.newsletters, ...state.records.newsletters],
          podcasts: [...added.podcasts, ...state.records.podcasts],
          custom: {
            ...state.records.custom,
            ...Object.fromEntries(
              Object.entries(added.custom).map(([moduleId, incoming]) => [
                moduleId,
                [...incoming, ...(state.records.custom[moduleId] ?? [])],
              ]),
            ),
          },
        },
      };
      persistLocal(next);
    },
    [persistLocal, state],
  );

  const deleteRecord = useCallback(
    async (recordId: string): Promise<void> => {
      await customFetch<{ data: unknown }>(`/api/v1/control-center/records/${recordId}`, {
        method: "DELETE",
      });
      persistLocal(removeRecordById(state, recordId));
    },
    [persistLocal, state],
  );

  const promoteRecordToTask = useCallback(
    async (recordId: string, boardId: string, priority = "medium"): Promise<void> => {
      await customFetch<{ data: { task_id: string } }>(
        `/api/v1/control-center/records/${recordId}/promote`,
        {
          method: "POST",
          body: JSON.stringify({
            board_id: boardId,
            priority,
          }),
        },
      );
    },
    [],
  );

  const updateRecordStage = useCallback(
    async (recordId: string, stage: string): Promise<void> => {
      const previous = state;
      const optimistic = updateRecordStageById(state, recordId, stage);
      persistLocal(optimistic);
      try {
        await customFetch<{ data: BackendRecord }>(
          `/api/v1/control-center/records/${recordId}`,
          {
            method: "PATCH",
            body: JSON.stringify({ stage }),
          },
        );
      } catch (error) {
        persistLocal(previous);
        throw error;
      }
    },
    [persistLocal, state],
  );

  const updateRecord = useCallback(
    async (
      recordId: string,
      patch: {
        title?: string;
        summary?: string;
        stage?: string;
        data?: Record<string, unknown>;
      },
    ): Promise<void> => {
      await customFetch<{ data: BackendRecord }>(`/api/v1/control-center/records/${recordId}`, {
        method: "PATCH",
        body: JSON.stringify(patch),
      });
      await refreshRecords();
    },
    [refreshRecords],
  );

  const ingestPodcastAudio = useCallback(
    async (file: File, title?: string, summary?: string): Promise<PodcastIngestResult> => {
      const formData = new FormData();
      formData.append("file", file);
      if (title && title.trim()) formData.append("title", title.trim());
      if (summary && summary.trim()) formData.append("summary", summary.trim());

      const response = await customFetch<{ data: PodcastIngestResult }>(
        "/api/v1/control-center/podcasts/ingest",
        {
          method: "POST",
          body: formData,
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const importBudgetStatement = useCallback(
    async (file: File): Promise<BudgetImportResult> => {
      const formData = new FormData();
      formData.append("file", file);
      const response = await customFetch<{ data: BudgetImportResult }>(
        "/api/v1/control-center/budget/import",
        {
          method: "POST",
          body: formData,
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const transcribePodcastRecord = useCallback(
    async (recordId: string, file: File, note?: string): Promise<PodcastTranscriptionResult> => {
      const formData = new FormData();
      formData.append("file", file);
      if (note && note.trim()) formData.append("note", note.trim());

      const response = await customFetch<{ data: PodcastTranscriptionResult }>(
        `/api/v1/control-center/records/${recordId}/transcribe`,
        {
          method: "POST",
          body: formData,
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const summarizePodcastRecord = useCallback(
    async (recordId: string): Promise<PodcastSummaryResult> => {
      const response = await customFetch<{ data: PodcastSummaryResult }>(
        `/api/v1/control-center/records/${recordId}/summarize`,
        {
          method: "POST",
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const extractPodcastActions = useCallback(async (_recordId: string, _tasksBoardId: string): Promise<void> => {
    return;
  }, []);

  const classifyPodcastRecord = useCallback(async (_recordId: string): Promise<void> => {
    return;
  }, []);

  const runPodcastPipeline = useCallback(async (_recordId: string, _maxRetries?: number): Promise<void> => {
    return;
  }, []);

  const syncPodcastDriveNow = useCallback(async (): Promise<PodcastDriveSyncResult> => {
    return { scanned: 0, synced: 0, imported: 0, skipped: 0, failed: 0 };
  }, []);

  const getPodcastRecordView = useCallback(async (_recordId: string): Promise<PodcastRecordView> => {
    return { record_id: _recordId };
  }, []);

  const getPodcastRecordAudioBlob = useCallback(async (_recordId: string): Promise<Blob> => {
    return new Blob();
  }, []);

  const scanEventsWeek = useCallback(
    async (
      _moduleId: string,
      _moduleSlug: string,
      _moduleTitle: string,
      _sources: string[],
      _weekStart?: string,
    ): Promise<EventScanResponse> => {
      throw new Error("Event source scanning is not configured in this deployment yet.");
    },
    [],
  );

  const addEventRecordToCalendar = useCallback(async (_recordId: string): Promise<unknown> => {
    return { status: "ok" };
  }, []);

  const runColdContactPipeline = useCallback(async (): Promise<ColdContactPipelineRunResult> => {
    return {
      success: false,
      status: "idle",
      processed: 0,
      imported: 0,
      total_candidates: 0,
      exported_records: 0,
      imported_records: 0,
      errors: [],
    };
  }, []);

  const getColdContactQueue = useCallback(
    async (_params?: {
      band?: "cold" | "moderate" | "warm" | "high_priority";
      stage?: string;
      platform?: string;
      source?: string;
      limit?: number;
    }): Promise<ProspectQueueResult> => {
      return { items: [], total: 0 };
    },
    [],
  );

  const getFollowUpTasks = useCallback(
    async (_params?: {
      due_today?: boolean;
      overdue?: boolean;
      stage?: string;
    }): Promise<FollowUpTaskResult> => {
      return { items: [], total: 0 };
    },
    [],
  );

  const recomputeFollowUpTasks = useCallback(async (): Promise<FollowUpRecomputeResult> => {
    return { created: 0, updated: 0, skipped: 0 };
  }, []);

  const setNetworkMarketingViewMode = useCallback(
    async (mode: NetworkMarketingViewMode): Promise<void> => {
      const normalized: NetworkMarketingViewMode = mode === "team_tree" ? "team_tree" : "pipeline";
      const next = {
        ...state,
        networkMarketingViewMode: normalized,
      };
      await saveModules(next);
    },
    [saveModules, state],
  );

  const enabledModules = useMemo(
    () =>
      state.modules
        .filter((module) => module.enabled)
        .sort((a, b) => a.order - b.order),
    [state.modules],
  );

  return {
    ready,
    state,
    setState: saveModules,
    enabledModules,
    addRecord,
    deleteRecord,
    promoteRecordToTask,
    updateRecordStage,
    updateRecord,
    ingestPodcastAudio,
    transcribePodcastRecord,
    summarizePodcastRecord,
    extractPodcastActions,
    classifyPodcastRecord,
    runPodcastPipeline,
    syncPodcastDriveNow,
    getPodcastRecordView,
    getPodcastRecordAudioBlob,
    importBudgetStatement,
    scanEventsWeek,
    addEventRecordToCalendar,
    runColdContactPipeline,
    getColdContactQueue,
    getFollowUpTasks,
    recomputeFollowUpTasks,
    refreshRecords,
    networkMarketingViewMode: state.networkMarketingViewMode,
    setNetworkMarketingViewMode,
  };
}

export function reorderModules(modules: ControlModule[]): ControlModule[] {
  return modules
    .slice()
    .sort((a, b) => a.order - b.order)
    .map((module, index) => ({ ...module, order: index + 1 }));
}
