"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import { customFetch, customFetchBlob } from "@/api/mutator";

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
export type NetworkKind =
  | "conversation"
  | "client"
  | "team_member"
  | "huddle_play"
  | "cold_contact";
export type NetworkMarketingViewMode =
  | "pipeline"
  | "team_tree"
  | "cold_contact";

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
  linkedMemberId?: string | null;
  linkedMemberName?: string | null;
  displayName?: string;
  statusSummary?: string;
  memberFolderPath?: string;
  movedToTreeAt?: string;
  updateTimeline?: Array<{ at?: string; note?: string }>;
  journeyEntries?: Array<{ at?: string; note?: string }>;
  quitParentMemberId?: string | null;
  quitParentMemberName?: string | null;
  quitAt?: string;
  quitReason?: string;
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
  transcript_path?: string;
  summary_path?: string;
  audio_path?: string;
  action_points?: string[];
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
  startAt?: string;
  endAt?: string;
  venue?: string;
  address?: string;
  city?: string;
  country?: string;
  organizer?: string;
  groupName?: string;
  price?: string;
  currency?: string;
  isFree?: boolean;
  eventUrl?: string;
  sourceUrl?: string;
  sourceName?: string;
  imageUrl?: string;
  eventType?: string;
  status?: string;
  cancelled?: boolean;
  onlineOrHybrid?: string;
  attendeeCount?: number;
  reviewCount?: number;
  ticketUrl?: string;
  timezone?: string;
  journeyEntries?: Array<Record<string, unknown>>;
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
  diagnostics?: Array<{
    source_url: string;
    source_name: string;
    scanned_candidates?: number;
    imported?: number;
    skipped?: number;
    failure_reasons?: Record<string, number>;
  }>;
  skipped_reasons?: Record<string, number>;
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

export type PodcastActionExtractionResult = {
  record_id: string;
  created_task_ids: string[];
  extracted_actions_count: number;
  skipped_duplicates: number;
  action_hashes: string[];
};

export type PodcastClassificationResult = {
  record_id: string;
  category: string;
  audio_path?: string | null;
  transcript_path?: string | null;
  summary_path?: string | null;
};

export type PodcastPipelineRunResult = {
  record_id: string;
  pipeline_status: "pending" | "processing" | "completed" | "failed";
  completed_stages: string[];
  retries: Record<string, number>;
  failed_stage?: string | null;
  max_retries: number;
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
  "dropped_out",
  "quit",
] as const;

type NetworkMarketingStage = (typeof networkMarketingStages)[number];

function normalizeNetworkMarketingStage(
  stage: string | null | undefined,
): NetworkMarketingStage {
  const candidate = (stage ?? "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
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
    lost: "dropped_out",
  };

  return legacyMap[candidate] ?? "contact_made";
}

const DEFAULT_MODULES: ControlModule[] = [
  {
    id: "finance",
    slug: "budget",
    title: "Budget",
    description:
      "Upload bank statements, categorize expenses, and track budget history.",
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
    description:
      "Audio ingest, transcription, summarization, and action extraction jobs.",
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

export function parseState(raw: string | null): ControlCenterState {
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
        parsed.networkMarketingViewMode === "team_tree" ||
        parsed.networkMarketingViewMode === "cold_contact"
          ? parsed.networkMarketingViewMode
          : "pipeline",
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
      description:
        "Upload bank statements, categorize expenses, and track budget history.",
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
  const defaultModules = DEFAULT_MODULES.map(normalizeModule);
  const defaultsBySlug = new Map(
    defaultModules.map((module) => [module.slug, module]),
  );
  const deduped: ControlModule[] = [];
  const presentIds = new Set<string>();
  const presentSlugs = new Set<string>();
  for (const moduleConfig of modules
    .map(normalizeModule)
    .slice()
    .sort((a, b) => a.order - b.order)) {
    const reservedModule = defaultsBySlug.get(moduleConfig.slug);
    const nextModule = reservedModule
      ? {
          ...reservedModule,
          enabled: moduleConfig.enabled,
          order: moduleConfig.order,
        }
      : moduleConfig;
    if (presentIds.has(nextModule.id) || presentSlugs.has(nextModule.slug))
      continue;
    deduped.push(nextModule);
    presentIds.add(nextModule.id);
    presentSlugs.add(nextModule.slug);
  }
  const appended = [...deduped];
  let nextOrder =
    deduped.reduce(
      (max, module) =>
        Math.max(max, Number.isFinite(module.order) ? module.order : 0),
      0,
    ) + 1;

  for (const defaultModule of defaultModules) {
    if (presentIds.has(defaultModule.id)) continue;
    appended.push({ ...defaultModule, order: nextOrder++ });
  }

  return appended
    .sort((a, b) => a.order - b.order)
    .map((module, index) => ({ ...module, order: index + 1 }));
}

function normalizeCustomRecord(
  item: BackendRecord,
  data: Record<string, unknown>,
): CustomRecord {
  const startAt =
    typeof data.startAt === "string"
      ? data.startAt
      : typeof data.start_at === "string"
        ? data.start_at
        : undefined;
  const endAt =
    typeof data.endAt === "string"
      ? data.endAt
      : typeof data.end_at === "string"
        ? data.end_at
        : undefined;
  const eventUrl =
    typeof data.eventUrl === "string"
      ? data.eventUrl
      : typeof data.event_url === "string"
        ? data.event_url
        : undefined;
  const sourceUrl =
    typeof data.sourceUrl === "string"
      ? data.sourceUrl
      : typeof data.source_url === "string"
        ? data.source_url
        : undefined;
  const sourceName =
    typeof data.sourceName === "string"
      ? data.sourceName
      : typeof data.source_name === "string"
        ? data.source_name
        : undefined;
  const venue = typeof data.venue === "string" ? data.venue : undefined;
  const address = typeof data.address === "string" ? data.address : undefined;
  const city = typeof data.city === "string" ? data.city : undefined;
  const country = typeof data.country === "string" ? data.country : undefined;
  const organizer =
    typeof data.organizer === "string" ? data.organizer : undefined;
  const groupName =
    typeof data.groupName === "string"
      ? data.groupName
      : typeof data.group_name === "string"
        ? data.group_name
        : undefined;
  const price = typeof data.price === "string" ? data.price : undefined;
  const currency =
    typeof data.currency === "string" ? data.currency : undefined;
  const imageUrl =
    typeof data.imageUrl === "string"
      ? data.imageUrl
      : typeof data.image_url === "string"
        ? data.image_url
        : undefined;
  const eventType =
    typeof data.eventType === "string"
      ? data.eventType
      : typeof data.event_type === "string"
        ? data.event_type
        : undefined;
  const status = typeof data.status === "string" ? data.status : undefined;
  const cancelled =
    typeof data.cancelled === "boolean" ? data.cancelled : undefined;
  const onlineOrHybrid =
    typeof data.onlineOrHybrid === "string"
      ? data.onlineOrHybrid
      : typeof data.online_or_hybrid === "string"
        ? data.online_or_hybrid
        : undefined;
  const attendeeCount =
    typeof data.attendeeCount === "number" &&
    Number.isFinite(data.attendeeCount)
      ? data.attendeeCount
      : typeof data.attendee_count === "number" &&
          Number.isFinite(data.attendee_count)
        ? data.attendee_count
        : undefined;
  const reviewCount =
    typeof data.reviewCount === "number" && Number.isFinite(data.reviewCount)
      ? data.reviewCount
      : typeof data.review_count === "number" &&
          Number.isFinite(data.review_count)
        ? data.review_count
        : undefined;
  const ticketUrl =
    typeof data.ticketUrl === "string"
      ? data.ticketUrl
      : typeof data.ticket_url === "string"
        ? data.ticket_url
        : undefined;
  const timezone =
    typeof data.timezone === "string" ? data.timezone : undefined;
  const isFree =
    typeof data.isFree === "boolean"
      ? data.isFree
      : typeof data.is_free === "boolean"
        ? data.is_free
        : false;

  return {
    ...(data as Record<string, unknown>),
    id: item.id,
    title: item.title,
    summary: item.summary ?? "",
    stage: item.stage ?? "inbox",
    data,
    startAt,
    endAt,
    venue,
    address,
    city,
    country,
    organizer,
    groupName,
    price,
    currency,
    eventUrl,
    sourceUrl,
    sourceName,
    imageUrl,
    eventType,
    status,
    cancelled,
    onlineOrHybrid,
    attendeeCount,
    reviewCount,
    ticketUrl,
    timezone,
    isFree,
    linkedTaskId: item.linked_task_id ?? null,
    updatedAt: item.updated_at,
  };
}

export function toStateFromBackend(
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
          typeof data.transaction_date === "string"
            ? data.transaction_date
            : undefined,
        category: typeof data.category === "string" ? data.category : undefined,
        subcategory:
          typeof data.subcategory === "string" ? data.subcategory : undefined,
        statementId:
          typeof data.statement_id === "string" ? data.statement_id : undefined,
        uncategorized: Boolean(data.uncategorized),
        linkedTaskId: item.linked_task_id ?? null,
        updatedAt: item.updated_at,
      });
      continue;
    }
    if (item.module_category === "network_marketing") {
      state.records.network_marketing.push({
        ...(data as Record<string, unknown>),
        id: item.id,
        kind: (data.kind as NetworkKind) ?? "conversation",
        title: item.title,
        summary: item.summary ?? "",
        stage: normalizeNetworkMarketingStage(item.stage),
        data,
        nextStep:
          typeof data.nextStep === "string"
            ? data.nextStep
            : typeof data.next_step === "string"
              ? data.next_step
              : "",
        personName:
          typeof data.person_name === "string" ? data.person_name : undefined,
        huddlePlay:
          typeof data.huddle_play === "string" ? data.huddle_play : undefined,
        huddleDay:
          typeof data.huddle_day === "string"
            ? data.huddle_day
            : typeof data.logged_on_date === "string"
              ? data.logged_on_date
              : undefined,
        coldContactScore:
          typeof data.cold_contact_score === "number" &&
          Number.isFinite(data.cold_contact_score)
            ? data.cold_contact_score
            : typeof data.coldContactScore === "number" &&
                Number.isFinite(data.coldContactScore)
              ? data.coldContactScore
              : undefined,
        coldContactPlatform:
          typeof data.cold_contact_platform === "string"
            ? data.cold_contact_platform
            : typeof data.coldContactPlatform === "string"
              ? data.coldContactPlatform
              : undefined,
        coldContactProfileUrl:
          typeof data.cold_contact_profile_url === "string"
            ? data.cold_contact_profile_url
            : typeof data.coldContactProfileUrl === "string"
              ? data.coldContactProfileUrl
              : undefined,
        coldContactWhyFit:
          typeof data.cold_contact_why_fit === "string"
            ? data.cold_contact_why_fit
            : typeof data.coldContactWhyFit === "string"
              ? data.coldContactWhyFit
              : undefined,
        coldContactWhyNow:
          typeof data.cold_contact_why_now === "string"
            ? data.cold_contact_why_now
            : typeof data.coldContactWhyNow === "string"
              ? data.coldContactWhyNow
              : undefined,
        coldContactSignals: Array.isArray(data.cold_contact_signals)
          ? data.cold_contact_signals.filter(
              (item): item is string => typeof item === "string",
            )
          : Array.isArray(data.coldContactSignals)
            ? data.coldContactSignals.filter(
                (item): item is string => typeof item === "string",
              )
            : undefined,
        coldContactConfidence:
          typeof data.cold_contact_confidence === "string"
            ? data.cold_contact_confidence
            : typeof data.coldContactConfidence === "string"
              ? data.coldContactConfidence
              : undefined,
        coldContactAngle:
          typeof data.cold_contact_angle === "string"
            ? data.cold_contact_angle
            : typeof data.coldContactAngle === "string"
              ? data.coldContactAngle
              : undefined,
        coldContactResearch:
          typeof data.cold_contact_research === "string"
            ? data.cold_contact_research
            : typeof data.coldContactResearch === "string"
              ? data.coldContactResearch
              : undefined,
        followUpDate:
          typeof data.follow_up_date === "string"
            ? data.follow_up_date
            : typeof data.followUpDate === "string"
              ? data.followUpDate
              : undefined,
        followUpCompleted:
          typeof data.follow_up_completed === "boolean"
            ? data.follow_up_completed
            : typeof data.followUpCompleted === "boolean"
              ? data.followUpCompleted
              : undefined,
        followUpCompletedAt:
          typeof data.follow_up_completed_at === "string"
            ? data.follow_up_completed_at
            : typeof data.followUpCompletedAt === "string"
              ? data.followUpCompletedAt
              : undefined,
        parentMemberId: (() => {
          const rawParentId =
            typeof data.parent_member_id === "string"
              ? data.parent_member_id
              : typeof data.reports_to_member_id === "string"
                ? data.reports_to_member_id
                : data.direct_to_me === true
                  ? null
                  : data.parent_member_id === null ||
                      data.reports_to_member_id === null
                    ? null
                    : undefined;
          if (rawParentId === item.id) return null;
          return rawParentId;
        })(),
        linkedMemberId:
          typeof data.linked_member_id === "string"
            ? data.linked_member_id
            : typeof data.linkedMemberId === "string"
              ? data.linkedMemberId
              : undefined,
        linkedMemberName:
          typeof data.linked_member_name === "string"
            ? data.linked_member_name
            : typeof data.linkedMemberName === "string"
              ? data.linkedMemberName
              : undefined,
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
        movedToTreeAt:
          typeof data.moved_to_tree_at === "string"
            ? data.moved_to_tree_at
            : typeof data.movedToTreeAt === "string"
              ? data.movedToTreeAt
              : undefined,
        updateTimeline: Array.isArray(data.update_timeline)
          ? data.update_timeline.filter(
              (item): item is { at?: string; note?: string } =>
                Boolean(item) && typeof item === "object",
            )
          : Array.isArray(data.updateTimeline)
            ? data.updateTimeline.filter(
                (item): item is { at?: string; note?: string } =>
                  Boolean(item) && typeof item === "object",
              )
            : undefined,
        journeyEntries: Array.isArray(data.journey_entries)
          ? data.journey_entries.filter(
              (item): item is { at?: string; note?: string } =>
                Boolean(item) && typeof item === "object",
            )
          : Array.isArray(data.journeyEntries)
            ? data.journeyEntries.filter(
                (item): item is { at?: string; note?: string } =>
                  Boolean(item) && typeof item === "object",
              )
            : undefined,
        quitParentMemberId:
          typeof data.quit_parent_member_id === "string"
            ? data.quit_parent_member_id
            : typeof data.quitParentMemberId === "string"
              ? data.quitParentMemberId
              : data.quit_parent_member_id === null ||
                  data.quitParentMemberId === null
                ? null
                : undefined,
        quitParentMemberName:
          typeof data.quit_parent_member_name === "string"
            ? data.quit_parent_member_name
            : typeof data.quitParentMemberName === "string"
              ? data.quitParentMemberName
              : undefined,
        quitAt:
          typeof data.quit_at === "string"
            ? data.quit_at
            : typeof data.quitAt === "string"
              ? data.quitAt
              : undefined,
        quitReason:
          typeof data.quit_reason === "string"
            ? data.quit_reason
            : typeof data.quitReason === "string"
              ? data.quitReason
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
          (data.transcript_status as PodcastRecord["transcript_status"]) ??
          "pending",
        summary_status:
          (data.summary_status as PodcastRecord["summary_status"]) ?? "pending",
        task_extraction_status:
          (data.task_extraction_status as PodcastRecord["task_extraction_status"]) ??
          "pending",
        category: String(data.category ?? ""),
        transcript_path:
          typeof data.transcript_path === "string"
            ? data.transcript_path
            : undefined,
        summary_path:
          typeof data.summary_path === "string" ? data.summary_path : undefined,
        audio_path:
          typeof data.audio_path === "string"
            ? data.audio_path
            : typeof data.source_path === "string"
              ? data.source_path
              : undefined,
        action_points: Array.isArray(data.action_points)
          ? data.action_points.filter(
              (item): item is string => typeof item === "string",
            )
          : undefined,
        summary_generated_at:
          typeof data.summary_generated_at === "string"
            ? data.summary_generated_at
            : undefined,
        summary_sections:
          data.summary_format &&
          typeof data.summary_format === "object" &&
          Array.isArray(
            (data.summary_format as { sections?: unknown[] }).sections,
          )
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
    customList.push(normalizeCustomRecord(item, data));
    state.records.custom[item.module_id] = customList;
  }

  return state;
}

function removeRecordById(
  state: ControlCenterState,
  recordId: string,
): ControlCenterState {
  return {
    ...state,
    records: {
      finance: state.records.finance.filter((record) => record.id !== recordId),
      network_marketing: state.records.network_marketing.filter(
        (record) => record.id !== recordId,
      ),
      newsletters: state.records.newsletters.filter(
        (record) => record.id !== recordId,
      ),
      podcasts: state.records.podcasts.filter(
        (record) => record.id !== recordId,
      ),
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
  if (
    typeof crypto !== "undefined" &&
    typeof crypto.randomUUID === "function"
  ) {
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
          configResponse.data.network_marketing_view_mode,
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
        await customFetch<{ data: unknown }>("/api/v1/control-center", {
          method: "PUT",
          body: JSON.stringify({
            version: normalizedNext.version,
            modules: normalizedNext.modules,
            network_marketing_view_mode:
              normalizedNext.networkMarketingViewMode,
          }),
        });
      } catch {
        // Keep local data even when backend is temporarily unavailable.
      }
    },
    [persistLocal],
  );

  const refreshRecords = useCallback(async () => {
    const recordsResponse = await customFetch<{
      data: { items: BackendRecord[] };
    }>("/api/v1/control-center/records", { method: "GET" });
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
      await customFetch<{ data: unknown }>(
        `/api/v1/control-center/records/${recordId}`,
        {
          method: "DELETE",
        },
      );
      persistLocal(removeRecordById(state, recordId));
    },
    [persistLocal, state],
  );

  const promoteRecordToTask = useCallback(
    async (
      recordId: string,
      boardId: string,
      priority = "medium",
    ): Promise<void> => {
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
      await customFetch<{ data: BackendRecord }>(
        `/api/v1/control-center/records/${recordId}`,
        {
          method: "PATCH",
          body: JSON.stringify(patch),
        },
      );
      await refreshRecords();
    },
    [refreshRecords],
  );

  const ingestPodcastAudio = useCallback(
    async (
      file: File,
      title?: string,
      summary?: string,
    ): Promise<PodcastIngestResult> => {
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
    async (
      recordId: string,
      file: File,
      note?: string,
    ): Promise<PodcastTranscriptionResult> => {
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

  const extractPodcastActions = useCallback(
    async (
      recordId: string,
      tasksBoardId: string,
    ): Promise<PodcastActionExtractionResult> => {
      const response = await customFetch<{
        data: PodcastActionExtractionResult;
      }>(
        `/api/v1/control-center/records/${recordId}/extract-actions?tasks_board_id=${encodeURIComponent(tasksBoardId)}`,
        {
          method: "POST",
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const classifyPodcastRecord = useCallback(
    async (recordId: string): Promise<PodcastClassificationResult> => {
      const response = await customFetch<{ data: PodcastClassificationResult }>(
        `/api/v1/control-center/records/${recordId}/classify`,
        {
          method: "POST",
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const runPodcastPipeline = useCallback(
    async (
      recordId: string,
      maxRetries = 1,
    ): Promise<PodcastPipelineRunResult> => {
      const response = await customFetch<{ data: PodcastPipelineRunResult }>(
        `/api/v1/control-center/records/${recordId}/pipeline/run?max_retries=${encodeURIComponent(String(maxRetries))}`,
        {
          method: "POST",
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const syncPodcastDriveNow =
    useCallback(async (): Promise<PodcastDriveSyncResult> => {
      throw new Error(
        "Google Drive podcast sync is not configured in this deployment yet.",
      );
    }, []);

  const getPodcastRecordView = useCallback(
    async (recordId: string): Promise<PodcastRecordView> => {
      const response = await customFetch<{ data: PodcastRecordView }>(
        `/api/v1/control-center/records/${recordId}/view`,
        {
          method: "GET",
        },
      );
      return response.data;
    },
    [],
  );

  const getPodcastRecordAudioBlob = useCallback(
    async (recordId: string): Promise<Blob> => {
      return customFetchBlob(
        `/api/v1/control-center/records/${recordId}/audio`,
        {
          method: "GET",
        },
      );
    },
    [],
  );

  const scanEventsWeek = useCallback(
    async (
      moduleId: string,
      moduleSlug: string,
      moduleTitle: string,
      sources: string[],
      weekStart?: string,
    ): Promise<EventScanResponse> => {
      const response = await customFetch<{ data: EventScanResponse }>(
        "/api/v1/control-center/events/scan-week",
        {
          method: "POST",
          body: JSON.stringify({
            module_id: moduleId,
            module_slug: moduleSlug,
            module_title: moduleTitle,
            sources,
            week_start: weekStart,
          }),
        },
      );
      await refreshRecords();
      return response.data;
    },
    [refreshRecords],
  );

  const addEventRecordToCalendar = useCallback(
    async (_recordId: string): Promise<unknown> => {
      return { status: "ok" };
    },
    [],
  );

  const runColdContactPipeline =
    useCallback(async (): Promise<ColdContactPipelineRunResult> => {
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

  const recomputeFollowUpTasks =
    useCallback(async (): Promise<FollowUpRecomputeResult> => {
      return { created: 0, updated: 0, skipped: 0 };
    }, []);

  const setNetworkMarketingViewMode = useCallback(
    async (mode: NetworkMarketingViewMode): Promise<void> => {
      const next = {
        ...state,
        networkMarketingViewMode: mode,
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
  return [...modules]
    .sort((left, right) => {
      if (left.order !== right.order) return left.order - right.order;
      return left.id.localeCompare(right.id);
    })
    .map((module, index) => ({ ...module, order: index + 1 }));
}
