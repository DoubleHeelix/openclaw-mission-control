"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Instagram } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  type BudgetImportResult,
  type ColdContactPipelineRunResult,
  type CustomRecord,
  type ControlCenterState,
  type ControlModule,
  type ControlRecordInput,
  type EventScanResponse,
  type FinanceRecord,
  type FinanceKind,
  type NetworkKind,
  type NetworkMarketingViewMode,
  type NetworkRecord,
  type NewsletterRecord,
  type PodcastIngestResult,
  type PodcastRecord,
  type PodcastDriveSyncResult,
  type PodcastRecordView,
  type PodcastSummaryResult,
  type PodcastTranscriptionResult,
  type ProspectQueueResult,
  type FollowUpTaskResult,
  type FollowUpRecomputeResult,
} from "@/lib/control-center";

type ModuleWorkspaceProps = {
  module: ControlModule;
  state: ControlCenterState;
  onCreateRecord: (input: ControlRecordInput) => Promise<void>;
  onDeleteRecord: (recordId: string) => Promise<void>;
  onPromoteRecord: (recordId: string, boardId: string) => Promise<void>;
  onUpdateStage: (recordId: string, stage: string) => Promise<void>;
  onUpdateRecord: (
    recordId: string,
    patch: {
      title?: string;
      summary?: string;
      stage?: string;
      data?: Record<string, unknown>;
    },
  ) => Promise<void>;
  onPodcastIngest: (file: File, title?: string, summary?: string) => Promise<PodcastIngestResult>;
  onPodcastTranscribe: (recordId: string, file: File, note?: string) => Promise<PodcastTranscriptionResult>;
  onPodcastSummarize: (recordId: string) => Promise<PodcastSummaryResult>;
  onPodcastExtractActions: (recordId: string, tasksBoardId: string) => Promise<void>;
  onPodcastClassify: (recordId: string) => Promise<void>;
  onPodcastRunPipeline: (recordId: string, maxRetries?: number) => Promise<void>;
  onPodcastDriveSyncNow: () => Promise<PodcastDriveSyncResult>;
  onPodcastView: (recordId: string) => Promise<PodcastRecordView>;
  onPodcastAudio: (recordId: string) => Promise<Blob>;
  onBudgetImport: (file: File) => Promise<BudgetImportResult>;
  onEventsScanWeek: (
    moduleId: string,
    moduleSlug: string,
    moduleTitle: string,
    sources: string[],
    weekStart?: string,
  ) => Promise<EventScanResponse>;
  onEventAddToCalendar: (recordId: string) => Promise<unknown>;
  onRunColdContactPipeline: () => Promise<ColdContactPipelineRunResult>;
  onGetColdContactQueue: (params?: {
    band?: "cold" | "moderate" | "warm" | "high_priority";
    stage?: string;
    platform?: string;
    source?: string;
    limit?: number;
  }) => Promise<ProspectQueueResult>;
  onGetFollowUpTasks: (params?: {
    due_today?: boolean;
    overdue?: boolean;
    stage?: string;
  }) => Promise<FollowUpTaskResult>;
  onRecomputeFollowUpTasks: () => Promise<FollowUpRecomputeResult>;
  onRefreshRecords: () => Promise<unknown>;
  networkMarketingViewMode: NetworkMarketingViewMode;
  onNetworkMarketingViewModeChange: (mode: NetworkMarketingViewMode) => Promise<void>;
};

const financeKinds: FinanceKind[] = ["budget_txn", "budget_rule", "email"];
const networkKinds: NetworkKind[] = ["conversation", "client"];
const priorities: Array<NewsletterRecord["priority"]> = ["low", "medium", "high"];
const DEFAULT_TASKS_BOARD_ID = "5fc5d021-13fa-4258-bd0e-8c4d22e151b0";
const MAX_PODCAST_UPLOAD_BYTES = 100 * 1024 * 1024; // Cloudflare proxied request ceiling.
const podcastCategoryOptions = [
  { value: "motivational-mindset", label: "Motivational / Mindset" },
  { value: "teaching", label: "Teaching" },
  { value: "process-call", label: "Process call" },
  { value: "general-catch-up", label: "General Catch up" },
  { value: "general", label: "General" },
  { value: "habits-productivity", label: "Habits / Productivity" },
  { value: "interview", label: "Interview" },
  { value: "uncategorized", label: "Uncategorized" },
] as const;
const budgetCategoryOptions = [
  "General / Home",
  "Motor Vehicle / Travel",
  "Insurance",
  "Entertainment",
  "Health & Fitness",
  "Financing Costs",
  "Discretionary",
  "Income",
  "Uncategorized",
] as const;
const coldContactConfidenceOptions = ["high", "medium", "low"] as const;

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
] as const;
const eventStages = [
  "discovered",
  "shortlisted",
  "interested",
  "contacted",
  "booked",
  "attended",
  "skipped",
] as const;
type EventStage = (typeof eventStages)[number];
const eventStageLabelMap: Record<EventStage, string> = {
  discovered: "Just added",
  shortlisted: "Top pick",
  interested: "Interested",
  contacted: "Contacted",
  booked: "Booked",
  attended: "Attended",
  skipped: "Skipped",
};

type NetworkMarketingStage = (typeof networkMarketingStages)[number];

function normalizeNetworkStage(stage: string | null | undefined): NetworkMarketingStage {
  const candidate = (stage ?? "").trim().toLowerCase().replace(/[\s-]+/g, "_");
  if ((networkMarketingStages as readonly string[]).includes(candidate)) {
    return candidate as NetworkMarketingStage;
  }

  const legacyMap: Record<string, NetworkMarketingStage> = {
    new_lead: "contact_made",
    contacted: "contact_made",
    call_booked: "meet_and_greet",
    proposal: "financial_blueprint",
    won: "launched",
    lost: "dropped_out",
  };

  return legacyMap[candidate] ?? "contact_made";
}

function formatFileSize(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  return `${bytes}B`;
}

function normalizeEventStage(stage: string | null | undefined): EventStage {
  const candidate = (stage ?? "").trim().toLowerCase().replace(/[\s-]+/g, "_");
  if ((eventStages as readonly string[]).includes(candidate)) {
    return candidate as EventStage;
  }
  return "discovered";
}

type TreeNode = {
  record: NetworkRecord;
  children: TreeNode[];
  depth: number;
  branchKey: string;
  branchLabel: string;
};

type TreeLevel = {
  depth: number;
  nodes: TreeNode[];
};

type MemberUpdateFields = {
  howDoing: string;
  whatsNew: string;
  urgent: string;
  notes: string;
};

type BranchGroup = {
  branchKey: string;
  branchLabel: string;
  nodes: TreeNode[];
};

type DealSignal = {
  label: string;
  items: string[];
};

type JourneyEntry = {
  at: string;
  note: string;
};

type TimedTranscriptWord = {
  key: string;
  start: number;
  end: number;
  text: string;
};

function readCustomRecordText(
  record: CustomRecord,
  keys: string[],
): string {
  const data = record.data && typeof record.data === "object" ? record.data : {};
  for (const key of keys) {
    const value = (data as Record<string, unknown>)[key];
    if (typeof value === "string" && value.trim()) return value.trim();
    if (Array.isArray(value)) {
      const parts = value.filter((item): item is string => typeof item === "string").map((item) => item.trim()).filter(Boolean);
      if (parts.length > 0) return parts.join("\n");
    }
  }
  return "";
}

function toPreviewText(value: string, maxChars = 180): string {
  const clean = value.trim();
  if (!clean || clean.length <= maxChars) return clean;
  return `${clean.slice(0, maxChars).trimEnd()}...`;
}

function getInstagramProfileUrl(record: NetworkRecord): string {
  const rawData = (record as unknown as { data?: unknown }).data;
  const data = rawData && typeof rawData === "object" ? (rawData as Record<string, unknown>) : {};
  const explicit = typeof data.instagram_profile_url === "string" ? data.instagram_profile_url.trim() : "";
  if (explicit) return explicit;
  const explicitAlt = typeof data.instagram_url === "string" ? data.instagram_url.trim() : "";
  if (explicitAlt) return explicitAlt;
  const usernameRaw = (typeof data.username === "string" ? data.username : "").trim();
  const username = usernameRaw.replace(/^@+/, "").trim();
  if (username) return `https://instagram.com/${username}`;
  const summary = String(record.summary || "").trim();
  const match = summary.match(/https?:\/\/(?:www\.)?instagram\.com\/[A-Za-z0-9._-]+\/?/i);
  if (match?.[0]) return match[0];
  return "";
}

function isProspectMessaged(record: CustomRecord): boolean {
  const data = record.data && typeof record.data === "object" ? record.data : {};
  const stage = String(record.stage || "").trim().toLowerCase();
  if (stage === "messaged") return true;
  return (
    (data as Record<string, unknown>).messaged === true ||
    String((data as Record<string, unknown>).status || "").trim().toLowerCase() === "messaged"
  );
}

function toColdContactScore(record: NetworkRecord): number {
  if (typeof record.coldContactScore === "number" && Number.isFinite(record.coldContactScore)) {
    return Math.max(0, Math.min(100, Math.round(record.coldContactScore)));
  }
  const summaryMatch = (record.summary || "").match(/(?:fit|score)\s*[:=]\s*(\d{1,3})/i);
  const parsed = summaryMatch ? Number(summaryMatch[1]) : Number.NaN;
  if (Number.isFinite(parsed)) return Math.max(0, Math.min(100, Math.round(parsed)));
  return 0;
}

function buildNetworkTree(records: NetworkRecord[]): TreeNode[] {
  const byId = new Map(records.map((record) => [record.id, record]));
  const childrenByParent = new Map<string, NetworkRecord[]>();
  const roots: NetworkRecord[] = [];

  for (const record of records) {
    const parentId = record.parentMemberId ?? null;
    if (!parentId || !byId.has(parentId) || parentId === record.id) {
      roots.push(record);
      continue;
    }
    const siblings = childrenByParent.get(parentId) ?? [];
    siblings.push(record);
    childrenByParent.set(parentId, siblings);
  }

  const sortByCreated = (a: NetworkRecord, b: NetworkRecord) => {
    const aTs = Date.parse(a.createdAt ?? a.updatedAt ?? "");
    const bTs = Date.parse(b.createdAt ?? b.updatedAt ?? "");
    if (Number.isFinite(aTs) && Number.isFinite(bTs) && aTs !== bTs) {
      return aTs - bTs;
    }
    return (a.displayName ?? a.title).localeCompare(b.displayName ?? b.title);
  };

  roots.sort(sortByCreated);

  const walk = (
    record: NetworkRecord,
    depth: number,
    seen: Set<string>,
    inheritedBranch: { key: string; label: string } | null,
  ): TreeNode => {
    if (seen.has(record.id)) {
      const fallback = inheritedBranch ?? {
        key: record.id,
        label: (record.displayName || record.title || "Team").trim(),
      };
      return { record, children: [], depth, branchKey: fallback.key, branchLabel: fallback.label };
    }
    const nextSeen = new Set(seen);
    nextSeen.add(record.id);
    const selfLabel = (record.displayName || record.title || "Team").trim();
    const branch =
      depth <= 1
        ? { key: record.id, label: selfLabel }
        : (inheritedBranch ?? { key: record.id, label: selfLabel });
    const children = [...(childrenByParent.get(record.id) ?? [])].sort(sortByCreated);
    return {
      record,
      depth,
      branchKey: branch.key,
      branchLabel: branch.label,
      children: children.map((child) => walk(child, depth + 1, nextSeen, branch)),
    };
  };

  return roots.map((root) => walk(root, 0, new Set(), null));
}

function flattenTreeLevels(roots: TreeNode[]): TreeLevel[] {
  if (!roots.length) return [];
  const levels = new Map<number, TreeNode[]>();
  const queue: TreeNode[] = [...roots];
  while (queue.length > 0) {
    const node = queue.shift();
    if (!node) break;
    const bucket = levels.get(node.depth) ?? [];
    bucket.push(node);
    levels.set(node.depth, bucket);
    queue.push(...node.children);
  }
  return [...levels.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([depth, nodes]) => ({ depth, nodes }));
}

function countLeafNodes(node: TreeNode): number {
  if (!node.children.length) return 1;
  return node.children.reduce((sum, child) => sum + countLeafNodes(child), 0);
}

function groupLevelByBranch(nodes: TreeNode[]): BranchGroup[] {
  const groups = new Map<string, BranchGroup>();
  for (const node of nodes) {
    const existing = groups.get(node.branchKey);
    if (existing) {
      existing.nodes.push(node);
      continue;
    }
    groups.set(node.branchKey, {
      branchKey: node.branchKey,
      branchLabel: node.branchLabel,
      nodes: [node],
    });
  }
  return [...groups.values()].sort((a, b) => a.branchLabel.localeCompare(b.branchLabel));
}

function branchAccentClass(input: string): string {
  const variants = [
    "border-emerald-300 bg-emerald-50/60",
    "border-sky-300 bg-sky-50/60",
    "border-violet-300 bg-violet-50/60",
    "border-amber-300 bg-amber-50/60",
    "border-rose-300 bg-rose-50/60",
  ];
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  return variants[hash % variants.length] ?? variants[0];
}

function resolveHuddleDay(record: NetworkRecord): string {
  if (record.huddleDay && record.huddleDay.trim()) return record.huddleDay.trim();
  const parsed = Date.parse(record.createdAt ?? record.updatedAt ?? "");
  if (Number.isFinite(parsed)) {
    return new Date(parsed).toLocaleDateString(undefined, {
      weekday: "short",
      day: "2-digit",
      month: "short",
      year: "numeric",
    });
  }
  return "Unknown day";
}

function formatTimelineDate(input: string): string {
  const ts = Date.parse(input);
  if (!Number.isFinite(ts)) return input;
  return new Date(ts).toLocaleString(undefined, {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function getTouchDistance(touches: {
  length: number;
  [index: number]: { clientX: number; clientY: number } | undefined;
}): number {
  if (touches.length < 2) return 0;
  const a = touches[0];
  const b = touches[1];
  if (!a || !b) return 0;
  const dx = a.clientX - b.clientX;
  const dy = a.clientY - b.clientY;
  return Math.sqrt(dx * dx + dy * dy);
}

function formatTranscriptForDisplay(value: string): string {
  const normalized = value.replace(/\r\n/g, "\n").trim();
  if (!normalized) return "";

  const lines = normalized
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const looksLineByLine = lines.length > 14 && lines.every((line) => line.length < 120);
  if (looksLineByLine) {
    const paragraphs: string[] = [];
    let current: string[] = [];
    for (const line of lines) {
      current.push(line);
      const joined = current.join(" ");
      if (/[.!?]$/.test(line) && joined.length > 180) {
        paragraphs.push(joined);
        current = [];
      }
    }
    if (current.length > 0) paragraphs.push(current.join(" "));
    return paragraphs.join("\n\n");
  }

  return normalized
    .replace(/\n{3,}/g, "\n\n")
    .replace(/([.!?])\s+(?=[A-Z])/g, "$1\n\n");
}

function formatSummaryForDisplay(value: string): string {
  const normalized = value.replace(/\r\n/g, "\n").trim();
  if (!normalized) return "";
  return normalized
    .replace(
      /^(Podcast Summary|Executive Summary|Key Points|Decisions|Risks|Follow-Ups|Action Plan)$/gm,
      "\n$1",
    )
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function parseVttTimestamp(value: string): number | null {
  const normalized = value.trim().replace(",", ".");
  const match = normalized.match(/^((\d+):)?(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?$/);
  if (!match) return null;
  const hours = Number(match[2] ?? 0);
  const minutes = Number(match[3] ?? 0);
  const seconds = Number(match[4] ?? 0);
  const millis = Number((match[5] ?? "0").padEnd(3, "0"));
  return hours * 3600 + minutes * 60 + seconds + millis / 1000;
}

function parseTimedTranscriptWords(vttText: string | null | undefined): TimedTranscriptWord[] {
  const raw = (vttText ?? "").trim();
  if (!raw) return [];
  const lines = raw.replace(/\r\n/g, "\n").split("\n");
  const words: TimedTranscriptWord[] = [];
  let idx = 0;
  let cueIndex = 0;

  while (idx < lines.length) {
    const line = lines[idx].trim();
    if (!line || line.toUpperCase() === "WEBVTT") {
      idx += 1;
      continue;
    }
    if (!line.includes("-->")) {
      idx += 1;
      continue;
    }
    const [startRaw, endRaw] = line.split("-->").map((part) => part.trim().split(" ")[0] || "");
    const start = parseVttTimestamp(startRaw);
    const end = parseVttTimestamp(endRaw);
    idx += 1;
    const cueTextLines: string[] = [];
    while (idx < lines.length && lines[idx].trim()) {
      cueTextLines.push(lines[idx].trim());
      idx += 1;
    }
    if (start === null || end === null || end <= start) continue;
    const cueText = cueTextLines
      .join(" ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (!cueText) continue;
    const cueWords = cueText.split(" ").filter(Boolean);
    if (cueWords.length === 0) continue;
    const step = (end - start) / cueWords.length;
    cueWords.forEach((word, wordIndex) => {
      const wordStart = start + step * wordIndex;
      words.push({
        key: `${cueIndex}-${wordIndex}`,
        start: wordStart,
        end: wordStart + step,
        text: word,
      });
    });
    cueIndex += 1;
  }
  return words.slice(0, 12000);
}

function parseTranscriptWordsPayload(
  input: Array<{ text?: unknown; start?: unknown; end?: unknown }> | null | undefined,
): TimedTranscriptWord[] {
  if (!Array.isArray(input) || input.length === 0) return [];
  const words: TimedTranscriptWord[] = [];
  input.forEach((item, idx) => {
    const text = typeof item?.text === "string" ? item.text.trim() : "";
    const start = typeof item?.start === "number" ? item.start : Number(item?.start);
    const end = typeof item?.end === "number" ? item.end : Number(item?.end);
    if (!text || !Number.isFinite(start) || !Number.isFinite(end) || end <= start) return;
    words.push({
      key: `word-${idx}`,
      start,
      end,
      text,
    });
  });
  return words.slice(0, 12000);
}

function buildApproxTimedTranscriptWords(
  transcriptText: string | null | undefined,
  durationSeconds: number,
): TimedTranscriptWord[] {
  const cleanText = (transcriptText ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleanText) return [];
  if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) return [];
  const tokens = cleanText.split(" ").map((token) => token.trim()).filter(Boolean);
  if (tokens.length === 0) return [];
  const cappedTokens = tokens.slice(0, 12000);
  const step = durationSeconds / cappedTokens.length;
  return cappedTokens.map((text, idx) => {
    const start = step * idx;
    return {
      key: `approx-${idx}`,
      start,
      end: start + step,
      text,
    };
  });
}

function buildPipelineSignals(record: NetworkRecord): {
  negative: DealSignal;
  positive: DealSignal;
} {
  const normalizedStage = normalizeNetworkStage(record.stage);
  const summaryLen = (record.summary || "").trim().length;
  const hasNextStep = Boolean((record.nextStep || "").trim());
  const updatedTs = Date.parse(record.updatedAt || "");
  const ageDays = Number.isFinite(updatedTs)
    ? Math.max(0, Math.floor((Date.now() - updatedTs) / 86400000))
    : 0;

  const negative: string[] = [];
  const positive: string[] = [];

  if (!hasNextStep) negative.push("No next step scheduled");
  if (summaryLen < 24) negative.push("Process notes are very light");
  if (["follow_up_2", "financial_blueprint", "offer_call"].includes(normalizedStage)) {
    negative.push("Late-stage process — keep momentum high");
  }
  if (ageDays >= 7) negative.push(`${ageDays} days since last update`);

  if (hasNextStep) positive.push("Next step scheduled");
  if (summaryLen >= 24) positive.push("Context-rich process notes");
  if (["launched"].includes(normalizedStage)) positive.push("Converted / launched");
  if (ageDays <= 2) positive.push("Recently active");

  return {
    negative: { label: `Negative signals (${negative.length})`, items: negative },
    positive: { label: `Positive signals (${positive.length})`, items: positive },
  };
}

function groupJourneyEntriesByDay(entries: JourneyEntry[]): Array<{ day: string; entries: JourneyEntry[] }> {
  const groups = new Map<string, JourneyEntry[]>();
  for (const entry of entries) {
    const ts = Date.parse(entry.at);
    const day = Number.isFinite(ts)
      ? new Date(ts).toLocaleDateString(undefined, {
          weekday: "short",
          day: "2-digit",
          month: "short",
          year: "numeric",
        })
      : "Unknown day";
    const bucket = groups.get(day) ?? [];
    bucket.push(entry);
    groups.set(day, bucket);
  }
  return [...groups.entries()].map(([day, dayEntries]) => ({
    day,
    entries: dayEntries.sort((a, b) => {
      const aTs = Date.parse(a.at);
      const bTs = Date.parse(b.at);
      return (Number.isFinite(bTs) ? bTs : 0) - (Number.isFinite(aTs) ? aTs : 0);
    }),
  }));
}

function isLikelyAllDayFromUtcMidnight(startAt?: string, sourceName?: string): boolean {
  if (!startAt || !sourceName) return false;
  const src = sourceName.toLowerCase();
  const dateOnlySources = [
    "whatson.melbourne.vic.gov.au",
    "eventbrite.com",
    "allevents.in",
    "concreteplayground.com",
  ];
  if (!dateOnlySources.some((domain) => src.includes(domain))) return false;
  return /t00:00(?::00(?:\.000)?)?(?:z|\+00:00)$/i.test(startAt);
}

function formatEventDateTime(startAt?: string, endAt?: string, sourceName?: string): string {
  const dateOnlyRe = /^\d{4}-\d{2}-\d{2}$/;
  const startIsDateOnly = Boolean(startAt && (dateOnlyRe.test(startAt) || isLikelyAllDayFromUtcMidnight(startAt, sourceName)));
  const endIsDateOnly = Boolean(endAt && dateOnlyRe.test(endAt));
  if (startIsDateOnly && startAt) {
    const normalizedStart = dateOnlyRe.test(startAt)
      ? startAt
      : new Date(startAt).toISOString().slice(0, 10);
    const [sy, sm, sd] = normalizedStart.split("-").map((part) => Number(part));
    const startDate = new Date(sy, (sm || 1) - 1, sd || 1, 12, 0, 0, 0);
    const startLabel = startDate.toLocaleDateString(undefined, {
      weekday: "short",
      day: "2-digit",
      month: "short",
    });
    if (!endAt || !endIsDateOnly) return `${startLabel} (All day)`;
    const [ey, em, ed] = endAt.split("-").map((part) => Number(part));
    const endDate = new Date(ey, (em || 1) - 1, ed || 1, 12, 0, 0, 0);
    const endLabel = endDate.toLocaleDateString(undefined, {
      weekday: "short",
      day: "2-digit",
      month: "short",
    });
    if (startDate.toDateString() === endDate.toDateString()) return `${startLabel} (All day)`;
    return `${startLabel} - ${endLabel} (All day)`;
  }

  const startTs = startAt ? Date.parse(startAt) : Number.NaN;
  const endTs = endAt ? Date.parse(endAt) : Number.NaN;
  if (!Number.isFinite(startTs)) return "Date TBC";
  const startDate = new Date(startTs);
  const startLabel = startDate.toLocaleString(undefined, {
    weekday: "short",
    day: "2-digit",
    month: "short",
    hour: "numeric",
    minute: "2-digit",
  });
  if (!Number.isFinite(endTs)) return startLabel;
  const endDate = new Date(endTs);
  const sameDay = startDate.toDateString() === endDate.toDateString();
  const endLabel = sameDay
    ? endDate.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" })
    : endDate.toLocaleString(undefined, {
        weekday: "short",
        day: "2-digit",
        month: "short",
        hour: "numeric",
        minute: "2-digit",
      });
  return `${startLabel} - ${endLabel}`;
}

function formatEventListDate(startAt?: string, sourceName?: string): string {
  const dateOnlyRe = /^\d{4}-\d{2}-\d{2}$/;
  const isDateOnly = Boolean(startAt && (dateOnlyRe.test(startAt) || isLikelyAllDayFromUtcMidnight(startAt, sourceName)));
  if (startAt && isDateOnly) {
    const normalized = dateOnlyRe.test(startAt)
      ? startAt
      : new Date(startAt).toISOString().slice(0, 10);
    const [y, m, d] = normalized.split("-").map((part) => Number(part));
    const startDate = new Date(y, (m || 1) - 1, d || 1, 12, 0, 0, 0);
    const weekday = startDate.toLocaleDateString(undefined, { weekday: "long" });
    return `${weekday} (All day)`;
  }
  const startTs = startAt ? Date.parse(startAt) : Number.NaN;
  if (!Number.isFinite(startTs)) return "Date TBC";
  const startDate = new Date(startTs);
  const weekday = startDate.toLocaleDateString(undefined, { weekday: "long" });
  const timeLabel = startDate.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
  return `${weekday} at ${timeLabel}`;
}

function formatEventDayHeader(startAt?: string, sourceName?: string): string {
  const dateOnlyRe = /^\d{4}-\d{2}-\d{2}$/;
  const isDateOnly = Boolean(
    startAt && (dateOnlyRe.test(startAt) || isLikelyAllDayFromUtcMidnight(startAt, sourceName)),
  );
  if (startAt && isDateOnly) {
    const normalized = dateOnlyRe.test(startAt) ? startAt : new Date(startAt).toISOString().slice(0, 10);
    const [y, m, d] = normalized.split("-").map((part) => Number(part));
    const local = new Date(y, (m || 1) - 1, d || 1, 12, 0, 0, 0);
    return local.toLocaleDateString(undefined, {
      weekday: "long",
      day: "2-digit",
      month: "short",
    });
  }
  const startTs = startAt ? Date.parse(startAt) : Number.NaN;
  if (!Number.isFinite(startTs)) return "Date TBC";
  const startDate = new Date(startTs);
  return startDate.toLocaleDateString(undefined, {
    weekday: "long",
    day: "2-digit",
    month: "short",
  });
}

function eventWeekdaySortKey(startAt?: string, sourceName?: string): number {
  const dateOnlyRe = /^\d{4}-\d{2}-\d{2}$/;
  const isDateOnly = Boolean(
    startAt && (dateOnlyRe.test(startAt) || isLikelyAllDayFromUtcMidnight(startAt, sourceName)),
  );
  if (startAt && isDateOnly) {
    const normalized = dateOnlyRe.test(startAt) ? startAt : new Date(startAt).toISOString().slice(0, 10);
    const [y, m, d] = normalized.split("-").map((part) => Number(part));
    const local = new Date(y, (m || 1) - 1, d || 1, 12, 0, 0, 0);
    return (local.getDay() + 6) % 7; // Monday=0 ... Sunday=6
  }
  const ts = startAt ? Date.parse(startAt) : Number.NaN;
  if (!Number.isFinite(ts)) return 99;
  const local = new Date(ts);
  return (local.getDay() + 6) % 7; // Monday=0 ... Sunday=6
}

type EventTimeConfidence = "exact" | "all_day" | "date_only" | "tbc";

function getEventTimeConfidence(record: CustomRecord): EventTimeConfidence {
  const startAt = (record.startAt || "").trim();
  if (!startAt) return "tbc";
  if (isLikelyAllDayFromUtcMidnight(startAt, record.sourceName)) return "all_day";
  if (/^\d{4}-\d{2}-\d{2}$/.test(startAt)) return "date_only";
  if (startAt.includes("T")) return "exact";
  return "tbc";
}

function eventTimeConfidenceMeta(confidence: EventTimeConfidence): { label: string; className: string } {
  switch (confidence) {
    case "exact":
      return {
        label: "Time: exact",
        className: "bg-emerald-100 text-emerald-800",
      };
    case "all_day":
      return {
        label: "Time: all day",
        className: "bg-sky-100 text-sky-800",
      };
    case "date_only":
      return {
        label: "Time: date only",
        className: "bg-amber-100 text-amber-800",
      };
    default:
      return {
        label: "Time: TBC",
        className: "bg-slate-100 text-slate-700",
      };
  }
}

type GeoPoint = { lat: number; lon: number };

function haversineKm(a: GeoPoint, b: GeoPoint): number {
  const R = 6371;
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = ((b.lon - a.lon) * Math.PI) / 180;
  const lat1 = (a.lat * Math.PI) / 180;
  const lat2 = (b.lat * Math.PI) / 180;
  const x =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));
  return R * c;
}

function deriveEventType(record: CustomRecord): string {
  const tags = Array.isArray(record.tags) ? record.tags : [];
  const firstTag = tags.find((tag) => String(tag || "").trim().length > 0);
  if (firstTag) return String(firstTag).trim();

  const text = `${record.title || ""} ${record.summary || ""}`.toLowerCase();
  const rules: Array<[string, string]> = [
    ["workshop", "Workshop"],
    ["conference", "Conference"],
    ["network", "Networking"],
    ["meetup", "Meetup"],
    ["music", "Music / Concert"],
    ["comedy", "Comedy"],
    ["fitness", "Fitness / Health"],
    ["sport", "Sports"],
    ["festival", "Festival"],
    ["food", "Food / Drink"],
    ["business", "Business"],
    ["startup", "Business"],
    ["art", "Art / Culture"],
    ["theatre", "Art / Culture"],
    ["movie", "Film / Cinema"],
    ["film", "Film / Cinema"],
  ];
  for (const [needle, label] of rules) {
    if (text.includes(needle)) return label;
  }
  return "General";
}

function eventSummarySnippet(record: CustomRecord): string {
  const raw = (record.summary || "").trim();
  if (!raw) return "";
  const compact = raw.replace(/\s+/g, " ");
  return compact.length > 220 ? `${compact.slice(0, 220)}...` : compact;
}

const URL_PATTERN = /https?:\/\/[^\s)]+/gi;

function extractSummaryUrls(summary?: string | null): string[] {
  if (!summary) return [];
  const matches = summary.match(URL_PATTERN) ?? [];
  const unique: string[] = [];
  for (const value of matches) {
    const cleaned = value.trim().replace(/[.,;!?]+$/, "");
    if (!cleaned) continue;
    if (!unique.includes(cleaned)) unique.push(cleaned);
  }
  return unique;
}

function resolveEventViewUrl(record: CustomRecord): string | null {
  const direct = (record.eventUrl || "").trim();
  if (direct) return direct;
  const links = extractSummaryUrls(record.summary);
  return links[0] ?? null;
}

function parseEventPriceValue(record: CustomRecord): number | null {
  if (record.isFree) return 0;
  const raw = String(record.price || "").trim();
  if (!raw) return null;
  const numeric = raw.replace(/[^0-9.]+/g, "");
  const parsed = Number.parseFloat(numeric);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildEventScanSummary(payload: EventScanResponse): string {
  const importedCount = payload.imported_count ?? payload.imported ?? payload.created ?? 0;
  const skippedDuplicates = payload.skipped_duplicates ?? payload.skipped ?? 0;
  const weekRange =
    payload.week_start && payload.week_end
      ? `${payload.week_start} -> ${payload.week_end}`
      : payload.week_start
        ? `from ${payload.week_start}`
        : "for the selected window";
  const dayCounts = new Map<string, number>();
  for (const event of payload.events ?? []) {
    const ts = Date.parse(event.start_at || "");
    const key = Number.isFinite(ts)
      ? new Date(ts).toLocaleDateString(undefined, { weekday: "short", day: "2-digit", month: "short" })
      : "Date TBC";
    dayCounts.set(key, (dayCounts.get(key) ?? 0) + 1);
  }

  const dayLine =
    dayCounts.size > 0
      ? [...dayCounts.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 4)
          .map(([day, count]) => `${day}: ${count}`)
          .join(" • ")
      : "No dated events found.";

  const topEvents = (payload.events ?? []).slice(0, 5).map((event) => {
    const when = formatEventListDate(event.start_at);
    return `- ${event.title} (${when})`;
  });

  return [
    payload.message?.trim() ||
      `Imported ${importedCount} events, skipped ${skippedDuplicates} duplicates (${weekRange}).`,
    `Busiest days: ${dayLine}`,
    ...(topEvents.length > 0 ? ["Top picks this week:", ...topEvents] : []),
  ].join("\n");
}

function buildEventSignals(record: CustomRecord): {
  negative: DealSignal;
  positive: DealSignal;
} {
  const negative: string[] = [];
  const positive: string[] = [];
  const stage = normalizeEventStage(record.stage);
  const hasWhen = Boolean(record.startAt && record.startAt.trim());
  const hasWhere = Boolean(record.venue && record.venue.trim());
  const hasLink = Boolean(record.eventUrl && record.eventUrl.trim());
  const hasSummary = Boolean(record.summary && record.summary.trim().length >= 20);
  const hasJourney = Array.isArray(record.journeyEntries) && record.journeyEntries.length > 0;

  if (!hasWhen) negative.push("No start date/time captured");
  if (!hasWhere) negative.push("Venue details missing");
  if (!hasLink) negative.push("No source/event URL saved");
  if (!hasSummary) negative.push("Summary is too light");
  if (["booked", "attended"].includes(stage) && !hasJourney) {
    negative.push("No journey notes for late-stage event");
  }

  if (hasWhen) positive.push("Date/time captured");
  if (hasWhere) positive.push("Venue captured");
  if (hasLink) positive.push("Source URL saved");
  if (hasSummary) positive.push("Context-rich event summary");
  if (hasJourney) positive.push("Journey notes available");
  if (record.isFree) positive.push("Free event");

  return {
    negative: { label: `Negative signals (${negative.length})`, items: negative },
    positive: { label: `Positive signals (${positive.length})`, items: positive },
  };
}

export function ModuleWorkspace({
  module,
  state,
  onCreateRecord,
  onDeleteRecord,
  onPromoteRecord,
  onUpdateStage,
  onUpdateRecord,
  onPodcastIngest,
  onPodcastTranscribe,
  onPodcastSummarize,
  onPodcastExtractActions,
  onPodcastClassify,
  onPodcastRunPipeline,
  onPodcastDriveSyncNow,
  onPodcastView,
  onPodcastAudio,
  onBudgetImport,
  onEventsScanWeek,
  onEventAddToCalendar,
  onRunColdContactPipeline,
  onGetColdContactQueue,
  onGetFollowUpTasks,
  onRecomputeFollowUpTasks,
  onRefreshRecords,
  networkMarketingViewMode,
  onNetworkMarketingViewModeChange,
}: ModuleWorkspaceProps) {
  const isBudgetModule = module.id === "finance";
  const [title, setTitle] = useState("");
  const [summary, setSummary] = useState("");
  const [stage, setStage] = useState("inbox");

  const [financeKind, setFinanceKind] = useState<FinanceKind>("budget_txn");
  const [financeAmount, setFinanceAmount] = useState("");
  const [budgetImportFile, setBudgetImportFile] = useState<File | null>(null);
  const [budgetImportBusy, setBudgetImportBusy] = useState(false);
  const [budgetRefreshBusy, setBudgetRefreshBusy] = useState(false);
  const [budgetResetBusy, setBudgetResetBusy] = useState(false);
  const [budgetImportMessage, setBudgetImportMessage] = useState<string | null>(null);
  const [budgetRuleKeyword, setBudgetRuleKeyword] = useState("");
  const [budgetRuleCategory, setBudgetRuleCategory] = useState<string>("Uncategorized");
  const [budgetRuleSubcategory, setBudgetRuleSubcategory] = useState("");

  const [networkKind, setNetworkKind] = useState<NetworkKind>("conversation");
  const [networkNextStep, setNetworkNextStep] = useState("");
  const [mobilePipelineStage, setMobilePipelineStage] = useState<NetworkMarketingStage>("contact_made");
  const [memberParentId, setMemberParentId] = useState<string>("root");
  const [memberStatusSummary, setMemberStatusSummary] = useState("");

  const [newsletterSource, setNewsletterSource] = useState("");
  const [newsletterAction, setNewsletterAction] = useState("");
  const [newsletterPriority, setNewsletterPriority] =
    useState<NewsletterRecord["priority"]>("medium");
  const [promoteBoardId, setPromoteBoardId] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [draggingRecordId, setDraggingRecordId] = useState<string | null>(null);
  const [podcastUploadFile, setPodcastUploadFile] = useState<File | null>(null);
  const [podcastBusyRecordId, setPodcastBusyRecordId] = useState<string | null>(null);
  const [podcastPipelineBusyRecordId, setPodcastPipelineBusyRecordId] = useState<string | null>(null);
  const [podcastDriveSyncBusy, setPodcastDriveSyncBusy] = useState(false);
  const [podcastDriveSyncMessage, setPodcastDriveSyncMessage] = useState<string | null>(null);
  const [expandedPodcastId, setExpandedPodcastId] = useState<string | null>(null);
  const [podcastViewLoadingId, setPodcastViewLoadingId] = useState<string | null>(null);
  const [podcastAudioLoadingId, setPodcastAudioLoadingId] = useState<string | null>(null);
  const [podcastViews, setPodcastViews] = useState<Record<string, PodcastRecordView>>({});
  const [podcastAudioUrls, setPodcastAudioUrls] = useState<Record<string, string>>({});
  const [podcastPlaybackSeconds, setPodcastPlaybackSeconds] = useState<Record<string, number>>({});
  const [podcastDurationSeconds, setPodcastDurationSeconds] = useState<Record<string, number>>({});
  const [podcastPendingJumpScrollRecordId, setPodcastPendingJumpScrollRecordId] = useState<string | null>(null);
  const [collapsedPodcastFolders, setCollapsedPodcastFolders] = useState<Record<string, boolean>>({});
  const [mpaBusyRecordId, setMpaBusyRecordId] = useState<string | null>(null);
  const [followUpToggleBusyRecordId, setFollowUpToggleBusyRecordId] = useState<string | null>(null);
  const [teamTreeZoom, setTeamTreeZoom] = useState(1);
  const pinchStartDistanceRef = useRef<number | null>(null);
  const pinchStartZoomRef = useRef<number>(1);
  const [activeMemberRecord, setActiveMemberRecord] = useState<NetworkRecord | null>(null);
  const [activePipelineRecord, setActivePipelineRecord] = useState<NetworkRecord | null>(null);
  const [pipelineModalBusy, setPipelineModalBusy] = useState(false);
  const [pipelineSummary, setPipelineSummary] = useState("");
  const [pipelineStage, setPipelineStage] = useState<NetworkMarketingStage>("contact_made");
  const [pipelineNextStep, setPipelineNextStep] = useState("");
  const [pipelineFollowUpDate, setPipelineFollowUpDate] = useState("");
  const [pipelineFollowUpCompleted, setPipelineFollowUpCompleted] = useState(false);
  const [pipelineFollowUpCompletedAt, setPipelineFollowUpCompletedAt] = useState<string | null>(null);
  const [pipelineLinkedMemberId, setPipelineLinkedMemberId] = useState<string>("none");
  const [pipelineJourneyDraft, setPipelineJourneyDraft] = useState("");
  const [pipelineJourneyEntries, setPipelineJourneyEntries] = useState<JourneyEntry[]>([]);
  const [memberModalBusy, setMemberModalBusy] = useState(false);
  const [memberHowDoing, setMemberHowDoing] = useState("");
  const [memberWhatsNew, setMemberWhatsNew] = useState("");
  const [memberUrgent, setMemberUrgent] = useState("");
  const [memberNotes, setMemberNotes] = useState("");
  const [eventsSourceInput, setEventsSourceInput] = useState(
    [
      "https://whatson.melbourne.vic.gov.au/",
      "https://www.eventbrite.com.au/d/australia--melbourne/events/",
      "https://allevents.in/melbourne",
      "https://www.meetup.com/find/?location=au--melbourne&source=EVENTS",
      "https://concreteplayground.com/melbourne/events",
      "https://events.humanitix.com/",
      "https://www.eventfinda.co.nz/feed/events/new-zealand/whatson/upcoming.rss",
    ].join("\n"),
  );
  const [eventsWeekStart, setEventsWeekStart] = useState("");
  const [eventsScanBusy, setEventsScanBusy] = useState(false);
  const [eventsScanMessage, setEventsScanMessage] = useState<string | null>(null);
  const [eventsBulkDeleteBusy, setEventsBulkDeleteBusy] = useState(false);
  const [eventCostFilter, setEventCostFilter] = useState<"all" | "free" | "paid">("all");
  const [eventPriceSort, setEventPriceSort] = useState<"none" | "asc" | "desc">("none");
  const [eventDateFromFilter, setEventDateFromFilter] = useState("");
  const [eventDateToFilter, setEventDateToFilter] = useState("");
  const [eventTypeFilter, setEventTypeFilter] = useState("all");
  const [eventReferenceAddress, setEventReferenceAddress] = useState("");
  const [eventDistanceBusy, setEventDistanceBusy] = useState(false);
  const [eventCalendarBusyRecordId, setEventCalendarBusyRecordId] = useState<string | null>(null);
  const [eventDistanceError, setEventDistanceError] = useState<string | null>(null);
  const [eventDistanceKmByRecord, setEventDistanceKmByRecord] = useState<Record<string, number>>({});
  const [eventGeoCache, setEventGeoCache] = useState<Record<string, GeoPoint | null>>({});
  const [activeEventRecord, setActiveEventRecord] = useState<CustomRecord | null>(null);
  const [eventModalBusy, setEventModalBusy] = useState(false);
  const [eventSummary, setEventSummary] = useState("");
  const [eventStage, setEventStage] = useState<EventStage>("discovered");
  const [pipelineProspectContext, setPipelineProspectContext] = useState("");
  const [pipelineProspectOpeners, setPipelineProspectOpeners] = useState("");
  const [pipelineProspectWhy, setPipelineProspectWhy] = useState("");
  const [coldContactPlatform, setColdContactPlatform] = useState("instagram");
  const [coldContactProfileUrl, setColdContactProfileUrl] = useState("");
  const [coldContactWhyFit, setColdContactWhyFit] = useState("");
  const [coldContactWhyNow, setColdContactWhyNow] = useState("");
  const [coldContactSignals, setColdContactSignals] = useState("");
  const [coldContactScore, setColdContactScore] = useState("");
  const [coldContactConfidence, setColdContactConfidence] = useState<(typeof coldContactConfidenceOptions)[number]>("medium");
  const [coldContactAngle, setColdContactAngle] = useState("");
  const [coldContactResearch, setColdContactResearch] = useState("");
  const [coldContactSource, setColdContactSource] = useState("");
  const [coldContactImportFile, setColdContactImportFile] = useState<File | null>(null);
  const [coldContactImportBusy, setColdContactImportBusy] = useState(false);
  const [coldContactPipelineBusy, setColdContactPipelineBusy] = useState(false);
  const [coldContactPipelineMessage, setColdContactPipelineMessage] = useState<string | null>(null);
  const [coldContactQueueBusy, setColdContactQueueBusy] = useState(false);
  const [coldContactFollowupBusy, setColdContactFollowupBusy] = useState(false);
  const [coldContactQueueItems, setColdContactQueueItems] = useState<ProspectQueueResult["items"]>([]);
  const [coldContactFollowupItems, setColdContactFollowupItems] = useState<FollowUpTaskResult["items"]>([]);
  const [pipelineImportFile, setPipelineImportFile] = useState<File | null>(null);
  const [pipelineImportBusy, setPipelineImportBusy] = useState(false);
  const [expandedProspectContextById, setExpandedProspectContextById] = useState<Record<string, boolean>>({});
  const [copiedProspectOpenerKey, setCopiedProspectOpenerKey] = useState<string | null>(null);
  const isNetworkMarketingModule =
    module.id === "network_marketing" || module.slug === "network-marketing";
  const isProspectingPipelineModule = module.slug === "pipeline";
  const isEventsModule =
    module.id === "events" || module.slug === "events" || module.title.trim().toLowerCase() === "events";
  const isPodcastsModule =
    module.id === "podcasts" || module.slug === "podcasts" || module.category === "podcasts";

  useEffect(() => {
    if (isNetworkMarketingModule) {
      setStage("contact_made");
      return;
    }
    if (isProspectingPipelineModule) {
      setStage("new");
      return;
    }
    if (isPodcastsModule) {
      setStage("general");
      return;
    }
    if (isEventsModule) {
      setStage("discovered");
      return;
    }
    if (module.id === "finance") {
      setStage("uncategorized");
      return;
    }
    setStage("inbox");
  }, [
    isEventsModule,
    isPodcastsModule,
    isNetworkMarketingModule,
    isProspectingPipelineModule,
    module.id,
    module.slug,
  ]);

  const records = useMemo(() => {
    if (module.id === "finance") return state.records.finance;
    if (isNetworkMarketingModule) return state.records.network_marketing;
    if (module.id === "newsletters") return state.records.newsletters;
    if (isPodcastsModule) return state.records.podcasts;
    return state.records.custom[module.id] ?? [];
  }, [isNetworkMarketingModule, isPodcastsModule, module.id, state.records]);

  const prospectingRecords = useMemo(() => {
    if (!isProspectingPipelineModule) return [] as CustomRecord[];
    const items = [...(records as CustomRecord[])];
    items.sort((a, b) => {
      const aMessaged = isProspectMessaged(a);
      const bMessaged = isProspectMessaged(b);
      if (aMessaged !== bMessaged) return aMessaged ? 1 : -1;
      const aTs = Date.parse(a.updatedAt || "");
      const bTs = Date.parse(b.updatedAt || "");
      if (Number.isFinite(aTs) && Number.isFinite(bTs) && aTs !== bTs) return bTs - aTs;
      return (a.title || "").localeCompare(b.title || "");
    });
    return items;
  }, [isProspectingPipelineModule, records]);

  const eventRecords = useMemo(
    () =>
      ((state.records.custom[module.id] ?? []).filter(
        (record) =>
          (record.kind || "").toLowerCase() === "event" ||
          !!record.startAt ||
          !!record.venue ||
          module.slug === "events",
      ) as CustomRecord[]).sort((a, b) => {
        const aTs = Date.parse(a.startAt ?? a.updatedAt ?? "");
        const bTs = Date.parse(b.startAt ?? b.updatedAt ?? "");
        if (Number.isFinite(aTs) && Number.isFinite(bTs) && aTs !== bTs) return aTs - bTs;
        return (a.title || "").localeCompare(b.title || "");
      }),
    [module.id, module.slug, state.records.custom],
  );

  const eventTypeOptions = useMemo(() => {
    const unique = new Set<string>();
    for (const record of eventRecords) unique.add(deriveEventType(record));
    return [...unique].sort((a, b) => a.localeCompare(b));
  }, [eventRecords]);

  const filteredEventRecords = useMemo(() => {
    const dateFrom = eventDateFromFilter ? Date.parse(`${eventDateFromFilter}T00:00:00`) : Number.NaN;
    const dateTo = eventDateToFilter ? Date.parse(`${eventDateToFilter}T23:59:59`) : Number.NaN;
    return eventRecords.filter((record) => {
      const isFree = record.isFree === true || (record.price || "").trim().toLowerCase() === "free";
      if (eventCostFilter === "free" && !isFree) return false;
      if (eventCostFilter === "paid" && isFree) return false;

      const ts = Date.parse(record.startAt ?? record.updatedAt ?? "");
      if (Number.isFinite(dateFrom) && (!Number.isFinite(ts) || ts < dateFrom)) return false;
      if (Number.isFinite(dateTo) && (!Number.isFinite(ts) || ts > dateTo)) return false;

      const type = deriveEventType(record);
      if (eventTypeFilter !== "all" && type !== eventTypeFilter) return false;
      return true;
    });
  }, [eventRecords, eventCostFilter, eventDateFromFilter, eventDateToFilter, eventTypeFilter]);

  const hasEventDistanceSort = useMemo(
    () => eventReferenceAddress.trim().length > 0 && Object.keys(eventDistanceKmByRecord).length > 0,
    [eventReferenceAddress, eventDistanceKmByRecord],
  );

  const hasCustomEventSort = hasEventDistanceSort || eventPriceSort !== "none";

  const visibleEventRecords = useMemo(() => {
    const records = [...filteredEventRecords];
    records.sort((a, b) => {
      if (hasEventDistanceSort) {
        const aKm = eventDistanceKmByRecord[a.id];
        const bKm = eventDistanceKmByRecord[b.id];
        const aKnown = Number.isFinite(aKm);
        const bKnown = Number.isFinite(bKm);
        if (aKnown && bKnown && aKm !== bKm) return (aKm as number) - (bKm as number);
        if (aKnown && !bKnown) return -1;
        if (!aKnown && bKnown) return 1;
      }

      if (eventPriceSort !== "none") {
        const aPrice = parseEventPriceValue(a);
        const bPrice = parseEventPriceValue(b);
        const aKnown = aPrice !== null;
        const bKnown = bPrice !== null;
        if (aKnown && bKnown && aPrice !== bPrice) {
          return eventPriceSort === "asc" ? aPrice - bPrice : bPrice - aPrice;
        }
        if (aKnown && !bKnown) return -1;
        if (!aKnown && bKnown) return 1;
      }

      const aWeekday = eventWeekdaySortKey(a.startAt, a.sourceName);
      const bWeekday = eventWeekdaySortKey(b.startAt, b.sourceName);
      if (aWeekday !== bWeekday) return aWeekday - bWeekday;

      const aTs = Date.parse(a.startAt ?? a.updatedAt ?? "");
      const bTs = Date.parse(b.startAt ?? b.updatedAt ?? "");
      if (Number.isFinite(aTs) && Number.isFinite(bTs) && aTs !== bTs) return aTs - bTs;
      if (Number.isFinite(aTs) && !Number.isFinite(bTs)) return -1;
      if (!Number.isFinite(aTs) && Number.isFinite(bTs)) return 1;
      return (a.title || "").localeCompare(b.title || "");
    });
    return records;
  }, [filteredEventRecords, eventDistanceKmByRecord, hasEventDistanceSort, eventPriceSort]);

  const podcastCategoryLabelMap: Record<string, string> = {
    "motivational-mindset": "Motivational / Mindset",
    teaching: "Teaching",
    "process-call": "Process call",
    "general-catch-up": "General Catch up",
    interview: "Interview",
    general: "General",
    "habits-productivity": "Habits / Productivity",
    uncategorized: "Uncategorized",
  };
  const podcastFolderOrder = [
    "motivational-mindset",
    "teaching",
    "process-call",
    "general-catch-up",
    "interview",
    "habits-productivity",
    "general",
    "uncategorized",
  ];
  const podcastFolders = useMemo(() => {
    if (!isPodcastsModule) return [] as Array<{ key: string; label: string; items: PodcastRecord[] }>;
    const grouped = new Map<string, PodcastRecord[]>();
    for (const item of records as PodcastRecord[]) {
      const key = (item.category || "uncategorized").trim().toLowerCase() || "uncategorized";
      const bucket = grouped.get(key) ?? [];
      bucket.push(item);
      grouped.set(key, bucket);
    }
    const ordered: Array<{ key: string; label: string; items: PodcastRecord[] }> = [];
    for (const key of podcastFolderOrder) {
      const items = grouped.get(key);
      if (items && items.length > 0) {
        ordered.push({ key, label: podcastCategoryLabelMap[key] ?? key, items });
        grouped.delete(key);
      }
    }
    for (const [key, items] of grouped.entries()) {
      ordered.push({ key, label: podcastCategoryLabelMap[key] ?? key, items });
    }
    return ordered;
  }, [isPodcastsModule, records]);

  useEffect(() => {
    if (!isPodcastsModule) return;
    setCollapsedPodcastFolders((prev) => {
      const next: Record<string, boolean> = {};
      for (const folder of podcastFolders) {
        next[folder.key] = prev[folder.key] ?? true;
      }
      return next;
    });
  }, [isPodcastsModule, podcastFolders]);

  useEffect(
    () => () => {
      for (const url of Object.values(podcastAudioUrls)) {
        URL.revokeObjectURL(url);
      }
    },
    [podcastAudioUrls],
  );

  useEffect(() => {
    if (!podcastPendingJumpScrollRecordId) return;
    const recordId = podcastPendingJumpScrollRecordId;
    const rafId = window.requestAnimationFrame(() => {
      const activeWord = document.querySelector<HTMLElement>(
        `[data-podcast-word="true"][data-record-id="${recordId}"][data-active="true"]`,
      );
      if (activeWord) {
        activeWord.scrollIntoView({
          block: "center",
          inline: "nearest",
          behavior: "smooth",
        });
      }
      setPodcastPendingJumpScrollRecordId((current) => (current === recordId ? null : current));
    });
    return () => window.cancelAnimationFrame(rafId);
  }, [podcastPendingJumpScrollRecordId, podcastPlaybackSeconds]);

  useEffect(() => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "cold_contact") return;
    void refreshColdContactQueue();
    void refreshColdContactFollowups();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isNetworkMarketingModule, networkMarketingViewMode]);

  const resetForm = () => {
    setTitle("");
    setSummary("");
    setStage(
      isNetworkMarketingModule
        ? "contact_made"
        : isProspectingPipelineModule
          ? "new"
        : isEventsModule
          ? "discovered"
        : isPodcastsModule
          ? "general"
        : module.id === "finance"
          ? "uncategorized"
          : "inbox",
    );
    setFinanceAmount("");
    setNetworkNextStep("");
    setMemberParentId("root");
    setMemberStatusSummary("");
    setNewsletterSource("");
    setNewsletterAction("");
    setNewsletterPriority("medium");
    setPodcastUploadFile(null);
    setBudgetImportFile(null);
    setPipelineProspectContext("");
    setPipelineProspectOpeners("");
    setPipelineProspectWhy("");
    setColdContactPlatform("instagram");
    setColdContactProfileUrl("");
    setColdContactWhyFit("");
    setColdContactWhyNow("");
    setColdContactSignals("");
    setColdContactScore("");
    setColdContactConfidence("medium");
    setColdContactAngle("");
    setColdContactResearch("");
    setColdContactSource("");
    setColdContactImportFile(null);
    setPipelineImportFile(null);
  };

  const onAdd = async () => {
    const cleanTitle = title.trim();
    if (!cleanTitle || isSubmitting) return;
    setIsSubmitting(true);
    setError(null);
    try {
      if (module.id === "finance") {
        const amount = financeAmount.trim() ? Number(financeAmount.trim()) : null;
        const category = stage.trim() || "Uncategorized";
        await onCreateRecord({
          module,
          kind: "finance",
          title: cleanTitle,
          summary: summary.trim(),
          stage: category,
          data: {
            kind: financeKind,
            amount: Number.isFinite(amount as number) ? amount : null,
            category,
            uncategorized: category === "Uncategorized",
          },
        });
      } else if (isNetworkMarketingModule) {
        if (networkMarketingViewMode === "team_tree") {
          const normalizedParentId = memberParentId === "root" ? null : memberParentId;
          const parentExists =
            normalizedParentId === null ||
            teamMemberRecords.some((record) => record.id === normalizedParentId);
          if (!parentExists) {
            setError("Selected parent member is invalid. Pick root or an existing member.");
            return;
          }
          await onCreateRecord({
            module,
            kind: "network_marketing",
            title: cleanTitle,
            summary: summary.trim(),
            stage: normalizeNetworkStage(stage),
            data: {
              kind: "team_member",
              nextStep: networkNextStep.trim(),
              parent_member_id: normalizedParentId,
              direct_to_me: normalizedParentId === null,
              display_name: cleanTitle,
              status_summary: memberStatusSummary.trim(),
              created_at: new Date().toISOString(),
            },
          });
        } else if (networkMarketingViewMode === "cold_contact") {
          const parsedScore = Number(coldContactScore.trim());
          await onCreateRecord({
            module,
            kind: "network_marketing",
            title: cleanTitle,
            summary: summary.trim() || coldContactWhyNow.trim(),
            stage: normalizeNetworkStage(stage),
            data: {
              kind: "cold_contact",
              nextStep: networkNextStep.trim(),
              cold_contact_platform: coldContactPlatform.trim().toLowerCase(),
              cold_contact_profile_url: coldContactProfileUrl.trim(),
              cold_contact_why_fit: coldContactWhyFit.trim(),
              cold_contact_why_now: coldContactWhyNow.trim(),
              cold_contact_signals: coldContactSignals
                .split("\n")
                .map((line) => line.trim())
                .filter(Boolean),
              cold_contact_score:
                Number.isFinite(parsedScore) && parsedScore >= 0
                  ? Math.max(0, Math.min(100, Math.round(parsedScore)))
                  : undefined,
              cold_contact_confidence: coldContactConfidence,
              cold_contact_angle: coldContactAngle.trim(),
              cold_contact_research: coldContactResearch.trim(),
              cold_contact_source: coldContactSource.trim(),
              cold_contact_last_active_at: new Date().toISOString(),
            },
          });
        } else {
          await onCreateRecord({
            module,
            kind: "network_marketing",
            title: cleanTitle,
            summary: summary.trim(),
            stage: normalizeNetworkStage(stage),
            data: {
              kind: networkKind,
              nextStep: networkNextStep.trim(),
            },
          });
        }
      } else if (module.id === "newsletters") {
        await onCreateRecord({
          module,
          kind: "newsletters",
          title: cleanTitle,
          summary: summary.trim(),
          stage: stage.trim() || "inbox",
          data: {
            source: newsletterSource.trim(),
            action: newsletterAction.trim(),
            priority: newsletterPriority,
          },
        });
      } else if (isPodcastsModule) {
        if (!podcastUploadFile) {
          setError("Select an audio file (mp3/m4a/wav) to upload.");
          return;
        }
        if (podcastUploadFile.size > MAX_PODCAST_UPLOAD_BYTES) {
          setError(
            `Audio file is too large (${formatFileSize(
              podcastUploadFile.size,
            )}). Max upload size is 100MB when using the remote URL.`,
          );
          return;
        }
        const selectedCategory = (stage || "general").trim().toLowerCase();
        const ingested = await onPodcastIngest(podcastUploadFile, cleanTitle);
        await onUpdateRecord(ingested.record.id, {
          data: {
            category: selectedCategory,
          },
        });
      } else if (isEventsModule) {
        await onCreateRecord({
          module,
          kind: "custom",
          title: cleanTitle,
          summary: summary.trim(),
          stage: normalizeEventStage(stage),
          data: {
            kind: "event",
            event_contact: "",
            event_priority: "normal",
            journey_entries: [],
          },
        });
      } else if (isProspectingPipelineModule) {
        await onCreateRecord({
          module,
          kind: "custom",
          title: cleanTitle,
          summary: pipelineProspectContext.trim() || summary.trim(),
          stage: stage.trim() || "new",
          data: {
            kind: "prospect",
            username: cleanTitle,
            context: pipelineProspectContext.trim(),
            openers: pipelineProspectOpeners
              .split("\n")
              .map((line) => line.trim())
              .filter(Boolean),
            why_this_opener: pipelineProspectWhy.trim(),
          },
        });
      } else {
        await onCreateRecord({
          module,
          kind: "custom",
          title: cleanTitle,
          summary: summary.trim(),
          stage: stage.trim() || "inbox",
        });
      }
      resetForm();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to create record.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const onDelete = async (recordId: string) => {
    try {
      await onDeleteRecord(recordId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to delete record.");
    }
  };

  const onMarkProspectMessaged = async (record: CustomRecord) => {
    if (isProspectMessaged(record)) return;
    try {
      setError(null);
      await onUpdateRecord(record.id, {
        stage: "messaged",
        data: {
          messaged: true,
          status: "messaged",
          messaged_at: new Date().toISOString(),
        },
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to mark prospect as messaged.");
    }
  };

  const onCopyProspectOpener = async (recordId: string, openerIndex: number, text: string) => {
    const clean = text.trim();
    if (!clean) return;
    const key = `${recordId}:${openerIndex}`;
    try {
      if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(clean);
      } else {
        const area = document.createElement("textarea");
        area.value = clean;
        area.style.position = "fixed";
        area.style.opacity = "0";
        document.body.appendChild(area);
        area.focus();
        area.select();
        document.execCommand("copy");
        document.body.removeChild(area);
      }
      setCopiedProspectOpenerKey(key);
      window.setTimeout(() => setCopiedProspectOpenerKey((prev) => (prev === key ? null : prev)), 1600);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to copy opener.");
    }
  };

  const onImportPipelineProspects = async () => {
    if (!isProspectingPipelineModule || !pipelineImportFile || pipelineImportBusy) return;
    setPipelineImportBusy(true);
    setError(null);
    try {
      const raw = await pipelineImportFile.text();
      const parsed = JSON.parse(raw) as {
        records?: Array<{
          username?: unknown;
          context?: unknown;
          openers?: unknown;
          why_this_opener?: unknown;
          stage?: unknown;
        }>;
      };
      const records = Array.isArray(parsed.records) ? parsed.records : [];
      if (records.length === 0) {
        throw new Error("Import file has no records[] entries.");
      }
      let created = 0;
      for (const item of records) {
        const usernameRaw = typeof item.username === "string" ? item.username.trim() : "";
        const username = usernameRaw.replace(/^@+/, "");
        if (!username) continue;
        const context = typeof item.context === "string" ? item.context.trim() : "";
        const openers = Array.isArray(item.openers)
          ? item.openers.filter((entry): entry is string => typeof entry === "string").map((entry) => entry.trim()).filter(Boolean)
          : [];
        const why = typeof item.why_this_opener === "string" ? item.why_this_opener.trim() : "";
        const rowStage = typeof item.stage === "string" && item.stage.trim() ? item.stage.trim() : "new";
        await onCreateRecord({
          module,
          kind: "custom",
          title: username,
          summary: context,
          stage: rowStage,
          data: {
            kind: "prospect",
            username,
            context,
            openers,
            why_this_opener: why,
          },
        });
        created += 1;
      }
      if (created === 0) {
        throw new Error("No valid rows found. Each record needs username.");
      }
      setPipelineImportFile(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to import pipeline records.");
    } finally {
      setPipelineImportBusy(false);
    }
  };

  const onImportColdContacts = async () => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "cold_contact" || !coldContactImportFile || coldContactImportBusy) {
      return;
    }
    setColdContactImportBusy(true);
    setError(null);
    try {
      const raw = await coldContactImportFile.text();
      const parsed = JSON.parse(raw) as {
        records?: Array<{
          title?: unknown;
          name?: unknown;
          platform?: unknown;
          profile_url?: unknown;
          why_fit?: unknown;
          why_now?: unknown;
          signals?: unknown;
          fit_score?: unknown;
          confidence?: unknown;
          angle?: unknown;
          research?: unknown;
          source?: unknown;
          stage?: unknown;
          next_step?: unknown;
          summary?: unknown;
        }>;
      };
      const rows = Array.isArray(parsed.records) ? parsed.records : [];
      if (rows.length === 0) {
        throw new Error("Import file has no records[] entries.");
      }
      let created = 0;
      for (const item of rows) {
        const title =
          typeof item.title === "string"
            ? item.title.trim()
            : typeof item.name === "string"
              ? item.name.trim()
              : "";
        if (!title) continue;
        const platform = typeof item.platform === "string" ? item.platform.trim().toLowerCase() : "instagram";
        const profileUrl = typeof item.profile_url === "string" ? item.profile_url.trim() : "";
        const whyFit = typeof item.why_fit === "string" ? item.why_fit.trim() : "";
        const whyNow = typeof item.why_now === "string" ? item.why_now.trim() : "";
        const signals = Array.isArray(item.signals)
          ? item.signals.filter((entry): entry is string => typeof entry === "string").map((entry) => entry.trim()).filter(Boolean)
          : [];
        const fitScoreRaw = typeof item.fit_score === "number" ? item.fit_score : Number(item.fit_score);
        const fitScore = Number.isFinite(fitScoreRaw) ? Math.max(0, Math.min(100, Math.round(fitScoreRaw))) : undefined;
        const confidence = typeof item.confidence === "string" ? item.confidence.trim().toLowerCase() : "medium";
        const angle = typeof item.angle === "string" ? item.angle.trim() : "";
        const research = typeof item.research === "string" ? item.research.trim() : "";
        const source = typeof item.source === "string" ? item.source.trim() : "cold_contact_import";
        const rowStage =
          typeof item.stage === "string" && item.stage.trim()
            ? normalizeNetworkStage(item.stage)
            : "contact_made";
        const nextStep = typeof item.next_step === "string" ? item.next_step.trim() : "Personal outreach";
        const summary =
          typeof item.summary === "string" && item.summary.trim()
            ? item.summary.trim()
            : whyNow || whyFit || research;

        await onCreateRecord({
          module,
          kind: "network_marketing",
          title,
          summary,
          stage: rowStage,
          data: {
            kind: "cold_contact",
            nextStep,
            cold_contact_platform: platform,
            cold_contact_profile_url: profileUrl,
            cold_contact_why_fit: whyFit,
            cold_contact_why_now: whyNow,
            cold_contact_signals: signals,
            cold_contact_score: fitScore,
            cold_contact_confidence: confidence,
            cold_contact_angle: angle,
            cold_contact_research: research,
            cold_contact_source: source,
            cold_contact_last_active_at: new Date().toISOString(),
          },
        });
        created += 1;
      }
      if (created === 0) {
        throw new Error("No valid rows found. Each record needs title or name.");
      }
      setColdContactImportFile(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to import cold contacts.");
    } finally {
      setColdContactImportBusy(false);
    }
  };

  const onRunColdContactPipelineNow = async () => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "cold_contact" || coldContactPipelineBusy) {
      return;
    }
    setColdContactPipelineBusy(true);
    setColdContactPipelineMessage(null);
    setError(null);
    try {
      const result = await onRunColdContactPipeline();
      const lines = [
        result.success ? "Cold Contact pipeline run completed." : "Cold Contact pipeline run failed.",
        `Candidates: ${result.total_candidates}`,
        `Exported: ${result.exported_records}`,
        `Imported to board: ${result.imported_records}`,
        `Skipped duplicates: ${result.skipped_duplicates}`,
        `High confidence: ${result.high_confidence}`,
        `Import file: ${result.import_path}`,
      ];
      if (result.output?.trim()) {
        lines.push("", result.output.trim());
      }
      setColdContactPipelineMessage(lines.join("\n"));
      const [queueRes, followRes] = await Promise.all([
        onGetColdContactQueue({ limit: 50 }),
        onGetFollowUpTasks({ due_today: true }),
      ]);
      setColdContactQueueItems(queueRes.items);
      setColdContactFollowupItems(followRes.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to run cold-contact pipeline.");
    } finally {
      setColdContactPipelineBusy(false);
    }
  };

  const refreshColdContactQueue = async () => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "cold_contact") return;
    try {
      setColdContactQueueBusy(true);
      setError(null);
      const queueRes = await onGetColdContactQueue({ limit: 100 });
      setColdContactQueueItems(queueRes.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load cold-contact queue.");
    } finally {
      setColdContactQueueBusy(false);
    }
  };

  const refreshColdContactFollowups = async () => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "cold_contact") return;
    try {
      setColdContactFollowupBusy(true);
      setError(null);
      const taskRes = await onGetFollowUpTasks({ due_today: true, overdue: true });
      setColdContactFollowupItems(taskRes.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load follow-up tasks.");
    } finally {
      setColdContactFollowupBusy(false);
    }
  };

  const recomputeColdContactFollowups = async () => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "cold_contact") return;
    try {
      setColdContactFollowupBusy(true);
      setError(null);
      const result = await onRecomputeFollowUpTasks();
      setColdContactPipelineMessage((prev) => {
        const base = prev?.trim() ? `${prev}\n\n` : "";
        return `${base}Follow-up recompute: created=${result.created}, updated=${result.updated}, skipped=${result.skipped}`;
      });
      const taskRes = await onGetFollowUpTasks({ due_today: true, overdue: true });
      setColdContactFollowupItems(taskRes.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to recompute follow-up tasks.");
    } finally {
      setColdContactFollowupBusy(false);
    }
  };

  const onPromote = async (recordId: string) => {
    const boardId = promoteBoardId.trim();
    if (!boardId) {
      setError("Enter a board ID to promote records into tasks.");
      return;
    }
    try {
      await onPromoteRecord(recordId, boardId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to create task.");
    }
  };

  const moveRecordToStage = async (recordId: string, nextStage: NetworkMarketingStage) => {
    try {
      setError(null);
      await onUpdateStage(recordId, nextStage);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to move record.");
    }
  };

  const onEditHuddlePlayName = async (record: NetworkRecord) => {
    const currentName = (record.title || record.personName || "").trim();
    const nextName = window.prompt("Update contact name", currentName);
    if (!nextName) return;
    const cleanName = nextName.trim();
    if (!cleanName || cleanName === currentName) return;
    try {
      setError(null);
      await onUpdateRecord(record.id, { title: cleanName });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to update contact name.");
    }
  };

  const onAddHuddleToMadeAware = async (record: NetworkRecord) => {
    try {
      setError(null);
      setMpaBusyRecordId(record.id);
      const contactName =
        record.title?.trim() || record.personName?.trim() || "Unknown Contact";
      await onCreateRecord({
        module,
        kind: "network_marketing",
        title: contactName,
        summary: "",
        stage: "made_aware",
        data: {
          kind: "client",
          nextStep: "",
        },
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to add contact to Made Aware.");
    } finally {
      setMpaBusyRecordId(null);
    }
  };

  const onToggleFollowUpCompleted = async (record: NetworkRecord, completed: boolean) => {
    try {
      setError(null);
      setFollowUpToggleBusyRecordId(record.id);
      const completedAt = completed ? new Date().toISOString() : null;
      await onUpdateRecord(record.id, {
        data: {
          follow_up_completed: completed,
          followUpCompleted: completed,
          follow_up_completed_at: completedAt,
          followUpCompletedAt: completedAt,
        },
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to update follow-up status.");
    } finally {
      setFollowUpToggleBusyRecordId(null);
    }
  };

  const onSummarizePodcast = async (recordId: string) => {
    try {
      setError(null);
      setPodcastBusyRecordId(recordId);
      await onPodcastSummarize(recordId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to summarize podcast record.");
    } finally {
      setPodcastBusyRecordId(null);
    }
  };

  const onExtractPodcastActions = async (recordId: string) => {
    const boardId = promoteBoardId.trim() || DEFAULT_TASKS_BOARD_ID;
    try {
      setError(null);
      setPodcastBusyRecordId(recordId);
      await onPodcastExtractActions(recordId, boardId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to extract actions.");
    } finally {
      setPodcastBusyRecordId(null);
    }
  };

  const onClassifyPodcast = async (recordId: string) => {
    try {
      setError(null);
      setPodcastBusyRecordId(recordId);
      await onPodcastClassify(recordId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to classify podcast record.");
    } finally {
      setPodcastBusyRecordId(null);
    }
  };

  const onRunPodcastPipeline = async (recordId: string) => {
    try {
      setError(null);
      setPodcastPipelineBusyRecordId(recordId);
      await onPodcastRunPipeline(recordId, 1);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to run podcast pipeline.");
    } finally {
      setPodcastPipelineBusyRecordId(null);
    }
  };

  const onSyncPodcastDriveNow = async () => {
    try {
      setError(null);
      setPodcastDriveSyncBusy(true);
      setPodcastDriveSyncMessage(null);
      const result = await onPodcastDriveSyncNow();
      const summary = `Sync complete: scanned ${result.scanned}, imported ${result.imported}, skipped ${result.skipped}, failed ${result.failed}`;
      setPodcastDriveSyncMessage(summary);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to sync podcasts from Drive.");
      setPodcastDriveSyncMessage(null);
    } finally {
      setPodcastDriveSyncBusy(false);
    }
  };

  const onTogglePodcastExpanded = async (recordId: string) => {
    if (expandedPodcastId === recordId) {
      setExpandedPodcastId(null);
      return;
    }
    setExpandedPodcastId(recordId);
    const needView = !podcastViews[recordId];
    const needAudio = !podcastAudioUrls[recordId];
    if (!needView && !needAudio) return;
    try {
      if (needView) setPodcastViewLoadingId(recordId);
      if (needAudio) setPodcastAudioLoadingId(recordId);
      setError(null);
      if (needView) {
        const payload = await onPodcastView(recordId);
        setPodcastViews((prev) => ({ ...prev, [recordId]: payload }));
      }
      if (needAudio) {
        const audioBlob = await onPodcastAudio(recordId);
        const audioUrl = URL.createObjectURL(audioBlob);
        setPodcastAudioUrls((prev) => {
          const existing = prev[recordId];
          if (existing) {
            URL.revokeObjectURL(audioUrl);
            return prev;
          }
          return { ...prev, [recordId]: audioUrl };
        });
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load podcast details.");
    } finally {
      setPodcastViewLoadingId(null);
      setPodcastAudioLoadingId(null);
    }
  };

  const onPromotePipelineToTeamTree = async (record: NetworkRecord) => {
    const alreadyMoved = Boolean(record.movedToTreeAt);
    if (alreadyMoved) return;
    const confirmMove = window.confirm(
      `Move ${record.title} to Team Tree? This creates a team member record and keeps audit history.`,
    );
    if (!confirmMove) return;

    const movedAt = new Date().toISOString();
    try {
      setError(null);
      await onCreateRecord({
        module,
        kind: "network_marketing",
        title: record.title,
        summary: record.summary,
        stage: "launched",
        data: {
          kind: "team_member",
          nextStep: record.nextStep || "",
          parent_member_id: null,
          direct_to_me: true,
          display_name: record.title,
          status_summary: record.summary || "",
          created_at: movedAt,
        },
      });

      await onUpdateRecord(record.id, {
        data: {
          moved_to_tree_at: movedAt,
          moved_by: "manual_ui",
          moved_from_record_id: record.id,
        },
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to move record to Team Tree.");
    }
  };

  const onUpdateTeamMember = async (record: NetworkRecord) => {
    const priorStatus = (record.statusSummary || record.summary || "").trim();
    const statusSummary = window.prompt("Update status summary", priorStatus) ?? "";
    const nextStep = window.prompt("Update next step", record.nextStep || "") ?? "";
    if (!statusSummary.trim() && !nextStep.trim()) return;

    const timeline = [...(record.updateTimeline ?? [])];
    const timelineNote = statusSummary.trim() || nextStep.trim();
    timeline.push({
      at: new Date().toISOString(),
      note: timelineNote,
    });

    try {
      setError(null);
      await onUpdateRecord(record.id, {
        summary: statusSummary.trim() || record.summary,
        data: {
          status_summary: statusSummary.trim(),
          next_step: nextStep.trim(),
          update_timeline: timeline,
        },
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to update team member.");
    }
  };

  const onImportBudgetStatement = async () => {
    if (budgetImportBusy) return;
    if (!budgetImportFile) {
      setError("Select a CSV or PDF statement first.");
      return;
    }
    try {
      setError(null);
      setBudgetImportMessage(null);
      setBudgetImportBusy(true);
      const result = await onBudgetImport(budgetImportFile);
      setBudgetImportMessage(
        `Imported ${result.imported_count} transactions (${result.categorized_count} categorized, ${result.uncategorized_count} uncategorized).`,
      );
      setBudgetImportFile(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to import statement.");
    } finally {
      setBudgetImportBusy(false);
    }
  };

  const openPipelineModal = (record: NetworkRecord) => {
    const normalizedStage = normalizeNetworkStage(record.stage);
    setActivePipelineRecord(record);
    setPipelineSummary(record.summary || "");
    setPipelineStage(normalizedStage);
    setPipelineNextStep(record.nextStep || "");
    setPipelineFollowUpDate((record.followUpDate || "").trim());
    setPipelineFollowUpCompleted(Boolean(record.followUpCompleted));
    setPipelineFollowUpCompletedAt(
      typeof record.followUpCompletedAt === "string" && record.followUpCompletedAt.trim()
        ? record.followUpCompletedAt.trim()
        : null,
    );
    setPipelineLinkedMemberId(record.linkedMemberId?.trim() || "none");
    setPipelineJourneyDraft("");
    setPipelineJourneyEntries(
      Array.isArray(record.journeyEntries) ? [...record.journeyEntries] : [],
    );
  };

  const closePipelineModal = () => {
    if (pipelineModalBusy) return;
    setActivePipelineRecord(null);
    setPipelineSummary("");
    setPipelineStage("contact_made");
    setPipelineNextStep("");
    setPipelineFollowUpDate("");
    setPipelineFollowUpCompleted(false);
    setPipelineFollowUpCompletedAt(null);
    setPipelineLinkedMemberId("none");
    setPipelineJourneyDraft("");
    setPipelineJourneyEntries([]);
  };

  const addPipelineJourneyEntry = () => {
    const item = pipelineJourneyDraft.trim();
    if (!item) return;
    setPipelineJourneyEntries((prev) =>
      [{ at: new Date().toISOString(), note: item }, ...prev].slice(0, 100),
    );
    setPipelineJourneyDraft("");
  };

  const savePipelineModal = async () => {
    if (!activePipelineRecord) return;
    const nowIso = new Date().toISOString();
    const draft = pipelineJourneyDraft.trim();
    const selectedMember = teamMemberRecords.find(
      (member) => member.id === pipelineLinkedMemberId,
    );
    const linkedMemberId = pipelineLinkedMemberId === "none" ? null : pipelineLinkedMemberId;
    const linkedMemberName =
      pipelineLinkedMemberId === "none"
        ? null
        : (selectedMember?.displayName || selectedMember?.title || "").trim() || null;
    const nextJourneyEntries = draft
      ? [{ at: nowIso, note: draft }, ...pipelineJourneyEntries]
      : [...pipelineJourneyEntries];
    try {
      setPipelineModalBusy(true);
      setError(null);
      await onUpdateRecord(activePipelineRecord.id, {
        summary: pipelineSummary.trim(),
        stage: pipelineStage,
        data: {
          next_step: pipelineNextStep.trim(),
          nextStep: pipelineNextStep.trim(),
          follow_up_date: pipelineFollowUpDate.trim(),
          followUpDate: pipelineFollowUpDate.trim(),
          follow_up_completed: pipelineFollowUpCompleted,
          followUpCompleted: pipelineFollowUpCompleted,
          follow_up_completed_at: pipelineFollowUpCompleted ? pipelineFollowUpCompletedAt : null,
          followUpCompletedAt: pipelineFollowUpCompleted ? pipelineFollowUpCompletedAt : null,
          linked_member_id: linkedMemberId,
          linked_member_name: linkedMemberName,
          journey_entries: nextJourneyEntries,
        },
      });
      setPipelineJourneyEntries(nextJourneyEntries);
      setPipelineJourneyDraft("");
      closePipelineModal();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save process details.");
    } finally {
      setPipelineModalBusy(false);
    }
  };

  const movePipelineModalToStage = async (nextStage: NetworkMarketingStage) => {
    if (!activePipelineRecord) return;
    try {
      setPipelineModalBusy(true);
      setError(null);
      await onUpdateStage(activePipelineRecord.id, nextStage);
      setPipelineStage(nextStage);
      setActivePipelineRecord((prev) =>
        prev ? { ...prev, stage: nextStage, updatedAt: new Date().toISOString() } : prev,
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to move process to stage.");
    } finally {
      setPipelineModalBusy(false);
    }
  };

  const deletePipelineJourneyEntry = async (entryAt: string, entryNote: string) => {
    if (!activePipelineRecord || pipelineModalBusy) return;
    const confirmed = window.confirm("Delete this journey entry?");
    if (!confirmed) return;

    const current = [...pipelineJourneyEntries];
    let removed = false;
    const next = current.filter((entry) => {
      if (!removed && entry.at === entryAt && entry.note === entryNote) {
        removed = true;
        return false;
      }
      return true;
    });
    if (!removed) return;

    try {
      setPipelineModalBusy(true);
      setError(null);
      await onUpdateRecord(activePipelineRecord.id, {
        data: {
          journey_entries: next,
        },
      });
      setPipelineJourneyEntries(next);
      setActivePipelineRecord((prev) =>
        prev ? { ...prev, journeyEntries: next } : prev,
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to delete journey entry.");
    } finally {
      setPipelineModalBusy(false);
    }
  };

  const openMemberModal = (record: NetworkRecord) => {
    setActiveMemberRecord(record);
    setMemberHowDoing("");
    setMemberWhatsNew("");
    setMemberUrgent("");
    setMemberNotes("");
  };

  const closeMemberModal = (force = false) => {
    if (memberModalBusy && !force) return;
    setActiveMemberRecord(null);
    setMemberHowDoing("");
    setMemberWhatsNew("");
    setMemberUrgent("");
    setMemberNotes("");
  };

  const saveMemberModal = async () => {
    if (!activeMemberRecord) return;
    const payload = {
      howDoing: memberHowDoing.trim(),
      whatsNew: memberWhatsNew.trim(),
      urgent: memberUrgent.trim(),
      notes: memberNotes.trim(),
    };

    const lines = [
      payload.howDoing ? `How doing: ${payload.howDoing}` : "",
      payload.whatsNew ? `New: ${payload.whatsNew}` : "",
      payload.urgent ? `Urgent: ${payload.urgent}` : "",
      payload.notes ? `Notes: ${payload.notes}` : "",
    ].filter(Boolean);

    const timelineNote = lines.length > 0 ? lines.join(" | ") : "Member update saved";
    const existingTimeline = [...(activeMemberRecord.updateTimeline ?? [])];
    existingTimeline.push({
      at: new Date().toISOString(),
      note: timelineNote,
    });

    try {
      setMemberModalBusy(true);
      setError(null);
      await onUpdateRecord(activeMemberRecord.id, {
        summary: payload.howDoing || activeMemberRecord.summary || "",
        data: {
          status_summary: payload.howDoing,
          next_step: payload.urgent,
          update_timeline: existingTimeline,
          member_how_doing: payload.howDoing,
          member_whats_new: payload.whatsNew,
          member_urgent: payload.urgent,
          member_notes: payload.notes,
          member_last_update_at: new Date().toISOString(),
        },
      });
      closeMemberModal(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save member update.");
    } finally {
      setMemberModalBusy(false);
    }
  };

  const deleteMemberTimelineEntry = async (entryAt: string, entryNote: string) => {
    if (!activeMemberRecord || memberModalBusy) return;
    const confirmed = window.confirm("Delete this update history entry?");
    if (!confirmed) return;

    const currentTimeline = [...(activeMemberRecord.updateTimeline ?? [])];
    let removed = false;
    const nextTimeline = currentTimeline.filter((item) => {
      if (!removed && item.at === entryAt && item.note === entryNote) {
        removed = true;
        return false;
      }
      return true;
    });
    if (!removed) return;

    try {
      setMemberModalBusy(true);
      setError(null);
      await onUpdateRecord(activeMemberRecord.id, {
        data: {
          update_timeline: nextTimeline,
        },
      });
      setActiveMemberRecord((prev) =>
        prev ? { ...prev, updateTimeline: nextTimeline } : prev,
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to delete update history entry.");
    } finally {
      setMemberModalBusy(false);
    }
  };

  const onRefreshBudget = async () => {
    if (budgetRefreshBusy) return;
    try {
      setError(null);
      setBudgetImportMessage(null);
      setBudgetRefreshBusy(true);
      await onRefreshRecords();
      setBudgetImportMessage("Budget refreshed.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to refresh budget.");
    } finally {
      setBudgetRefreshBusy(false);
    }
  };

  const onResetBudget = async () => {
    if (budgetResetBusy) return;
    const shouldReset = window.confirm(
      "Reset Budget will remove all imported statements, transactions, and rules. Continue?",
    );
    if (!shouldReset) return;
    try {
      setError(null);
      setBudgetImportMessage(null);
      setBudgetResetBusy(true);
      const financeRecords = records as FinanceRecord[];
      await Promise.all(financeRecords.map((record) => onDeleteRecord(record.id)));
      await onRefreshRecords();
      setBudgetImportMessage("Budget reset complete.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to reset budget.");
    } finally {
      setBudgetResetBusy(false);
    }
  };

  const scanEventsForWeek = async () => {
    if (eventsScanBusy) return;
    const sources = eventsSourceInput
      .split(/\n|,/)
      .map((value) => value.trim())
      .filter(Boolean);
    if (sources.length === 0) {
      setError("Enter at least one event source URL.");
      return;
    }
    try {
      setError(null);
      setEventsScanMessage(null);
      setEventsScanBusy(true);
      const payload = await onEventsScanWeek(
        module.id,
        module.slug,
        module.title,
        sources,
        eventsWeekStart.trim() || undefined,
      );
      await onRefreshRecords();
      setEventsScanMessage(buildEventScanSummary(payload));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to scan event sources.");
    } finally {
      setEventsScanBusy(false);
    }
  };

  const removeAllEvents = async () => {
    if (eventsBulkDeleteBusy || eventRecords.length === 0) return;
    const confirmed = window.confirm(
      `Delete all ${eventRecords.length} events? This cannot be undone.`,
    );
    if (!confirmed) return;
    try {
      setError(null);
      setEventsScanMessage(null);
      setEventsBulkDeleteBusy(true);
      for (const record of eventRecords) {
        // Sequential delete reduces API pressure and avoids burst failures.
        // eslint-disable-next-line no-await-in-loop
        await onDeleteRecord(record.id);
      }
      await onRefreshRecords();
      setEventsScanMessage("All events removed.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to remove all events.");
    } finally {
      setEventsBulkDeleteBusy(false);
    }
  };

  const geocodeAddress = async (query: string): Promise<GeoPoint | null> => {
    const clean = query.trim();
    if (!clean) return null;
    try {
      const response = await fetch(
        `/api/v1/control-center/events/geocode?${new URLSearchParams({ query: clean }).toString()}`,
      );
      if (!response.ok) return null;
      const payload = (await response.json()) as { lat?: number; lon?: number; ok?: boolean };
      const lat = Number(payload.lat ?? Number.NaN);
      const lon = Number(payload.lon ?? Number.NaN);
      if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
      return null;
    } catch {
      return null;
    }
  };

  const eventLocationQuery = (record: CustomRecord): string => {
    const address = String(record.address || "").trim();
    const venue = String(record.venue || "").trim();
    const city = String(record.city || "").trim() || "Melbourne";
    const country = String(record.country || "").trim() || "Australia";
    const base = [address, venue, city, country].filter((value) => value.length > 0).join(", ");
    return base;
  };

  const sortEventsByNearest = async () => {
    const originText = eventReferenceAddress.trim();
    if (!originText) {
      setEventDistanceError("Enter your address first.");
      return;
    }
    if (filteredEventRecords.length === 0) {
      setEventDistanceError("No events match current filters.");
      return;
    }
    setEventDistanceBusy(true);
    setEventDistanceError(null);
    try {
      const origin = await geocodeAddress(originText);
      if (!origin) {
        setEventDistanceError("Could not geocode your address. Try a more specific suburb/street.");
        return;
      }

      const nextGeoCache: Record<string, GeoPoint | null> = { ...eventGeoCache };
      const nextDistance: Record<string, number> = {};

      for (const record of filteredEventRecords) {
        let point = nextGeoCache[record.id];
        if (point === undefined) {
          const query = eventLocationQuery(record);
          point = await geocodeAddress(query);
          nextGeoCache[record.id] = point;
        }
        if (point) {
          nextDistance[record.id] = haversineKm(origin, point);
        }
      }

      setEventGeoCache(nextGeoCache);
      setEventDistanceKmByRecord(nextDistance);
      const resolved = Object.keys(nextDistance).length;
      if (resolved === 0) {
        setEventDistanceError("Could not calculate distances for current events.");
      }
    } finally {
      setEventDistanceBusy(false);
    }
  };

  const openEventModal = (record: CustomRecord) => {
    setActiveEventRecord(record);
    setEventSummary(record.summary || "");
    setEventStage(normalizeEventStage(record.stage));
  };

  const closeEventModal = () => {
    if (eventModalBusy) return;
    setActiveEventRecord(null);
    setEventSummary("");
    setEventStage("discovered");
  };

  const saveEventModal = async () => {
    if (!activeEventRecord) return;
    try {
      setEventModalBusy(true);
      setError(null);
      await onUpdateRecord(activeEventRecord.id, {
        summary: eventSummary.trim(),
        stage: eventStage,
        data: { event_notes: eventSummary.trim() },
      });
      closeEventModal();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save event.");
    } finally {
      setEventModalBusy(false);
    }
  };

  const addEventToCalendar = async (record: CustomRecord) => {
    try {
      setError(null);
      setEventsScanMessage(null);
      setEventCalendarBusyRecordId(record.id);
      await onEventAddToCalendar(record.id);
      setEventsScanMessage(`Added "${record.title}" to your Google Calendar.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to add event to calendar.");
    } finally {
      setEventCalendarBusyRecordId(null);
    }
  };

  const moveEventToStage = async (nextStage: EventStage) => {
    if (!activeEventRecord) return;
    try {
      setEventModalBusy(true);
      setError(null);
      await onUpdateStage(activeEventRecord.id, nextStage);
      setEventStage(nextStage);
      setActiveEventRecord((prev) =>
        prev ? { ...prev, stage: nextStage, updatedAt: new Date().toISOString() } : prev,
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to move event.");
    } finally {
      setEventModalBusy(false);
    }
  };

  const exportEventsWeekCsv = () => {
    const headers = [
      "title",
      "stage",
      "start_at",
      "end_at",
      "venue",
      "city",
      "country",
      "organizer",
      "price",
      "currency",
      "is_free",
      "event_url",
      "source_name",
      "source_url",
      "summary",
    ];
    const escape = (value: unknown) => {
      const text = String(value ?? "");
      return `"${text.replace(/"/g, '""')}"`;
    };
    const rows = eventRecords.map((record) =>
      [
        record.title,
        record.stage,
        record.startAt ?? "",
        record.endAt ?? "",
        record.venue ?? "",
        record.city ?? "",
        record.country ?? "",
        record.organizer ?? "",
        record.price ?? "",
        record.currency ?? "",
        record.isFree ? "true" : "false",
        record.eventUrl ?? "",
        record.sourceName ?? "",
        record.sourceUrl ?? "",
        record.summary ?? "",
      ]
        .map(escape)
        .join(","),
    );
    const csv = [headers.join(","), ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `events-${eventsWeekStart || "this-week"}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  const onRecategorizeBudgetTransaction = async (
    record: FinanceRecord,
    nextCategory: string,
  ) => {
    try {
      setError(null);
      await onUpdateRecord(record.id, {
        stage: nextCategory,
        data: {
          category: nextCategory,
          uncategorized: nextCategory === "Uncategorized",
        },
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to update category.");
    }
  };

  const onSaveBudgetRule = async () => {
    const keyword = budgetRuleKeyword.trim();
    const category = budgetRuleCategory.trim();
    const subcategory = budgetRuleSubcategory.trim();
    if (!keyword || !category || !subcategory) return;
    try {
      setError(null);
      await onCreateRecord({
        module,
        kind: "finance",
        title: `Rule: ${keyword}`,
        summary: `${category} / ${subcategory}`,
        stage: "rule",
        data: {
          kind: "budget_rule",
          keyword,
          category,
          subcategory,
        },
      });
      setBudgetRuleKeyword("");
      setBudgetRuleSubcategory("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save budget rule.");
    }
  };

  const networkColumns = useMemo(() => {
    if (!isNetworkMarketingModule) {
      return [] as Array<{ id: NetworkMarketingStage; title: string; records: typeof records }>;
    }

    return networkMarketingStages.map((stageId) => ({
      id: stageId,
      title: stageId.replaceAll("_", " "),
      records: records.filter(
        (record) =>
          "kind" in record &&
          String(record.kind) !== "huddle_play" &&
          String(record.kind) !== "team_member" &&
          String(record.kind) !== "cold_contact" &&
          !("movedToTreeAt" in record && Boolean(record.movedToTreeAt)) &&
          normalizeNetworkStage("stage" in record ? record.stage : "") === stageId,
      ),
    }));
  }, [module.id, records]);

  const activeMobilePipelineColumn = useMemo(
    () =>
      networkColumns.find((column) => column.id === mobilePipelineStage) ??
      networkColumns[0] ??
      null,
    [mobilePipelineStage, networkColumns],
  );

  useEffect(() => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "pipeline") return;
    if (networkColumns.length === 0) return;
    if (networkColumns.some((column) => column.id === mobilePipelineStage)) return;
    setMobilePipelineStage(networkColumns[0].id);
  }, [isNetworkMarketingModule, mobilePipelineStage, networkColumns, networkMarketingViewMode]);

  const networkTree = useMemo(() => {
    if (!isNetworkMarketingModule) return [] as TreeNode[];
    return buildNetworkTree(
      (records as NetworkRecord[]).filter((record) => String(record.kind) === "team_member"),
    );
  }, [module.id, records]);

  const treeLevels = useMemo(() => flattenTreeLevels(networkTree), [networkTree]);
  const treeLevelGroups = useMemo(
    () =>
      treeLevels.map((level) => ({
        level,
        groups: level.depth === 0 ? [] : groupLevelByBranch(level.nodes),
      })),
    [treeLevels],
  );
  const treeCanvasMinWidth = useMemo(() => {
    const totalLeaves = Math.max(
      1,
      networkTree.reduce((sum, root) => sum + countLeafNodes(root), 0),
    );
    return Math.max(760, totalLeaves * 260);
  }, [networkTree]);

  const teamMemberRecords = useMemo(() => {
    if (!isNetworkMarketingModule) return [] as NetworkRecord[];
    return (records as NetworkRecord[]).filter((record) => String(record.kind) === "team_member");
  }, [module.id, records]);

  const activeMemberTimeline = useMemo(() => {
    if (!activeMemberRecord?.updateTimeline) return [];
    return [...activeMemberRecord.updateTimeline].sort((a, b) => {
      const aTs = Date.parse(a.at);
      const bTs = Date.parse(b.at);
      if (Number.isFinite(aTs) && Number.isFinite(bTs)) return bTs - aTs;
      return 0;
    });
  }, [activeMemberRecord]);

  const groupedHuddlePlays = useMemo(() => {
    if (!isNetworkMarketingModule) {
      return [] as Array<{ day: string; records: NetworkRecord[] }>;
    }
    const huddle = (records as NetworkRecord[]).filter(
      (record) => String(record.kind) === "huddle_play",
    );
    const byDay = new Map<string, NetworkRecord[]>();
    for (const record of huddle) {
      const day = resolveHuddleDay(record);
      const list = byDay.get(day) ?? [];
      list.push(record);
      byDay.set(day, list);
    }
    const parsedTime = (value: string) => {
      const ts = Date.parse(value);
      return Number.isFinite(ts) ? ts : 0;
    };
    const ordered = [...byDay.entries()].sort((a, b) => {
      const aNewest = Math.max(...a[1].map((record) => parsedTime(record.updatedAt)));
      const bNewest = Math.max(...b[1].map((record) => parsedTime(record.updatedAt)));
      return bNewest - aNewest;
    });
    return ordered.map(([day, dayRecords]) => ({
      day,
      records: [...dayRecords].sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
    }));
  }, [module.id, records]);

  const followUpQuickGlance = useMemo(() => {
    if (!isNetworkMarketingModule || networkMarketingViewMode !== "pipeline") {
      return [] as Array<{ record: NetworkRecord; dueDate: string; dueState: "overdue" | "today" | "upcoming" }>;
    }
    const today = new Date().toISOString().slice(0, 10);
    const list = (records as NetworkRecord[])
      .filter((record) => String(record.kind) !== "huddle_play" && String(record.kind) !== "team_member")
      .filter((record) => !record.followUpCompleted)
      .map((record) => ({ record, dueDate: (record.followUpDate || "").trim() }))
      .filter((item) => Boolean(item.dueDate))
      .map((item) => ({
        ...item,
        dueState:
          item.dueDate < today ? ("overdue" as const) : item.dueDate === today ? ("today" as const) : ("upcoming" as const),
      }))
      .sort((a, b) => a.dueDate.localeCompare(b.dueDate));
    return list;
  }, [isNetworkMarketingModule, networkMarketingViewMode, records]);

  const coldContactRecords = useMemo(() => {
    if (!isNetworkMarketingModule) return [] as NetworkRecord[];
    const filtered = (records as NetworkRecord[]).filter(
      (record) => String(record.kind) === "cold_contact",
    );
    return [...filtered].sort((a, b) => {
      const scoreDiff = toColdContactScore(b) - toColdContactScore(a);
      if (scoreDiff !== 0) return scoreDiff;
      const aTs = Date.parse(a.updatedAt || "");
      const bTs = Date.parse(b.updatedAt || "");
      if (Number.isFinite(aTs) && Number.isFinite(bTs) && aTs !== bTs) return bTs - aTs;
      return (a.title || "").localeCompare(b.title || "");
    });
  }, [isNetworkMarketingModule, records]);

  const budgetTransactions = useMemo(() => {
    if (module.id !== "finance") return [] as FinanceRecord[];
    return (records as FinanceRecord[]).filter((record) => record.kind === "budget_txn");
  }, [module.id, records]);

  const budgetStatements = useMemo(() => {
    if (module.id !== "finance") return [] as FinanceRecord[];
    return (records as FinanceRecord[]).filter((record) => record.kind === "budget_statement");
  }, [module.id, records]);

  const budgetRules = useMemo(() => {
    if (module.id !== "finance") return [] as FinanceRecord[];
    return (records as FinanceRecord[]).filter((record) => record.kind === "budget_rule");
  }, [module.id, records]);

  const budgetGroupedByCategory = useMemo(() => {
    if (module.id !== "finance") {
      return [] as Array<{ category: string; records: FinanceRecord[]; total: number }>;
    }
    return budgetCategoryOptions.map((category) => {
      const groupRecords = budgetTransactions.filter((record) => {
        const recordCategory =
          (record.category || record.stage || "Uncategorized").trim() || "Uncategorized";
        return recordCategory === category;
      });
      const total = groupRecords.reduce(
        (sum, record) => sum + (typeof record.amount === "number" ? record.amount : 0),
        0,
      );
      return {
        category,
        records: groupRecords,
        total,
      };
    });
  }, [budgetTransactions, module.id]);

  const renderTreeNode = (node: TreeNode) => (
    <div key={node.record.id} className="min-w-[220px] sm:min-w-[260px]">
      <Card className="border border-slate-200 bg-white shadow-sm transition hover:shadow-md">
        <CardContent className="space-y-3 p-3 sm:p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="text-[11px] font-medium uppercase tracking-wide text-slate-500">Team member</p>
              <button
                type="button"
                className="mt-1 line-clamp-2 text-left text-sm font-semibold text-slate-900 hover:text-blue-700 hover:underline sm:text-base"
                onClick={() => openMemberModal(node.record)}
                title="Open member updates"
              >
                {node.record.displayName || node.record.title}
              </button>
              <p className="mt-1 text-xs text-slate-500">Depth {node.depth}</p>
            </div>
            <span className="shrink-0 rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] font-medium uppercase tracking-wide text-slate-600">
              {node.record.kind}
            </span>
          </div>
          <div className="rounded-lg border border-slate-100 bg-slate-50/70 p-2">
            <label className="mb-1 block text-[11px] font-medium uppercase tracking-wide text-slate-500">
              Parent link
            </label>
            <div className="flex flex-wrap items-center gap-2">
              <select
                value={node.record.parentMemberId ?? "root"}
                onChange={(event) => {
                  const nextParent = event.target.value === "root" ? null : event.target.value;
                  const hasCycle = nextParent === node.record.id;
                  if (hasCycle) return;
                  void onUpdateRecord(node.record.id, {
                    data: {
                      parent_member_id: nextParent,
                      direct_to_me: nextParent === null,
                    },
                  });
                }}
                className="h-8 min-w-[140px] rounded-lg border border-slate-300 bg-white px-2 text-xs"
              >
                <option value="root">Linked to me</option>
                {teamMemberRecords
                  .filter((member) => member.id !== node.record.id)
                  .map((member) => (
                    <option key={member.id} value={member.id}>
                      {member.displayName || member.title}
                    </option>
                  ))}
              </select>
              <button
                type="button"
                className="rounded-md border border-rose-200 px-2 py-1 text-xs font-medium text-rose-700 hover:bg-rose-50"
                onClick={() => void onDelete(node.record.id)}
              >
                Delete
              </button>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              className="rounded-md border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs font-medium text-blue-700 hover:bg-blue-100"
              onClick={() => openMemberModal(node.record)}
            >
              Open updates
            </button>
            <button
              type="button"
              className="rounded-md border border-slate-200 px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
              onClick={() => void onUpdateTeamMember(node.record)}
            >
              Quick edit
            </button>
          </div>
        </CardContent>
      </Card>
    </div>
  );

  const renderTreeSubtree = (node: TreeNode) => {
    return (
      <div key={`subtree-${node.record.id}`} className="inline-flex min-w-[220px] shrink-0 flex-col items-center sm:min-w-[240px]">
        {renderTreeNode(node)}
        {node.children.length > 0 ? (
          <div className="mt-3 inline-flex flex-col items-center">
            <div className="h-4 w-px bg-slate-400" />
            <div className="h-px w-10 bg-slate-400" />
            <div className="mt-3 flex flex-nowrap items-start gap-2.5 sm:gap-4">
              {node.children.map((child) => renderTreeSubtree(child))}
            </div>
          </div>
        ) : null}
      </div>
    );
  };

  return (
    <div
      className={
        isBudgetModule
          ? "grid gap-4 overflow-x-hidden sm:gap-6"
          : isEventsModule
            ? "grid gap-4 overflow-x-hidden sm:gap-6 lg:grid-cols-[minmax(0,2.35fr)_minmax(320px,0.95fr)]"
            : "grid gap-4 overflow-x-hidden sm:gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(360px,1fr)]"
      }
    >
      <div className="order-2 min-w-0 space-y-4 sm:space-y-6 lg:order-1">
        <Card className="border border-slate-200 bg-white shadow-sm">
        <CardHeader>
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="text-lg font-semibold text-slate-900">
              {module.title} {isNetworkMarketingModule
                ? networkMarketingViewMode === "team_tree"
                  ? "Team Tree"
                  : networkMarketingViewMode === "cold_contact"
                    ? "Cold Contact"
                    : "Pipeline"
                : "Pipeline"}
            </h2>
            {isProspectingPipelineModule ? (
              <span className="inline-flex items-center rounded-full border border-slate-300 bg-slate-100 px-2.5 py-0.5 text-xs font-semibold text-slate-700">
                {prospectingRecords.length} {prospectingRecords.length === 1 ? "record" : "records"}
              </span>
            ) : null}
          </div>
          <p className="mt-1 text-sm text-slate-500">{module.description}</p>
          {isNetworkMarketingModule ? (
            <div
              className="mt-3 inline-flex rounded-lg border border-slate-300 p-1"
              role="tablist"
              aria-label="Network Marketing view mode"
            >
              {([
                { id: "pipeline", label: "Pipeline" },
                { id: "cold_contact", label: "Cold Contact" },
                { id: "team_tree", label: "Team Tree" },
              ] as const).map((option) => {
                const isActive = networkMarketingViewMode === option.id;
                return (
                  <button
                    key={option.id}
                    type="button"
                    role="tab"
                    aria-selected={isActive}
                    className={`rounded-md px-3 py-1.5 text-sm transition ${
                      isActive
                        ? "bg-slate-900 text-white"
                        : "text-slate-600 hover:bg-slate-100"
                    }`}
                    onClick={() => void onNetworkMarketingViewModeChange(option.id)}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>
          ) : null}
        </CardHeader>
        <CardContent>
          {module.id === "finance" ? (
            <div className="space-y-4">
              <div className="rounded-2xl border border-slate-200 bg-gradient-to-br from-white via-slate-50 to-blue-50 p-4">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <h4 className="text-sm font-semibold text-slate-900">Import Bank Statement</h4>
                  <div className="flex items-center gap-2">
                    <Button
                      type="button"
                      variant="secondary"
                      onClick={() => void onRefreshBudget()}
                      disabled={budgetRefreshBusy || budgetResetBusy}
                      className="h-8 px-3 text-xs"
                    >
                      {budgetRefreshBusy ? "Refreshing..." : "Refresh"}
                    </Button>
                    <Button
                      type="button"
                      variant="secondary"
                      onClick={() => void onResetBudget()}
                      disabled={budgetRefreshBusy || budgetResetBusy}
                      className="h-8 border-rose-300 px-3 text-xs text-rose-700 hover:bg-rose-50"
                    >
                      {budgetResetBusy ? "Resetting..." : "Reset Budget"}
                    </Button>
                  </div>
                </div>
                <p className="mt-1 text-xs text-slate-600">
                  Upload CSV or PDF statement(s). Transactions are auto-categorized. Unknown items go to
                  Uncategorized.
                </p>
                <div className="mt-3 flex flex-wrap items-center gap-2">
                  <input
                    type="file"
                    accept=".csv,.pdf,text/csv,application/pdf,text/plain"
                    onChange={(event) => setBudgetImportFile(event.target.files?.[0] ?? null)}
                    className="max-w-[340px] text-xs"
                  />
                  <Button
                    type="button"
                    onClick={() => void onImportBudgetStatement()}
                    disabled={!budgetImportFile || budgetImportBusy}
                    className="h-8 px-3 text-xs"
                  >
                    {budgetImportBusy ? "Importing..." : "Import statement"}
                  </Button>
                </div>
                {budgetImportMessage ? (
                  <p className="mt-2 text-xs text-emerald-700">{budgetImportMessage}</p>
                ) : null}
                {error ? (
                  <p className="mt-2 text-xs text-rose-700">{error}</p>
                ) : null}
                <div className="mt-3 grid gap-2 text-xs text-slate-600 md:grid-cols-3">
                  <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 shadow-sm">
                    Statements: {budgetStatements.length}
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 shadow-sm">
                    Transactions: {budgetTransactions.length}
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 shadow-sm">
                    Uncategorized: {
                      budgetTransactions.filter(
                        (record) =>
                          record.uncategorized ||
                          (record.category || record.stage || "") === "Uncategorized",
                      ).length
                    }
                  </div>
                </div>
              </div>

              <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
                <h4 className="text-sm font-semibold text-slate-900">Category Rules</h4>
                <p className="mt-1 text-xs text-slate-600">
                  Add keyword rules so future imports are auto-classified into your categories.
                </p>
                <div className="mt-3 grid gap-2 md:grid-cols-4">
                  <Input
                    value={budgetRuleKeyword}
                    onChange={(event) => setBudgetRuleKeyword(event.target.value)}
                    placeholder="Keyword (e.g. woolworths)"
                  />
                  <select
                    value={budgetRuleCategory}
                    onChange={(event) => setBudgetRuleCategory(event.target.value)}
                    className="h-10 rounded-xl border border-slate-300 bg-white px-3 text-sm"
                  >
                    {budgetCategoryOptions.map((category) => (
                      <option key={category} value={category}>
                        {category}
                      </option>
                    ))}
                  </select>
                  <Input
                    value={budgetRuleSubcategory}
                    onChange={(event) => setBudgetRuleSubcategory(event.target.value)}
                    placeholder="Subcategory (e.g. Grocery Shopping)"
                  />
                  <Button
                    type="button"
                    onClick={() => void onSaveBudgetRule()}
                    disabled={
                      !budgetRuleKeyword.trim() ||
                      !budgetRuleCategory.trim() ||
                      !budgetRuleSubcategory.trim()
                    }
                  >
                    Save rule
                  </Button>
                </div>
                <p className="mt-2 text-xs text-slate-600">Saved rules: {budgetRules.length}</p>
              </div>

              <div className="overflow-x-auto pb-2">
                <div className="grid min-w-[1800px] grid-cols-9 gap-3">
                  {budgetGroupedByCategory.map((group) => (
                    <div
                      key={group.category}
                      className="rounded-xl border border-slate-200 bg-slate-50 p-3"
                    >
                      <div className="mb-3 flex items-center justify-between gap-2">
                        <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-700">
                          {group.category}
                        </h4>
                        <span className="rounded-full bg-white px-2 py-0.5 text-xs text-slate-500">
                          {group.records.length}
                        </span>
                      </div>
                      <p className="mb-2 text-xs text-slate-500">
                        Total {group.total.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </p>
                      <div className="space-y-2">
                        {group.records.length === 0 ? (
                          <div className="rounded-lg border border-dashed border-slate-300 bg-white/70 p-2 text-xs text-slate-400">
                            No transactions
                          </div>
                        ) : (
                          group.records.slice(0, 80).map((record) => (
                            <div key={record.id} className="rounded-lg border border-slate-200 bg-white p-3 shadow-sm">
                              <div className="mb-1 flex items-start justify-between gap-2">
                                <p className="text-sm font-medium text-slate-900">{record.title}</p>
                                <button
                                  type="button"
                                  className="inline-flex h-5 w-5 items-center justify-center rounded-full border border-slate-300 text-xs text-slate-500 hover:bg-rose-50 hover:text-rose-600"
                                  title="Remove card"
                                  onClick={() => void onDelete(record.id)}
                                >
                                  x
                                </button>
                              </div>
                              <p className="mt-1 text-xs text-slate-500">
                                {record.transactionDate || "-"} •{" "}
                                {(record.amount ?? 0).toLocaleString(undefined, {
                                  minimumFractionDigits: 2,
                                  maximumFractionDigits: 2,
                                })}
                              </p>
                              <div className="mt-2 flex items-center gap-2">
                                <select
                                  defaultValue={record.category || record.stage || "Uncategorized"}
                                  onChange={(event) =>
                                    void onRecategorizeBudgetTransaction(record, event.target.value)
                                  }
                                  className="h-8 w-full rounded-lg border border-slate-300 bg-white px-2 text-xs"
                                >
                                  {budgetCategoryOptions.map((category) => (
                                    <option key={category} value={category}>
                                      {category}
                                    </option>
                                  ))}
                                </select>
                              </div>
                            </div>
                          ))
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : isNetworkMarketingModule && networkMarketingViewMode === "pipeline" ? (
            <>
              <div className="space-y-3 md:hidden">
                <div className="rounded-xl border border-slate-200 bg-white p-3 shadow-sm">
                  <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Pipeline stages</p>
                  <div className="mt-2 grid grid-cols-2 gap-2">
                    {networkColumns.map((column) => (
                      <button
                        key={column.id}
                        type="button"
                        onClick={() => setMobilePipelineStage(column.id)}
                        className={`rounded-lg border px-2 py-2 text-xs font-semibold ${
                          activeMobilePipelineColumn?.id === column.id
                            ? "border-slate-900 bg-slate-900 text-white"
                            : "border-slate-200 bg-slate-50 text-slate-700"
                        }`}
                      >
                        <span className="block truncate">{column.title}</span>
                        <span className="block text-[11px] opacity-80">{column.records.length} leads</span>
                      </button>
                    ))}
                  </div>
                </div>

                <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <p className="text-sm font-semibold text-slate-900">
                      {activeMobilePipelineColumn?.title || "Stage"}
                    </p>
                    <span className="rounded-full bg-white px-2 py-0.5 text-xs text-slate-600">
                      {activeMobilePipelineColumn?.records.length ?? 0}
                    </span>
                  </div>
                  <div className="space-y-2">
                    {activeMobilePipelineColumn?.records.length ? (
                      [...activeMobilePipelineColumn.records]
                        .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))
                        .map((record) => (
                          <div key={record.id} className="rounded-lg border border-slate-200 bg-white p-3 shadow-sm">
                            <button
                              type="button"
                              className="w-full text-left text-sm font-semibold text-slate-900 hover:text-blue-700"
                              onClick={() => openPipelineModal(record as NetworkRecord)}
                            >
                              {record.title}
                            </button>
                            {"nextStep" in record && String(record.nextStep || "") ? (
                              <p className="mt-1 line-clamp-2 text-xs text-slate-600">
                                Next: {String(record.nextStep || "")}
                              </p>
                            ) : (
                              <p className="mt-1 text-xs text-slate-500">No next step set</p>
                            )}
                          </div>
                        ))
                    ) : (
                      <div className="rounded-lg border border-dashed border-slate-300 bg-white/70 p-3 text-xs text-slate-500">
                        No records in this stage.
                      </div>
                    )}
                  </div>
                </div>
              </div>

              <div className="hidden overflow-x-auto pb-2 md:block">
                <div
                  className="grid gap-3"
                  style={{
                    minWidth: `${Math.max(1700, networkColumns.length * 185)}px`,
                    gridTemplateColumns: `repeat(${networkColumns.length}, minmax(0, 1fr))`,
                  }}
                >
                  {networkColumns.map((column) => (
                    <div
                      key={column.id}
                      className="rounded-xl border border-slate-200 bg-slate-50 p-3"
                      onDragOver={(event) => event.preventDefault()}
                      onDrop={(event) => {
                        event.preventDefault();
                        const recordId = event.dataTransfer.getData("text/plain");
                        setDraggingRecordId(null);
                        if (recordId) {
                          void moveRecordToStage(recordId, column.id);
                        }
                      }}
                    >
                      <div className="mb-3 flex items-center justify-between">
                        <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                          {column.title}
                        </h4>
                        <span className="rounded-full bg-white px-2 py-0.5 text-xs text-slate-500">
                          {column.records.length}
                        </span>
                      </div>
                      <div
                        className={`space-y-2 ${
                          column.records.length > 5
                            ? "max-h-[62vh] overflow-y-auto pr-1"
                            : ""
                        }`}
                      >
                        {column.records.length === 0 ? (
                          <div className="rounded-lg border border-dashed border-slate-300 bg-white/70 p-2 text-xs text-slate-400">
                            Drop card here
                          </div>
                        ) : (
                          column.records.map((record) => (
                            <div
                              key={record.id}
                              draggable
                              onDragStart={(event) => {
                                event.dataTransfer.setData("text/plain", record.id);
                                setDraggingRecordId(record.id);
                              }}
                              onDragEnd={() => setDraggingRecordId(null)}
                              className={`rounded-lg border bg-white p-3 shadow-sm ${
                                draggingRecordId === record.id
                                  ? "border-blue-300 opacity-70"
                                  : "border-slate-200"
                              }`}
                            >
                              <button
                                type="button"
                                className="text-left text-sm font-semibold text-slate-900 hover:text-blue-700"
                                onClick={() => openPipelineModal(record as NetworkRecord)}
                              >
                                {record.title}
                              </button>
                              {getInstagramProfileUrl(record as NetworkRecord) ? (
                                <a
                                  href={getInstagramProfileUrl(record as NetworkRecord)}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  aria-label="Open Instagram profile"
                                  title="Open Instagram profile"
                                  className="mt-1 inline-flex h-8 w-8 items-center justify-center rounded-full border border-pink-200 bg-pink-50 text-pink-700 hover:bg-pink-100 hover:text-pink-800"
                                >
                                  <Instagram className="h-4 w-4" />
                                </a>
                              ) : (
                                <p className="mt-1 text-xs text-slate-500">{record.summary || "-"}</p>
                              )}
                              {"nextStep" in record && String(record.nextStep || "") ? (
                                <p className="mt-2 text-xs text-slate-600">
                                  Next: {String(record.nextStep || "")}
                                </p>
                              ) : null}
                              {"movedToTreeAt" in record && record.movedToTreeAt ? (
                                <p className="mt-2 text-xs text-emerald-700">
                                  Added to Team Tree
                                </p>
                              ) : null}
                              <div className="mt-3 flex items-center gap-3">
                                <button
                                  type="button"
                                  className="text-xs text-emerald-700 hover:text-emerald-800 disabled:text-slate-400"
                                  onClick={() => void onPromotePipelineToTeamTree(record as NetworkRecord)}
                                  disabled={"movedToTreeAt" in record && Boolean(record.movedToTreeAt)}
                                >
                                  {"movedToTreeAt" in record && record.movedToTreeAt
                                    ? "In Team Tree"
                                    : "Add to Team Tree"}
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-blue-700 hover:text-blue-800"
                                  onClick={() => void onPromote(record.id)}
                                >
                                  Task
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-rose-600 hover:text-rose-700"
                                  onClick={() => void onDelete(record.id)}
                                >
                                  Delete
                                </button>
                              </div>
                            </div>
                          ))
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          ) : isNetworkMarketingModule && networkMarketingViewMode === "cold_contact" ? (
            <div className="space-y-4">
              <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <h4 className="text-sm font-semibold text-slate-900">Prospect Intel Engine Blueprint</h4>
                  <Button
                    type="button"
                    variant="secondary"
                    className="h-8 px-3 text-xs"
                    onClick={() => void onRunColdContactPipelineNow()}
                    disabled={coldContactPipelineBusy}
                  >
                    {coldContactPipelineBusy ? "Running..." : "Run helper now"}
                  </Button>
                </div>
                <p className="mt-1 text-xs text-slate-600">
                  Openclaw automation flow: ingest prospects, research intent signals, score fit, and hand off ranked leads for manual outreach.
                </p>
                {coldContactPipelineMessage ? (
                  <div className="mt-3 whitespace-pre-line rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-xs text-emerald-800">
                    {coldContactPipelineMessage}
                  </div>
                ) : null}
                <div className="mt-3 grid gap-2 text-xs text-slate-700 md:grid-cols-2">
                  <div className="rounded-lg border border-slate-200 bg-white p-2.5">1) ICP + disqualifier filters</div>
                  <div className="rounded-lg border border-slate-200 bg-white p-2.5">2) Multi-source lead ingestion</div>
                  <div className="rounded-lg border border-slate-200 bg-white p-2.5">3) Profile enrichment + intent extraction</div>
                  <div className="rounded-lg border border-slate-200 bg-white p-2.5">4) Why Fit + Why Now scoring</div>
                  <div className="rounded-lg border border-slate-200 bg-white p-2.5">5) Prioritized queue (hot / warm / watch)</div>
                  <div className="rounded-lg border border-slate-200 bg-white p-2.5">6) Human outreach with tracked outcomes</div>
                </div>
              </div>
              <div className="grid gap-4 lg:grid-cols-2">
                <div className="rounded-xl border border-slate-200 bg-white p-4">
                  <div className="mb-3 flex items-center justify-between gap-2">
                    <h5 className="text-sm font-semibold text-slate-900">Priority Queue</h5>
                    <Button
                      type="button"
                      variant="secondary"
                      className="h-8 px-3 text-xs"
                      onClick={() => void refreshColdContactQueue()}
                      disabled={coldContactQueueBusy}
                    >
                      {coldContactQueueBusy ? "Refreshing..." : "Refresh queue"}
                    </Button>
                  </div>
                  {coldContactQueueItems.length === 0 ? (
                    <p className="text-xs text-slate-500">No queued prospects yet.</p>
                  ) : (
                    <div className="space-y-2">
                      {coldContactQueueItems.slice(0, 8).map((item) => (
                        <div key={item.prospect_id} className="rounded-lg border border-slate-200 bg-slate-50 p-2.5">
                          <div className="flex items-center justify-between gap-2">
                            <p className="text-sm font-medium text-slate-900">{item.name}</p>
                            <span className="rounded-full bg-white px-2 py-0.5 text-[11px] font-semibold text-slate-700">
                              {item.band} · {item.score_total}
                            </span>
                          </div>
                          <p className="mt-1 text-xs text-slate-600">{item.platform} · {item.stage}</p>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <div className="rounded-xl border border-slate-200 bg-white p-4">
                  <div className="mb-3 flex items-center justify-between gap-2">
                    <h5 className="text-sm font-semibold text-slate-900">Follow-up Tasks</h5>
                    <div className="flex items-center gap-2">
                      <Button
                        type="button"
                        variant="secondary"
                        className="h-8 px-3 text-xs"
                        onClick={() => void recomputeColdContactFollowups()}
                        disabled={coldContactFollowupBusy}
                      >
                        {coldContactFollowupBusy ? "Running..." : "Recompute"}
                      </Button>
                      <Button
                        type="button"
                        variant="secondary"
                        className="h-8 px-3 text-xs"
                        onClick={() => void refreshColdContactFollowups()}
                        disabled={coldContactFollowupBusy}
                      >
                        {coldContactFollowupBusy ? "Refreshing..." : "Refresh"}
                      </Button>
                    </div>
                  </div>
                  {coldContactFollowupItems.length === 0 ? (
                    <p className="text-xs text-slate-500">No due follow-up tasks.</p>
                  ) : (
                    <div className="space-y-2">
                      {coldContactFollowupItems.slice(0, 8).map((task) => (
                        <div key={task.task_id} className="rounded-lg border border-slate-200 bg-slate-50 p-2.5">
                          <p className="text-sm font-medium text-slate-900">{task.prospect_name || "Prospect"}</p>
                          <p className="mt-1 text-xs text-slate-600">
                            {task.task_type} · {task.stage}
                          </p>
                          {task.recommendation ? (
                            <p className="mt-1 text-xs text-slate-500">{task.recommendation}</p>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
              {coldContactRecords.length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-400 bg-slate-50 p-4 text-sm text-slate-700">
                  No cold contact leads yet. Add prospects from the right panel to build your ranked outreach queue.
                </div>
              ) : (
                <div className="grid gap-3 md:grid-cols-2">
                  {coldContactRecords.map((record) => {
                    const score = toColdContactScore(record);
                    const platform = (record.coldContactPlatform || "unknown").trim() || "unknown";
                    const confidence = (record.coldContactConfidence || "medium").trim() || "medium";
                    const whyFit = (record.coldContactWhyFit || "").trim();
                    const whyNow = (record.coldContactWhyNow || "").trim();
                    const signals = record.coldContactSignals ?? [];
                    return (
                      <div key={record.id} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                        <div className="flex flex-wrap items-start justify-between gap-2">
                          <div>
                            <p className="text-base font-semibold text-slate-900">{record.title}</p>
                            <p className="mt-1 text-xs uppercase tracking-wide text-slate-500">
                              {platform} • confidence {confidence}
                            </p>
                          </div>
                          <span className="rounded-full border border-slate-300 bg-slate-100 px-2.5 py-0.5 text-xs font-semibold text-slate-700">
                            Fit {score}/100
                          </span>
                        </div>
                        {record.coldContactProfileUrl ? (
                          <a
                            href={record.coldContactProfileUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="mt-2 inline-flex text-xs font-medium text-blue-700 hover:text-blue-800"
                          >
                            Open profile
                          </a>
                        ) : null}
                        {whyFit ? (
                          <div className="mt-3">
                            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Why fit</p>
                            <p className="mt-1 whitespace-pre-wrap text-sm text-slate-800">{whyFit}</p>
                          </div>
                        ) : null}
                        {whyNow ? (
                          <div className="mt-3">
                            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Why now</p>
                            <p className="mt-1 whitespace-pre-wrap text-sm text-slate-800">{whyNow}</p>
                          </div>
                        ) : null}
                        {signals.length > 0 ? (
                          <div className="mt-3">
                            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Signals</p>
                            <ul className="mt-1 list-disc space-y-1 pl-5 text-sm text-slate-800">
                              {signals.map((signal, idx) => (
                                <li key={`${record.id}-signal-${idx}`}>{signal}</li>
                              ))}
                            </ul>
                          </div>
                        ) : null}
                        {record.coldContactAngle ? (
                          <p className="mt-3 text-xs text-slate-600">
                            Angle: {record.coldContactAngle}
                          </p>
                        ) : null}
                        <div className="mt-3 flex items-center gap-3">
                          <button
                            type="button"
                            className="text-xs text-blue-700 hover:text-blue-800"
                            onClick={() => void onPromote(record.id)}
                          >
                            Task
                          </button>
                          <button
                            type="button"
                            className="text-xs text-rose-600 hover:text-rose-700"
                            onClick={() => void onDelete(record.id)}
                          >
                            Delete
                          </button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          ) : isNetworkMarketingModule ? (
            <div className="overflow-auto pb-2">
              <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                <p className="text-[11px] text-slate-500 sm:hidden">
                  Swipe left/right to navigate the full team tree.
                </p>
                <div className="ml-auto inline-flex items-center gap-1 rounded-lg border border-slate-300 bg-white p-1">
                  <button
                    type="button"
                    className="h-7 w-7 rounded-md border border-slate-200 text-sm text-slate-700 hover:bg-slate-100"
                    onClick={() => setTeamTreeZoom((prev) => Math.max(0.7, Number((prev - 0.1).toFixed(2))))}
                    title="Zoom out"
                  >
                    -
                  </button>
                  <button
                    type="button"
                    className="h-7 rounded-md border border-slate-200 px-2 text-[11px] text-slate-700 hover:bg-slate-100"
                    onClick={() => setTeamTreeZoom(1)}
                    title="Reset zoom"
                  >
                    {Math.round(teamTreeZoom * 100)}%
                  </button>
                  <button
                    type="button"
                    className="h-7 w-7 rounded-md border border-slate-200 text-sm text-slate-700 hover:bg-slate-100"
                    onClick={() => setTeamTreeZoom((prev) => Math.min(1.8, Number((prev + 0.1).toFixed(2))))}
                    title="Zoom in"
                  >
                    +
                  </button>
                </div>
              </div>
              <div
                className="rounded-xl border border-slate-200 bg-slate-50 p-2.5 sm:p-4"
                style={{ minWidth: `${treeCanvasMinWidth * teamTreeZoom}px` }}
                onTouchStart={(event) => {
                  if (event.touches.length === 2) {
                    pinchStartDistanceRef.current = getTouchDistance(event.touches);
                    pinchStartZoomRef.current = teamTreeZoom;
                  }
                }}
                onTouchMove={(event) => {
                  if (event.touches.length !== 2 || !pinchStartDistanceRef.current) return;
                  event.preventDefault();
                  const distance = getTouchDistance(event.touches);
                  if (!distance) return;
                  const zoom = (pinchStartZoomRef.current * distance) / pinchStartDistanceRef.current;
                  setTeamTreeZoom(Math.max(0.7, Math.min(1.8, Number(zoom.toFixed(2)))));
                }}
                onTouchEnd={(event) => {
                  if (event.touches.length < 2) {
                    pinchStartDistanceRef.current = null;
                  }
                }}
                onTouchCancel={() => {
                  pinchStartDistanceRef.current = null;
                }}
              >
                {networkTree.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-slate-300 bg-white p-4 text-sm text-slate-500">
                    No team members found yet. Add members to render the Team Tree.
                  </div>
                ) : (
                  <div
                    style={{
                      transform: `scale(${teamTreeZoom})`,
                      transformOrigin: "top left",
                      width: `${100 / teamTreeZoom}%`,
                    }}
                  >
                    <div className="flex flex-nowrap items-start justify-center gap-3 sm:gap-8">
                      {networkTree.map((root) => renderTreeSubtree(root))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          ) : isEventsModule ? (
            <div className="space-y-4">
              <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                  <h4 className="text-sm font-semibold text-slate-900">Weekly Event Feed</h4>
                  <div className="flex flex-wrap items-center gap-2">
                    <Button
                      type="button"
                      variant="secondary"
                      className="h-8 px-3 text-xs"
                      onClick={exportEventsWeekCsv}
                      disabled={filteredEventRecords.length === 0}
                    >
                      Export CSV
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      className="h-8 border-rose-200 px-3 text-xs text-rose-700 hover:bg-rose-50 hover:text-rose-800"
                      onClick={() => void removeAllEvents()}
                      disabled={eventRecords.length === 0 || eventsBulkDeleteBusy}
                    >
                      {eventsBulkDeleteBusy ? "Removing..." : "Remove all events"}
                    </Button>
                  </div>
                </div>
                <div className="mb-3 grid gap-2 md:grid-cols-2 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,0.95fr)_minmax(0,0.95fr)_minmax(0,1.05fr)_minmax(0,1fr)_minmax(0,1.1fr)]">
                  <select
                    value={eventCostFilter}
                    onChange={(event) =>
                      setEventCostFilter(event.target.value as "all" | "free" | "paid")
                    }
                    className="h-9 rounded-xl border border-slate-300 bg-white px-3 text-sm"
                  >
                    <option value="all">Free / Cost: All</option>
                    <option value="free">Free only</option>
                    <option value="paid">Paid only</option>
                  </select>
                  <Input
                    type="date"
                    value={eventDateFromFilter}
                    onChange={(event) => setEventDateFromFilter(event.target.value)}
                    placeholder="Date from"
                  />
                  <Input
                    type="date"
                    value={eventDateToFilter}
                    onChange={(event) => setEventDateToFilter(event.target.value)}
                    placeholder="Date to"
                  />
                  <select
                    value={eventTypeFilter}
                    onChange={(event) => setEventTypeFilter(event.target.value)}
                    className="h-9 rounded-xl border border-slate-300 bg-white px-3 text-sm"
                  >
                    <option value="all">Type of Event: All</option>
                    {eventTypeOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                  <select
                    value={eventPriceSort}
                    onChange={(event) =>
                      setEventPriceSort(event.target.value as "none" | "asc" | "desc")
                    }
                    className="h-9 rounded-xl border border-slate-300 bg-white px-3 text-sm"
                  >
                    <option value="none">Price: Default order</option>
                    <option value="asc">Price: Lowest → Highest</option>
                    <option value="desc">Price: Highest → Lowest</option>
                  </select>
                  <Input
                    value={eventReferenceAddress}
                    onChange={(event) => setEventReferenceAddress(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        event.preventDefault();
                        void sortEventsByNearest();
                      }
                    }}
                    placeholder="Address for nearest-first sort"
                  />
                </div>
                <div className="mb-3 flex flex-wrap items-center gap-2">
                  <Button
                    type="button"
                    variant="secondary"
                    className="h-8 px-3 text-xs"
                    onClick={() => void sortEventsByNearest()}
                    disabled={eventDistanceBusy || filteredEventRecords.length === 0}
                  >
                    {eventDistanceBusy ? "Calculating..." : "Sort nearest → furthest"}
                  </Button>
                  {eventDistanceError ? (
                    <span className="text-xs text-rose-600">{eventDistanceError}</span>
                  ) : eventReferenceAddress.trim() ? (
                    <span className="text-xs text-slate-500">
                      Showing nearest-first where distance is available.
                    </span>
                  ) : null}
                </div>
                {visibleEventRecords.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-slate-300 bg-white p-4 text-sm text-slate-500">
                    No events match the current filter.
                  </div>
                ) : (
                  <div className="space-y-4">
                    {visibleEventRecords.map((record, index) => {
                      const stage = normalizeEventStage(record.stage);
                      const stageLabel = eventStageLabelMap[stage] ?? "Just added";
                      const eventType = deriveEventType(record);
                      const viewUrl = resolveEventViewUrl(record);
                      const timeMeta = eventTimeConfidenceMeta(getEventTimeConfidence(record));
                      const distanceKm = eventDistanceKmByRecord[record.id];
                      const priceLabel = record.isFree
                        ? "Free"
                        : record.price
                          ? `From ${record.currency ? `${record.currency} ` : "$"}${record.price}`
                          : "Price TBC";
                      const sourceLabel = (record.sourceName || "").trim() || "Imported";
                      const locationLabel = [record.city, record.venue]
                        .filter((value) => String(value || "").trim().length > 0)
                        .join(" · ");
                      const dayHeader = formatEventDayHeader(record.startAt, record.sourceName);
                      const prev = index > 0 ? visibleEventRecords[index - 1] : null;
                      const prevDayHeader = prev ? formatEventDayHeader(prev.startAt, prev.sourceName) : null;
                      const showDayHeader = !hasCustomEventSort && dayHeader !== prevDayHeader;
                      return (
                        <div key={record.id} className="space-y-3">
                          {showDayHeader ? (
                            <div className="sticky top-0 z-10 rounded-lg border border-slate-200 bg-slate-100/95 px-3 py-2 text-sm font-semibold text-slate-700 backdrop-blur">
                              {dayHeader}
                            </div>
                          ) : null}
                          <div className="rounded-xl border border-slate-200 bg-white p-3 shadow-sm sm:p-4">
                            <div className="flex flex-col gap-4 md:flex-row md:items-start">
                              <button
                                type="button"
                                onClick={() => openEventModal(record)}
                                className="h-36 w-full shrink-0 overflow-hidden rounded-lg bg-slate-100 md:h-40 md:w-64"
                                title="Open event"
                              >
                                {record.imageUrl ? (
                                  <img
                                    src={record.imageUrl}
                                    alt={record.title}
                                    className="h-full w-full object-cover"
                                    loading="lazy"
                                  />
                                ) : (
                                  <div className="flex h-full w-full items-center justify-center text-sm text-slate-500">
                                    No image
                                  </div>
                                )}
                              </button>
                              <div className="min-w-0 flex-1">
                                <div className="mb-2 flex flex-wrap items-center gap-2">
                                  <span className="rounded-xl bg-violet-100 px-3 py-1 text-sm font-semibold text-violet-900">
                                    {stageLabel}
                                  </span>
                                  <span className="rounded-xl bg-slate-100 px-3 py-1 text-sm font-semibold text-slate-700">
                                    {eventType}
                                  </span>
                                  <span className={`rounded-xl px-3 py-1 text-sm font-semibold ${timeMeta.className}`}>
                                    {timeMeta.label}
                                  </span>
                                  {Number.isFinite(distanceKm) ? (
                                    <span className="rounded-xl bg-emerald-100 px-3 py-1 text-sm font-semibold text-emerald-800">
                                      {(distanceKm as number).toFixed(1)} km
                                    </span>
                                  ) : null}
                                  <span
                                    className="max-w-[210px] truncate text-xs text-slate-500"
                                    title={`Source: ${sourceLabel}`}
                                  >
                                    Source: {sourceLabel}
                                  </span>
                                </div>
                                <button
                                  type="button"
                                  className="text-left text-xl font-semibold leading-tight text-slate-900 hover:text-blue-700"
                                  onClick={() => openEventModal(record)}
                                >
                                  {record.title}
                                </button>
                                <p className="mt-2 text-lg text-slate-600">
                                  {formatEventListDate(record.startAt, record.sourceName)}
                                </p>
                                <p className="mt-1 text-lg text-slate-600">{locationLabel || "Location TBC"}</p>
                                {eventSummarySnippet(record) ? (
                                  <p className="mt-2 line-clamp-3 text-sm leading-relaxed text-slate-600">
                                    {eventSummarySnippet(record)}
                                  </p>
                                ) : null}
                                <p className="mt-3 text-xl font-semibold text-slate-900">{priceLabel}</p>
                                <div className="mt-3 flex flex-wrap items-center gap-3 text-sm">
                                  <button
                                    type="button"
                                    className="text-emerald-700 hover:text-emerald-800 disabled:text-slate-400"
                                    onClick={() => void addEventToCalendar(record)}
                                    disabled={eventCalendarBusyRecordId === record.id}
                                  >
                                    {eventCalendarBusyRecordId === record.id ? "Adding..." : "Calendar"}
                                  </button>
                                  <button
                                    type="button"
                                    className="text-blue-700 hover:text-blue-800"
                                    onClick={() => openEventModal(record)}
                                  >
                                    Open details
                                  </button>
                                  {viewUrl ? (
                                    <a
                                      href={viewUrl}
                                      target="_blank"
                                      rel="noreferrer"
                                      className="text-blue-700 hover:text-blue-800"
                                    >
                                      View
                                    </a>
                                  ) : null}
                                  <button
                                    type="button"
                                    className="text-rose-600 hover:text-rose-700"
                                    onClick={() => void onDelete(record.id)}
                                  >
                                    Delete
                                  </button>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          ) : isPodcastsModule ? (
            <div className="space-y-3">
              {records.length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-300 bg-white p-4 text-sm text-slate-500">
                  No podcast records yet. Upload your first MP3/M4A from the panel on the right.
                </div>
              ) : (
                podcastFolders.map((folder) => {
                  const collapsed = collapsedPodcastFolders[folder.key] ?? false;
                  return (
                  <div
                    key={folder.key}
                    className="rounded-2xl border border-slate-200 bg-white/95 shadow-[0_14px_30px_rgba(15,23,42,0.08)] backdrop-blur-sm"
                  >
                    <button
                      type="button"
                      className="flex w-full items-center justify-between gap-3 rounded-2xl px-4 py-3 text-left hover:bg-slate-50/80"
                      onClick={() =>
                        setCollapsedPodcastFolders((prev) => ({
                          ...prev,
                          [folder.key]: !collapsed,
                        }))
                      }
                    >
                      <div className="flex min-w-0 items-center gap-2">
                        <span className="text-sm">{collapsed ? "📁" : "📂"}</span>
                        <h4 className="truncate text-sm font-semibold text-slate-800">{folder.label}</h4>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-600">
                          {folder.items.length}
                        </span>
                        <span className="text-xs text-slate-500">{collapsed ? "Show" : "Hide"}</span>
                      </div>
                    </button>
                    {!collapsed ? (
                    <div className="space-y-3 border-t border-slate-100 px-3 pb-3 pt-3">
                      {folder.items.slice(0, 100).map((record) => {
                        const podcastRecord = record as PodcastRecord;
                        const isExpanded = expandedPodcastId === record.id;
                        const view = podcastViews[record.id];
                        return (
                          <div key={record.id} className="rounded-xl border border-slate-200 bg-white shadow-sm">
                            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-100 px-4 py-3">
                              <div>
                                <button
                                  type="button"
                                  className="text-left text-base font-semibold text-slate-900 hover:text-blue-700"
                                  onClick={() => void onTogglePodcastExpanded(record.id)}
                                >
                                  {record.title}
                                </button>
                                <p className="text-xs text-slate-500">
                                  {podcastRecord.source_filename || "-"} • {podcastRecord.source_format || "audio"}
                                </p>
                              </div>
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-700">
                                  transcript: {podcastRecord.transcript_status}
                                </span>
                                <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-700">
                                  summary: {podcastRecord.summary_status}
                                </span>
                                <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-700">
                                  actions: {podcastRecord.task_extraction_status}
                                </span>
                              </div>
                            </div>
                            <div className="px-4 py-3">
                              <div className="mb-3 flex flex-wrap items-center gap-2">
                                <button
                                  type="button"
                                  className="text-xs text-violet-700 hover:text-violet-800"
                                  onClick={() => void onSummarizePodcast(record.id)}
                                  disabled={podcastBusyRecordId === record.id}
                                >
                                  Summarize
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-cyan-700 hover:text-cyan-800"
                                  onClick={() => void onExtractPodcastActions(record.id)}
                                  disabled={podcastBusyRecordId === record.id}
                                >
                                  Extract actions
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-fuchsia-700 hover:text-fuchsia-800"
                                  onClick={() => void onClassifyPodcast(record.id)}
                                  disabled={podcastBusyRecordId === record.id}
                                >
                                  Classify folder
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-emerald-700 hover:text-emerald-800"
                                  onClick={() => void onRunPodcastPipeline(record.id)}
                                  disabled={podcastPipelineBusyRecordId === record.id}
                                >
                                  {podcastPipelineBusyRecordId === record.id ? "Running..." : "Run pipeline"}
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-blue-700 hover:text-blue-800"
                                  onClick={() => void onPromote(record.id)}
                                >
                                  Task
                                </button>
                                <button
                                  type="button"
                                  className="text-xs text-rose-600 hover:text-rose-700"
                                  onClick={() => void onDelete(record.id)}
                                >
                                  Delete
                                </button>
                              </div>

                              {isExpanded ? (
                                <div className="grid gap-3 lg:grid-cols-[minmax(0,1.6fr)_minmax(280px,1fr)]">
                                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                                    <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                                      Transcript
                                    </p>
                                    {podcastAudioLoadingId === record.id ? (
                                      <p className="mb-2 text-sm text-slate-500">Loading audio...</p>
                                    ) : podcastAudioUrls[record.id] ? (
                                      <audio
                                        controls
                                        src={podcastAudioUrls[record.id]}
                                        className="mb-3 w-full"
                                        onLoadedMetadata={(event) => {
                                          const audioEl = event.currentTarget;
                                          const duration =
                                            audioEl && Number.isFinite(audioEl.duration) ? audioEl.duration : 0;
                                          setPodcastDurationSeconds((prev) => ({
                                            ...prev,
                                            [record.id]: duration,
                                          }));
                                        }}
                                        onTimeUpdate={(event) => {
                                          const audioEl = event.currentTarget;
                                          const currentTime =
                                            audioEl && Number.isFinite(audioEl.currentTime)
                                              ? audioEl.currentTime
                                              : 0;
                                          setPodcastPlaybackSeconds((prev) => {
                                            const previousTime = prev[record.id] ?? 0;
                                            if (Math.abs(currentTime - previousTime) >= 2.5) {
                                              setPodcastPendingJumpScrollRecordId(record.id);
                                            }
                                            return {
                                              ...prev,
                                              [record.id]: currentTime,
                                            };
                                          });
                                        }}
                                        onSeeking={(event) => {
                                          const audioEl = event.currentTarget;
                                          const currentTime =
                                            audioEl && Number.isFinite(audioEl.currentTime)
                                              ? audioEl.currentTime
                                              : 0;
                                          setPodcastPendingJumpScrollRecordId(record.id);
                                          setPodcastPlaybackSeconds((prev) => ({
                                            ...prev,
                                            [record.id]: currentTime,
                                          }));
                                        }}
                                        onSeeked={(event) => {
                                          const audioEl = event.currentTarget;
                                          const currentTime =
                                            audioEl && Number.isFinite(audioEl.currentTime)
                                              ? audioEl.currentTime
                                              : 0;
                                          setPodcastPendingJumpScrollRecordId(record.id);
                                          setPodcastPlaybackSeconds((prev) => ({
                                            ...prev,
                                            [record.id]: currentTime,
                                          }));
                                        }}
                                      />
                                    ) : (
                                      <p className="mb-2 text-sm text-slate-500">
                                        Audio not available yet. Upload/transcribe audio first.
                                      </p>
                                    )}
                                    {podcastViewLoadingId === record.id ? (
                                      <p className="text-sm text-slate-500">Loading transcript...</p>
                                    ) : view?.transcript_text ? (
                                      (() => {
                                        const timedWordsFromPayload = parseTranscriptWordsPayload(
                                          view.transcript_words,
                                        );
                                        const timedWordsFromVtt = parseTimedTranscriptWords(view.transcript_vtt_text);
                                        const timedWords =
                                          timedWordsFromPayload.length > 0
                                            ? timedWordsFromPayload
                                            : timedWordsFromVtt.length > 0
                                              ? timedWordsFromVtt
                                            : buildApproxTimedTranscriptWords(
                                                view.transcript_text,
                                                podcastDurationSeconds[record.id] ?? 0,
                                              );
                                        const currentSecond = podcastPlaybackSeconds[record.id] ?? 0;
                                        if (timedWords.length > 0) {
                                          return (
                                            <div className="max-h-[380px] overflow-auto rounded-md border border-slate-200 bg-white p-3 text-sm leading-7 text-slate-800">
                                              {timedWords.map((word) => {
                                                const isActive =
                                                  currentSecond >= word.start && currentSecond < word.end;
                                                return (
                                                  <span
                                                    key={word.key}
                                                    data-podcast-word="true"
                                                    data-record-id={record.id}
                                                    data-active={isActive ? "true" : "false"}
                                                    className={
                                                      isActive
                                                        ? "rounded bg-amber-200 px-1 font-semibold text-slate-900"
                                                        : ""
                                                    }
                                                  >
                                                    {word.text}{" "}
                                                  </span>
                                                );
                                              })}
                                            </div>
                                          );
                                        }
                                        return (
                                          <pre className="max-h-[380px] whitespace-pre-wrap overflow-auto text-sm text-slate-800">
                                            {formatTranscriptForDisplay(view.transcript_text)}
                                          </pre>
                                        );
                                      })()
                                    ) : (
                                      <p className="text-sm text-slate-500">
                                        Transcript not available yet. Run transcription first.
                                      </p>
                                    )}
                                  </div>
                                  <div className="space-y-3">
                                    <div className="rounded-lg border border-slate-200 bg-white p-3">
                                      <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                                        Summary
                                      </p>
                                      {podcastViewLoadingId === record.id ? (
                                        <p className="text-sm text-slate-500">Loading summary...</p>
                                      ) : view?.summary_text ? (
                                        <pre className="max-h-[230px] whitespace-pre-wrap overflow-auto text-sm text-slate-800">
                                          {formatSummaryForDisplay(view.summary_text)}
                                        </pre>
                                      ) : (
                                        <p className="text-sm text-slate-500">
                                          Summary not available yet. Run summarize.
                                        </p>
                                      )}
                                    </div>
                                    <div className="rounded-lg border border-slate-200 bg-white p-3">
                                      <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                                        Action points
                                      </p>
                                      {podcastViewLoadingId === record.id ? (
                                        <p className="text-sm text-slate-500">Loading actions...</p>
                                      ) : view?.action_points && view.action_points.length > 0 ? (
                                        <ul className="space-y-1 text-sm text-slate-800">
                                          {view.action_points.map((item, idx) => (
                                            <li key={`${record.id}-action-${idx}`}>• {item}</li>
                                          ))}
                                        </ul>
                                      ) : (
                                        <p className="text-sm text-slate-500">
                                          No action points detected yet.
                                        </p>
                                      )}
                                    </div>
                                  </div>
                                </div>
                              ) : null}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                    ) : null}
                  </div>
                )})
              )}
            </div>
          ) : isProspectingPipelineModule ? (
            <div className="space-y-3">
              {(records as CustomRecord[]).length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-400 bg-slate-50 p-4 text-sm text-slate-700">
                  No prospects yet. Add your first lead from the panel on the right.
                </div>
              ) : (
                <>
                  <div className="space-y-3 md:hidden">
                    {prospectingRecords.slice(0, 200).map((record) => {
                      const username =
                        readCustomRecordText(record, [
                          "username",
                          "instagram_username",
                          "ig_username",
                          "handle",
                          "person_name",
                        ]) || record.title;
                      const context = readCustomRecordText(record, ["context"]) || record.summary || "-";
                      const openersRaw =
                        readCustomRecordText(record, ["openers", "opener_options", "conversation_starters"]) || "-";
                      const why =
                        readCustomRecordText(record, [
                          "why_this_opener",
                          "whyThisOpener",
                          "opener_rationale",
                        ]) || "-";
                      const normalizedUsername = username.replace(/^@+/, "");
                      const openerLines = openersRaw
                        .split("\n")
                        .map((line) => line.replace(/^\s*[-•]\s*/, "").trim())
                        .filter(Boolean);
                      const isContextExpanded = Boolean(expandedProspectContextById[record.id]);
                      const contextDisplay =
                        isContextExpanded || context.length <= 160
                          ? context
                          : toPreviewText(context, 160);
                      const messaged = isProspectMessaged(record);
                      return (
                        <div
                          key={record.id}
                          className={`rounded-xl border p-3 shadow-sm ${
                            messaged ? "border-emerald-300 bg-emerald-50" : "border-slate-300 bg-slate-50"
                          }`}
                        >
                          <div className="mb-2 flex items-center justify-between gap-2">
                            <a
                              href={`https://instagram.com/${encodeURIComponent(normalizedUsername)}`}
                              className="inline-flex items-center rounded-md border border-blue-300 bg-blue-100 px-2 py-1 text-xs font-semibold text-blue-900 hover:bg-blue-200"
                              rel="noreferrer"
                            >
                              @{normalizedUsername}
                            </a>
                            <div className="flex items-center gap-2">
                              <button
                                type="button"
                                className={`rounded-md border px-2 py-1 text-xs font-semibold ${
                                  messaged
                                    ? "border-emerald-400 bg-emerald-200 text-emerald-900"
                                    : "border-emerald-300 bg-emerald-100 text-emerald-900 hover:bg-emerald-200"
                                }`}
                                onClick={() => void onMarkProspectMessaged(record)}
                                disabled={messaged}
                              >
                                Messaged
                              </button>
                              <button
                                type="button"
                                className="rounded-md border border-blue-300 bg-blue-100 px-2 py-1 text-xs font-semibold text-blue-900 hover:bg-blue-200"
                                onClick={() => void onPromote(record.id)}
                              >
                                Task
                              </button>
                              <button
                                type="button"
                                className="rounded-md border border-rose-300 bg-rose-100 px-2 py-1 text-xs font-semibold text-rose-900 hover:bg-rose-200"
                                onClick={() => void onDelete(record.id)}
                              >
                                Delete
                              </button>
                            </div>
                          </div>
                          <p className="text-xs font-semibold uppercase tracking-wide text-slate-700">Context</p>
                          <p className="mt-1 whitespace-pre-wrap text-sm text-slate-900">{contextDisplay || "-"}</p>
                          {context.length > 160 ? (
                            <button
                              type="button"
                              className="mt-1 text-xs font-semibold text-blue-900 underline underline-offset-2 hover:text-blue-950"
                              onClick={() =>
                                setExpandedProspectContextById((prev) => ({
                                  ...prev,
                                  [record.id]: !prev[record.id],
                                }))
                              }
                            >
                              {isContextExpanded ? "Show less" : "Show more"}
                            </button>
                          ) : null}

                          <p className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-700">Openers</p>
                          <div className="mt-2 space-y-2">
                            {(openerLines.length > 0 ? openerLines : ["-"]).map((line, idx) => {
                              const key = `${record.id}:${idx}`;
                              const copied = copiedProspectOpenerKey === key;
                              return (
                                <div
                                  key={key}
                                  className="rounded-lg border border-slate-300 bg-white px-2 py-2 text-sm text-slate-900"
                                >
                                  <p>{line}</p>
                                  <button
                                    type="button"
                                    className="mt-1 inline-flex items-center rounded-md border border-blue-300 bg-blue-100 px-2 py-1 text-xs font-semibold text-blue-900 hover:bg-blue-200"
                                    onClick={() => void onCopyProspectOpener(record.id, idx, line)}
                                  >
                                    {copied ? "Copied" : "Copy"}
                                  </button>
                                </div>
                              );
                            })}
                          </div>

                          <p className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-700">
                            Why This Opener
                          </p>
                          <p className="mt-1 whitespace-pre-wrap text-sm text-slate-900">{why}</p>
                        </div>
                      );
                    })}
                  </div>

                  <div className="hidden overflow-x-auto md:block">
                    <table className="w-full min-w-[880px] border-collapse text-sm">
                      <thead>
                        <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500">
                          <th className="px-2 py-2">Username</th>
                          <th className="px-2 py-2">Context</th>
                          <th className="px-2 py-2">Openers</th>
                          <th className="px-2 py-2">Why This Opener</th>
                          <th className="px-2 py-2">Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {prospectingRecords.slice(0, 200).map((record) => {
                          const username =
                            readCustomRecordText(record, [
                              "username",
                              "instagram_username",
                              "ig_username",
                              "handle",
                              "person_name",
                            ]) || record.title;
                          const context = readCustomRecordText(record, ["context"]) || record.summary || "-";
                          const openersRaw =
                            readCustomRecordText(record, ["openers", "opener_options", "conversation_starters"]) || "-";
                          const why =
                            readCustomRecordText(record, [
                              "why_this_opener",
                              "whyThisOpener",
                              "opener_rationale",
                            ]) || "-";
                          const normalizedUsername = username.replace(/^@+/, "");
                          const openerLines = openersRaw
                            .split("\n")
                            .map((line) => line.replace(/^\s*[-•]\s*/, "").trim())
                            .filter(Boolean);
                          const isContextExpanded = Boolean(expandedProspectContextById[record.id]);
                          const contextDisplay =
                            isContextExpanded || context.length <= 180
                              ? context
                              : toPreviewText(context, 180);
                          const messaged = isProspectMessaged(record);
                          return (
                            <tr
                              key={record.id}
                              className={`border-b border-slate-100 align-top ${
                                messaged ? "bg-emerald-50" : ""
                              }`}
                            >
                              <td className="px-2 py-3 font-medium text-slate-900">
                                <a
                                  href={`https://instagram.com/${encodeURIComponent(normalizedUsername)}`}
                                  className="inline-flex items-center rounded-md border border-blue-300 bg-blue-100 px-2 py-1 text-xs font-semibold text-blue-900 hover:bg-blue-200"
                                  rel="noreferrer"
                                >
                                  @{normalizedUsername}
                                </a>
                              </td>
                              <td className="max-w-[260px] px-2 py-3 whitespace-pre-wrap text-slate-800">
                                {contextDisplay || "-"}
                                {context.length > 180 ? (
                                  <button
                                    type="button"
                                    className="mt-2 block text-xs font-semibold text-blue-900 underline underline-offset-2 hover:text-blue-950"
                                    onClick={() =>
                                      setExpandedProspectContextById((prev) => ({
                                        ...prev,
                                        [record.id]: !prev[record.id],
                                      }))
                                    }
                                  >
                                    {isContextExpanded ? "Show less" : "Show more"}
                                  </button>
                                ) : null}
                              </td>
                              <td className="px-2 py-3 text-slate-800">
                                <div className="space-y-2">
                                  {(openerLines.length > 0 ? openerLines : ["-"]).map((line, idx) => {
                                    const key = `${record.id}:${idx}`;
                                    const copied = copiedProspectOpenerKey === key;
                                    return (
                                      <div key={key} className="rounded-md border border-slate-300 bg-white px-2 py-1.5">
                                        <p className="whitespace-pre-wrap">{line}</p>
                                        <button
                                          type="button"
                                          className="mt-1 inline-flex items-center rounded-md border border-blue-300 bg-blue-100 px-2 py-1 text-xs font-semibold text-blue-900 hover:bg-blue-200"
                                          onClick={() => void onCopyProspectOpener(record.id, idx, line)}
                                        >
                                          {copied ? "Copied" : "Copy"}
                                        </button>
                                      </div>
                                    );
                                  })}
                                </div>
                              </td>
                              <td className="px-2 py-3 whitespace-pre-wrap text-slate-800">{why}</td>
                              <td className="px-2 py-3">
                                <div className="flex flex-wrap items-center gap-2">
                                  <button
                                    type="button"
                                    className={`rounded-md border px-2 py-1 text-xs font-semibold ${
                                      messaged
                                        ? "border-emerald-400 bg-emerald-200 text-emerald-900"
                                        : "border-emerald-300 bg-emerald-100 text-emerald-900 hover:bg-emerald-200"
                                    }`}
                                    onClick={() => void onMarkProspectMessaged(record)}
                                    disabled={messaged}
                                  >
                                    Messaged
                                  </button>
                                  <button
                                    type="button"
                                    className="rounded-md border border-blue-300 bg-blue-100 px-2 py-1 text-xs font-semibold text-blue-900 hover:bg-blue-200"
                                    onClick={() => void onPromote(record.id)}
                                  >
                                    Task
                                  </button>
                                  <button
                                    type="button"
                                    className="rounded-md border border-rose-300 bg-rose-100 px-2 py-1 text-xs font-semibold text-rose-900 hover:bg-rose-200"
                                    onClick={() => void onDelete(record.id)}
                                  >
                                    Delete
                                  </button>
                                </div>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500">
                    <th className="px-2 py-2">Title</th>
                    <th className="px-2 py-2">Type</th>
                    <th className="px-2 py-2">Stage</th>
                    <th className="px-2 py-2">Summary</th>
                    {isPodcastsModule ? <th className="px-2 py-2">Job Status</th> : null}
                    <th className="px-2 py-2">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {records.length === 0 ? (
                    <tr>
                      <td className="px-2 py-4 text-slate-500" colSpan={isPodcastsModule ? 6 : 5}>
                        No records yet. Add your first one from the panel on the right.
                      </td>
                    </tr>
                  ) : (
                    records.slice(0, 100).map((record) => {
                      const podcastRecord: PodcastRecord | null =
                        isPodcastsModule ? (record as PodcastRecord) : null;
                      const typeLabel: string =
                        "kind" in record
                          ? String(record.kind)
                          : "source" in record
                            ? String(record.source || "newsletter")
                            : podcastRecord
                              ? String(podcastRecord.source_format || "podcast")
                              : "custom";
                      const stageLabel: string =
                        "stage" in record
                          ? String(record.stage)
                          : "priority" in record
                            ? String(record.priority)
                            : "active";
                      return (
                        <tr key={record.id} className="border-b border-slate-100">
                          <td className="px-2 py-3 font-medium text-slate-900">
                            {record.title}
                            {podcastRecord ? (
                              <p className="mt-1 text-xs text-slate-500">
                                {podcastRecord.source_filename || "-"}
                              </p>
                            ) : null}
                          </td>
                          <td className="px-2 py-3 text-slate-600">{typeLabel}</td>
                          <td className="px-2 py-3 text-slate-600">{stageLabel}</td>
                          <td className="px-2 py-3 text-slate-600">
                            {record.summary || "-"}
                            {podcastRecord?.summary_path ? (
                              <p className="mt-1 text-xs text-slate-500">{podcastRecord.summary_path}</p>
                            ) : null}
                          </td>
                          {podcastRecord ? (
                            <td className="px-2 py-3 text-xs text-slate-600">
                              <div>ingest: {podcastRecord.ingest_status}</div>
                              <div>transcript: {podcastRecord.transcript_status}</div>
                              <div>summary: {podcastRecord.summary_status}</div>
                              <div>actions: {podcastRecord.task_extraction_status}</div>
                            </td>
                          ) : null}
                          <td className="px-2 py-3">
                            <div className="flex flex-wrap items-center gap-2">
                              {isPodcastsModule ? (
                                <>
                                  <button
                                    type="button"
                                    className="text-xs text-violet-700 hover:text-violet-800"
                                    onClick={() => void onSummarizePodcast(record.id)}
                                    disabled={podcastBusyRecordId === record.id}
                                  >
                                    Summarize
                                  </button>
                                  <button
                                    type="button"
                                    className="text-xs text-cyan-700 hover:text-cyan-800"
                                    onClick={() => void onExtractPodcastActions(record.id)}
                                    disabled={podcastBusyRecordId === record.id}
                                  >
                                    Extract actions
                                  </button>
                                  <button
                                    type="button"
                                    className="text-xs text-fuchsia-700 hover:text-fuchsia-800"
                                    onClick={() => void onClassifyPodcast(record.id)}
                                    disabled={podcastBusyRecordId === record.id}
                                  >
                                    Classify
                                  </button>
                                  <button
                                    type="button"
                                    className="text-xs text-emerald-700 hover:text-emerald-800"
                                    onClick={() => void onRunPodcastPipeline(record.id)}
                                    disabled={podcastPipelineBusyRecordId === record.id}
                                  >
                                    {podcastPipelineBusyRecordId === record.id
                                      ? "Running..."
                                      : "Run pipeline"}
                                  </button>
                                </>
                              ) : null}
                              <button
                                type="button"
                                className="text-xs text-blue-700 hover:text-blue-800"
                                onClick={() => void onPromote(record.id)}
                              >
                                Task
                              </button>
                              <button
                                type="button"
                                className="text-xs text-rose-600 hover:text-rose-700"
                                onClick={() => void onDelete(record.id)}
                              >
                                Delete
                              </button>
                              {"linkedTaskId" in record && record.linkedTaskId ? (
                                <span className="text-xs text-slate-500">task: {record.linkedTaskId}</span>
                              ) : null}
                            </div>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
        </Card>

        {isNetworkMarketingModule && networkMarketingViewMode === "pipeline" ? (
          <>
            <Card className="rounded-2xl surface-card border border-slate-200 bg-white shadow-sm">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-semibold text-slate-900">Follow up quick glance</h3>
                  <span className="text-xs text-slate-500">
                    Pending: {followUpQuickGlance.length}
                  </span>
                </div>
              </CardHeader>
              <CardContent>
                {followUpQuickGlance.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 p-4 text-sm text-slate-500">
                    No pending follow ups with a date set.
                  </div>
                ) : (
                  <div className="space-y-2">
                    {followUpQuickGlance.map(({ record, dueDate, dueState }) => {
                      const latestJourney = [...(record.journeyEntries ?? [])].sort((a, b) => {
                        const aTs = Date.parse(a.at || "");
                        const bTs = Date.parse(b.at || "");
                        if (Number.isFinite(aTs) && Number.isFinite(bTs)) return bTs - aTs;
                        return 0;
                      })[0];
                      const latestComment =
                        (latestJourney?.note || record.nextStep || record.summary || "").trim() ||
                        "No comment yet";
                      return (
                      <div
                        key={record.id}
                        className="flex flex-col items-start justify-between gap-3 rounded-lg border border-slate-200 bg-white p-3 sm:flex-row sm:items-center"
                      >
                          <div className="min-w-0">
                            <p className="truncate text-sm font-semibold text-slate-900">
                              {record.title?.trim() || record.personName?.trim() || "Unknown Contact"}
                            </p>
                            <p className="text-xs text-slate-600">
                              Follow up: {dueDate}
                              {" · "}
                              <span
                                className={
                                  dueState === "overdue" || dueState === "today"
                                    ? "font-semibold text-rose-600"
                                    : "font-semibold text-amber-600"
                                }
                              >
                                {dueState === "overdue"
                                  ? "Overdue"
                                  : dueState === "today"
                                    ? "Due today"
                                    : "Upcoming"}
                              </span>
                            </p>
                            <p className="mt-1 truncate text-xs text-slate-500">
                              Latest: {latestComment}
                            </p>
                          </div>
                          <label className="flex items-center gap-2 text-xs text-slate-700">
                            <input
                              type="checkbox"
                              checked={false}
                              disabled={followUpToggleBusyRecordId === record.id}
                              onChange={(event) =>
                                void onToggleFollowUpCompleted(record, event.target.checked)
                              }
                            />
                            {followUpToggleBusyRecordId === record.id ? "Saving..." : "Completed"}
                          </label>
                        </div>
                      );
                    })}
                  </div>
                )}
              </CardContent>
            </Card>

            <Card className="rounded-2xl surface-card border border-slate-200 bg-white shadow-sm">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <h3 className="text-base font-semibold text-slate-900">Huddle Plays</h3>
                  <span className="text-xs text-slate-500">Grouped by day</span>
                </div>
              </CardHeader>
              <CardContent>
                {groupedHuddlePlays.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 p-4 text-sm text-slate-500">
                    No Huddle plays logged yet. Zohan-created plays will appear here automatically.
                  </div>
                ) : (
                  <div className="overflow-x-auto pb-2">
                    <div className="grid gap-3 md:min-w-[900px] md:auto-cols-fr md:grid-flow-col">
                      {groupedHuddlePlays.map((group) => (
                        <div
                          key={group.day}
                          className="rounded-xl border border-indigo-200 bg-indigo-50/60 p-3 md:min-w-[280px]"
                        >
                          <div className="mb-3 flex items-center justify-between">
                            <h4 className="text-xs font-semibold uppercase tracking-wide text-indigo-800">
                              {group.day}
                            </h4>
                            <span className="rounded-full bg-white px-2 py-0.5 text-xs text-slate-600">
                              {group.records.length}
                            </span>
                          </div>
                          <div className="space-y-2">
                            {group.records.map((record) => (
                              <div
                                key={record.id}
                                className="rounded-lg border border-slate-200 bg-white p-3 shadow-sm"
                              >
                                <p className="text-sm font-medium text-slate-900">
                                  {record.title?.trim() || record.personName?.trim() || "Unknown Contact"}
                                </p>
                                <p className="mt-1 whitespace-pre-wrap text-xs text-slate-600">
                                  {record.huddlePlay?.trim() || record.summary || "-"}
                                </p>
                                <div className="mt-3 flex items-center gap-3">
                                  <button
                                    type="button"
                                    className="text-xs text-blue-700 hover:text-blue-800 disabled:text-slate-400"
                                    onClick={() => void onAddHuddleToMadeAware(record)}
                                    disabled={mpaBusyRecordId === record.id}
                                  >
                                    {mpaBusyRecordId === record.id ? "Adding..." : "MPA"}
                                  </button>
                                  <button
                                    type="button"
                                    className="text-xs text-slate-700 hover:text-slate-900"
                                    onClick={() => void onEditHuddlePlayName(record)}
                                  >
                                    Edit name
                                  </button>
                                  <button
                                    type="button"
                                    className="text-xs text-rose-600 hover:text-rose-700"
                                    onClick={() => void onDelete(record.id)}
                                  >
                                    Delete
                                  </button>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          </>
        ) : null}

      </div>

      {!isBudgetModule ? (
      <div className="order-1 min-w-0 lg:order-2">
      <Card className="border border-slate-200 bg-white shadow-sm lg:sticky lg:top-24">
        <CardHeader>
          <h3 className="text-base font-semibold text-slate-900">
            {isEventsModule ? "Scan Event Sources" : "Add Record"}
          </h3>
          <p className="mt-1 text-sm text-slate-500">
            {isEventsModule
              ? "Scan multiple event sites and import events for a selected week."
              : "Create operational records for this module."}
          </p>
        </CardHeader>
        <CardContent className="space-y-3">
          {isEventsModule ? (
            <>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Source URLs (one per line)
              </label>
              <Textarea
                value={eventsSourceInput}
                onChange={(event) => setEventsSourceInput(event.target.value)}
                rows={6}
                placeholder="https://.../events.ics"
              />
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Week start (optional)
              </label>
              <Input
                type="date"
                value={eventsWeekStart}
                onChange={(event) => setEventsWeekStart(event.target.value)}
              />
              <Button
                onClick={() => void scanEventsForWeek()}
                className="w-full"
                disabled={eventsScanBusy}
              >
                {eventsScanBusy ? "Scanning..." : "Scan and import this week"}
              </Button>
              {eventsScanMessage ? (
                <div className="whitespace-pre-line rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-xs text-emerald-700">
                  {eventsScanMessage}
                </div>
              ) : null}
            </>
          ) : null}
          {!isEventsModule ? (
            <>
              <Input
                value={title}
                onChange={(event) => setTitle(event.target.value)}
                placeholder={
                  isProspectingPipelineModule
                    ? "Instagram username (without @)"
                    : isNetworkMarketingModule && networkMarketingViewMode === "cold_contact"
                      ? "Prospect name or @handle"
                      : "Title"
                }
              />
              {!isPodcastsModule && !isProspectingPipelineModule ? (
                <Textarea
                  value={summary}
                  onChange={(event) => setSummary(event.target.value)}
                  placeholder="Summary"
                  rows={4}
                />
              ) : null}
              {isPodcastsModule ? (
                <>
                  <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                    Category
                  </label>
                  <select
                    value={stage}
                    onChange={(event) => setStage(event.target.value)}
                    className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
                  >
                    {podcastCategoryOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </>
              ) : isNetworkMarketingModule && networkMarketingViewMode === "cold_contact" ? (
                <>
                  <Input
                    value={stage}
                    onChange={(event) => setStage(event.target.value)}
                    placeholder="Stage (contact_made, made_aware, door_opened...)"
                  />
                  <select
                    value={coldContactPlatform}
                    onChange={(event) => setColdContactPlatform(event.target.value)}
                    className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
                  >
                    <option value="instagram">Instagram</option>
                    <option value="facebook">Facebook</option>
                    <option value="linkedin">LinkedIn</option>
                    <option value="other">Other</option>
                  </select>
                  <Input
                    value={coldContactProfileUrl}
                    onChange={(event) => setColdContactProfileUrl(event.target.value)}
                    placeholder="Profile URL"
                  />
                  <Textarea
                    value={coldContactWhyFit}
                    onChange={(event) => setColdContactWhyFit(event.target.value)}
                    placeholder="Why this person is a good fit"
                    rows={3}
                  />
                  <Textarea
                    value={coldContactWhyNow}
                    onChange={(event) => setColdContactWhyNow(event.target.value)}
                    placeholder="Why they may be looking for opportunities now"
                    rows={3}
                  />
                  <Textarea
                    value={coldContactSignals}
                    onChange={(event) => setColdContactSignals(event.target.value)}
                    placeholder={"Signals/evidence (one per line)\n- ...\n- ..."}
                    rows={3}
                  />
                  <Input
                    value={coldContactScore}
                    onChange={(event) => setColdContactScore(event.target.value)}
                    placeholder="Fit score (0-100)"
                  />
                  <select
                    value={coldContactConfidence}
                    onChange={(event) =>
                      setColdContactConfidence(event.target.value as (typeof coldContactConfidenceOptions)[number])
                    }
                    className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
                  >
                    {coldContactConfidenceOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                  <Input
                    value={coldContactAngle}
                    onChange={(event) => setColdContactAngle(event.target.value)}
                    placeholder="Suggested first-message angle"
                  />
                  <Input
                    value={coldContactSource}
                    onChange={(event) => setColdContactSource(event.target.value)}
                    placeholder="Lead source (group/hashtag/search)"
                  />
                  <Textarea
                    value={coldContactResearch}
                    onChange={(event) => setColdContactResearch(event.target.value)}
                    placeholder="Research notes"
                    rows={3}
                  />
                </>
              ) : !isProspectingPipelineModule ? (
                <Input
                  value={stage}
                  onChange={(event) => setStage(event.target.value)}
                  placeholder={
                    isNetworkMarketingModule
                      ? "Stage (contact_made, made_aware, door_opened...)"
                      : "Stage (e.g. inbox, review, active)"
                  }
                />
              ) : (
                <>
                  <Textarea
                    value={pipelineProspectContext}
                    onChange={(event) => setPipelineProspectContext(event.target.value)}
                    placeholder="Context from profile/posts"
                    rows={4}
                  />
                  <Textarea
                    value={pipelineProspectOpeners}
                    onChange={(event) => setPipelineProspectOpeners(event.target.value)}
                    placeholder={"Openers (one per line)\n1) ...\n2) ..."}
                    rows={5}
                  />
                  <Textarea
                    value={pipelineProspectWhy}
                    onChange={(event) => setPipelineProspectWhy(event.target.value)}
                    placeholder="Why these openers should work for this person"
                    rows={3}
                  />
                  <Input
                    value={stage}
                    onChange={(event) => setStage(event.target.value)}
                    placeholder="Stage (e.g. new, researching, ready)"
                  />
                </>
              )}
            </>
          ) : null}

          {isPodcastsModule ? (
            <>
              <Button
                onClick={() => void onSyncPodcastDriveNow()}
                className="w-full bg-emerald-600 text-white hover:bg-emerald-700"
                disabled={podcastDriveSyncBusy}
              >
                {podcastDriveSyncBusy ? "Syncing..." : "Sync + Transcribe"}
              </Button>
              {podcastDriveSyncMessage ? (
                <div className="whitespace-pre-line rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-xs text-emerald-800">
                  {podcastDriveSyncMessage}
                </div>
              ) : null}
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Audio Upload
              </label>
              <input
                type="file"
                accept=".mp3,.m4a,.wav,audio/*"
                onChange={(event) => {
                  const file = event.target.files?.[0] ?? null;
                  setPodcastUploadFile(file);
                  if (file && file.size > MAX_PODCAST_UPLOAD_BYTES) {
                    setError(
                      `Audio file is too large (${formatFileSize(
                        file.size,
                      )}). Max upload size is 100MB when using the remote URL.`,
                    );
                  }
                }}
                className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
              />
              {podcastUploadFile ? (
                <p className="text-xs text-slate-500">
                  Selected: {podcastUploadFile.name} ({formatFileSize(podcastUploadFile.size)})
                </p>
              ) : null}
              <p className="text-xs text-slate-500">
                Remote uploads are limited to 100MB. For larger files, trim/compress first.
              </p>
            </>
          ) : null}

          {module.id === "finance" ? (
            <>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Finance Type
              </label>
              <select
                value={financeKind}
                onChange={(event) => setFinanceKind(event.target.value as FinanceKind)}
                className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
              >
                {financeKinds.map((kind) => (
                  <option key={kind} value={kind}>
                    {kind}
                  </option>
                ))}
              </select>
              <Input
                value={financeAmount}
                onChange={(event) => setFinanceAmount(event.target.value)}
                placeholder="Amount (optional)"
              />
            </>
          ) : null}

          {isNetworkMarketingModule ? (
            networkMarketingViewMode === "team_tree" ? (
              <>
                <div className="rounded-xl border border-blue-100 bg-blue-50 p-3 text-xs text-blue-800">
                  Add Member mode: creates a Team Tree member under root or an existing member.
                </div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                  Parent Member
                </label>
                <select
                  value={memberParentId}
                  onChange={(event) => setMemberParentId(event.target.value)}
                  className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
                >
                  <option value="root">Root (direct to me)</option>
                  {teamMemberRecords.map((member) => (
                    <option key={member.id} value={member.id}>
                      {(member.displayName || member.title).trim()} ({member.id.slice(0, 8)})
                    </option>
                  ))}
                </select>
                <Input
                  value={memberStatusSummary}
                  onChange={(event) => setMemberStatusSummary(event.target.value)}
                  placeholder="Status summary (optional)"
                />
                <Input
                  value={networkNextStep}
                  onChange={(event) => setNetworkNextStep(event.target.value)}
                  placeholder="Next step"
                />
              </>
            ) : networkMarketingViewMode === "cold_contact" ? (
              <>
                <div className="rounded-xl border border-amber-100 bg-amber-50 p-3 text-xs text-amber-800">
                  Cold Contact mode: save fit score, intent signals, and research so outreach stays prioritized and personalized.
                </div>
                <Input
                  value={networkNextStep}
                  onChange={(event) => setNetworkNextStep(event.target.value)}
                  placeholder="Next step for your outreach"
                />
              </>
            ) : (
              <>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                  Record Type
                </label>
                <select
                  value={networkKind}
                  onChange={(event) => setNetworkKind(event.target.value as NetworkKind)}
                  className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
                >
                  {networkKinds.map((kind) => (
                    <option key={kind} value={kind}>
                      {kind}
                    </option>
                  ))}
                </select>
                <Input
                  value={networkNextStep}
                  onChange={(event) => setNetworkNextStep(event.target.value)}
                  placeholder="Next step"
                />
              </>
            )
          ) : null}

          {module.id === "newsletters" ? (
            <>
              <Input
                value={newsletterSource}
                onChange={(event) => setNewsletterSource(event.target.value)}
                placeholder="Source (e.g. Morning Brew)"
              />
              <Input
                value={newsletterAction}
                onChange={(event) => setNewsletterAction(event.target.value)}
                placeholder="Action to take"
              />
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Priority
              </label>
              <select
                value={newsletterPriority}
                onChange={(event) =>
                  setNewsletterPriority(
                    event.target.value as NewsletterRecord["priority"],
                  )
                }
                className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
              >
                {priorities.map((priority) => (
                  <option key={priority} value={priority}>
                    {priority}
                  </option>
                ))}
              </select>
            </>
          ) : null}

          {error ? (
            <div className="rounded-xl border border-rose-200 bg-rose-50 p-3 text-xs text-rose-700">
              {error}
            </div>
          ) : null}
          {isNetworkMarketingModule && networkMarketingViewMode === "cold_contact" ? (
            <>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Import cold contacts (JSON)
              </label>
              <input
                type="file"
                accept=".json,application/json"
                onChange={(event) => setColdContactImportFile(event.target.files?.[0] ?? null)}
                className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
              />
              <Button
                type="button"
                variant="secondary"
                onClick={() => void onImportColdContacts()}
                disabled={!coldContactImportFile || coldContactImportBusy}
                className="w-full"
              >
                {coldContactImportBusy ? "Importing..." : "Import JSON to Cold Contact"}
              </Button>
            </>
          ) : null}
          {isProspectingPipelineModule ? (
            <>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Import prospects (JSON)
              </label>
              <input
                type="file"
                accept=".json,application/json"
                onChange={(event) => setPipelineImportFile(event.target.files?.[0] ?? null)}
                className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
              />
              <Button
                type="button"
                variant="secondary"
                onClick={() => void onImportPipelineProspects()}
                disabled={!pipelineImportFile || pipelineImportBusy}
                className="w-full"
              >
                {pipelineImportBusy ? "Importing..." : "Import JSON to Pipeline"}
              </Button>
            </>
          ) : null}
          {!isPodcastsModule && !isEventsModule ? (
            <Input
              value={promoteBoardId}
              onChange={(event) => setPromoteBoardId(event.target.value)}
              placeholder="Board ID for Task action"
            />
          ) : null}
          {!isEventsModule ? (
            <Button onClick={() => void onAdd()} className="w-full" disabled={isSubmitting}>
              {isSubmitting ? "Saving..." : "Add record"}
            </Button>
          ) : null}
        </CardContent>
        </Card>
      </div>
      ) : null}

      {activeMemberRecord ? (
        <div className="fixed inset-0 z-50 overflow-y-auto bg-slate-900/55 p-3 sm:p-4">
          <div className="flex min-h-full items-start justify-center py-2 sm:items-center sm:py-4">
          <div className="flex w-full max-w-2xl max-h-[92vh] flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl">
            <div className="flex items-start justify-between gap-4 border-b border-slate-100 p-4 sm:p-5">
              <div>
                <h3 className="text-lg font-semibold text-slate-900">
                  {activeMemberRecord.displayName || activeMemberRecord.title}
                </h3>
                <p className="text-xs text-slate-500">
                  Linked folder: {activeMemberRecord.memberFolderPath || "Not set"}
                </p>
              </div>
              <button
                type="button"
                className="rounded-lg border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
                onClick={() => closeMemberModal()}
                disabled={memberModalBusy}
              >
                Close
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-4 sm:p-5">
            <div className="grid gap-3">
              <div>
                <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">
                  How they are doing
                </p>
                <Textarea
                  rows={2}
                  value={memberHowDoing}
                  onChange={(event) => setMemberHowDoing(event.target.value)}
                  placeholder="Current momentum, mindset, and progress."
                />
              </div>
              <div>
                <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">
                  What is new
                </p>
                <Textarea
                  rows={2}
                  value={memberWhatsNew}
                  onChange={(event) => setMemberWhatsNew(event.target.value)}
                  placeholder="New updates since last check-in."
                />
              </div>
              <div>
                <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">
                  Urgent
                </p>
                <Textarea
                  rows={2}
                  value={memberUrgent}
                  onChange={(event) => setMemberUrgent(event.target.value)}
                  placeholder="Urgent follow-up items or blockers."
                />
              </div>
              <div>
                <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-600">
                  Notes
                </p>
                <Textarea
                  rows={3}
                  value={memberNotes}
                  onChange={(event) => setMemberNotes(event.target.value)}
                  placeholder="Anything else worth tracking for this member."
                />
              </div>
            </div>

            <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 p-3">
              <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                Update history
              </p>
              {activeMemberTimeline.length === 0 ? (
                <p className="text-xs text-slate-500">No updates saved yet.</p>
              ) : (
                <div className="max-h-52 space-y-2 overflow-y-auto pr-1">
                  {activeMemberTimeline.map((entry, index) => (
                    <div key={`${entry.at}-${index}`} className="rounded-lg border border-slate-200 bg-white p-2">
                      <div className="flex items-start justify-between gap-3">
                        <p className="text-[11px] font-semibold text-slate-500">
                          {formatTimelineDate(entry.at)}
                        </p>
                        <button
                          type="button"
                          className="text-[11px] font-medium text-rose-600 hover:text-rose-700 disabled:text-slate-400"
                          onClick={() => void deleteMemberTimelineEntry(entry.at, entry.note)}
                          disabled={memberModalBusy}
                          title="Delete this history entry"
                        >
                          Delete
                        </button>
                      </div>
                      <p className="mt-1 text-xs text-slate-700">{entry.note}</p>
                    </div>
                  ))}
                </div>
              )}
            </div>
            </div>

            <div className="flex items-center justify-end gap-2 border-t border-slate-100 p-4 sm:p-5">
              <Button
                type="button"
                variant="secondary"
                onClick={() => closeMemberModal()}
                disabled={memberModalBusy}
              >
                Cancel
              </Button>
              <Button type="button" onClick={() => void saveMemberModal()} disabled={memberModalBusy}>
                {memberModalBusy ? "Saving..." : "Save update"}
              </Button>
            </div>
          </div>
          </div>
        </div>
      ) : null}

      {activeEventRecord ? (
        <div className="fixed inset-0 z-50 overflow-y-auto bg-slate-950/60 p-3 sm:p-5">
          <div className="flex min-h-full items-start justify-center sm:items-center">
            <div className="flex w-full max-w-6xl flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl">
              <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">
                <div className="flex items-center gap-3">
                  <h3 className="text-2xl font-semibold text-slate-900">{activeEventRecord.title}</h3>
                  <span className="rounded-lg border border-violet-200 bg-violet-50 px-2 py-1 text-xs font-semibold text-violet-700">
                    AI insights
                  </span>
                </div>
                <button
                  type="button"
                  className="rounded-lg border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-100"
                  onClick={closeEventModal}
                  disabled={eventModalBusy}
                >
                  Close
                </button>
              </div>

              <div className="grid gap-4 p-4 lg:grid-cols-[340px_1fr]">
                <div className="space-y-4">
                  <div className="rounded-xl border border-emerald-200 bg-emerald-50/20">
                    <div className="border-b border-slate-200 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Event insights</h4>
                    </div>
                    <div className="p-4">
                      {(() => {
                        const signals = buildEventSignals(activeEventRecord);
                        return (
                          <div className="rounded-xl border border-slate-200 bg-white p-4">
                            <p className="text-sm font-semibold text-slate-900">{signals.negative.label}</p>
                            {signals.negative.items.length === 0 ? (
                              <p className="mt-1 text-sm text-slate-500">No negative signals detected.</p>
                            ) : (
                              <ul className="mt-1 space-y-1 text-sm text-rose-600">
                                {signals.negative.items.map((item) => (
                                  <li key={item}>⚠ {item}</li>
                                ))}
                              </ul>
                            )}
                            <p className="mt-4 text-sm font-semibold text-slate-900">{signals.positive.label}</p>
                            {signals.positive.items.length === 0 ? (
                              <p className="mt-1 text-sm text-slate-500">No positive signals yet.</p>
                            ) : (
                              <ul className="mt-1 space-y-1 text-sm text-emerald-700">
                                {signals.positive.items.map((item) => (
                                  <li key={item}>✓ {item}</li>
                                ))}
                              </ul>
                            )}
                          </div>
                        );
                      })()}
                    </div>
                  </div>
                </div>

                <div className="space-y-4">
                  <div className="rounded-xl border border-slate-200 bg-white">
                    <div className="border-b border-slate-100 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Event stages</h4>
                    </div>
                    <div className="p-4">
                      <div className="mb-3 flex flex-wrap items-center gap-2 text-xs">
                        <span className="rounded bg-emerald-50 px-2 py-1 font-semibold text-emerald-700">
                          Stage: {eventStage.replaceAll("_", " ")}
                        </span>
                        <span className={`rounded px-2 py-1 font-semibold ${eventTimeConfidenceMeta(getEventTimeConfidence(activeEventRecord)).className}`}>
                          {eventTimeConfidenceMeta(getEventTimeConfidence(activeEventRecord)).label}
                        </span>
                        <span className="rounded bg-slate-100 px-2 py-1 text-slate-600">
                          Updated {formatTimelineDate(activeEventRecord.updatedAt)}
                        </span>
                      </div>
                      <div className="mb-3 grid gap-1 sm:grid-cols-3 lg:grid-cols-4">
                        {eventStages.map((stageId) => (
                          <button
                            key={stageId}
                            type="button"
                            className={`rounded px-2 py-2 text-xs font-semibold ${
                              eventStage === stageId
                                ? "bg-emerald-600 text-white"
                                : "bg-emerald-100 text-emerald-800 hover:bg-emerald-200"
                            }`}
                            onClick={() => void moveEventToStage(stageId)}
                            disabled={eventModalBusy}
                          >
                            {stageId.replaceAll("_", " ")}
                          </button>
                        ))}
                      </div>
                      <div className="grid gap-1 rounded-lg border border-slate-200 bg-slate-50 p-2 text-xs text-slate-600 sm:grid-cols-2">
                        <div><span className="font-semibold text-slate-700">When:</span> {formatEventDateTime(activeEventRecord.startAt, activeEventRecord.endAt, activeEventRecord.sourceName)}</div>
                        <div><span className="font-semibold text-slate-700">Where:</span> {activeEventRecord.venue || "TBC"}</div>
                        <div><span className="font-semibold text-slate-700">Organizer:</span> {activeEventRecord.organizer || "Unknown"}</div>
                        <div><span className="font-semibold text-slate-700">Price:</span> {activeEventRecord.price || (activeEventRecord.isFree ? "Free" : "N/A")}</div>
                      </div>
                    </div>
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-white">
                    <div className="border-b border-slate-100 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Summary</h4>
                    </div>
                    <div className="space-y-3 p-4">
                      <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                        Summary
                      </label>
                      <Textarea
                        rows={10}
                        value={eventSummary}
                        onChange={(event) => setEventSummary(event.target.value)}
                        placeholder="Event context, value, and notes."
                        className="min-h-[280px]"
                      />
                      {extractSummaryUrls(eventSummary).length > 0 ? (
                        <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                          <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                            Links
                          </p>
                          <div className="flex flex-col gap-1">
                            {extractSummaryUrls(eventSummary).map((link) => (
                              <a
                                key={link}
                                href={link}
                                target="_blank"
                                rel="noreferrer"
                                className="truncate text-sm text-blue-700 hover:text-blue-800"
                                title={link}
                              >
                                {link}
                              </a>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </div>

                </div>
              </div>

              <div className="flex items-center justify-end gap-2 border-t border-slate-100 px-5 py-4">
                <Button type="button" variant="secondary" onClick={closeEventModal} disabled={eventModalBusy}>
                  Cancel
                </Button>
                <Button type="button" onClick={() => void saveEventModal()} disabled={eventModalBusy}>
                  {eventModalBusy ? "Saving..." : "Save event"}
                </Button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {activePipelineRecord ? (
        <div className="fixed inset-0 z-50 overflow-y-auto bg-slate-950/60 p-3 sm:p-5">
          <div className="flex min-h-full items-start justify-center sm:items-center">
            <div className="flex w-full max-w-6xl flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl">
              <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">
                <div className="flex items-center gap-3">
                  <h3 className="text-2xl font-semibold text-slate-900">
                    {activePipelineRecord.title}
                  </h3>
                  <span className="rounded-lg border border-violet-200 bg-violet-50 px-2 py-1 text-xs font-semibold text-violet-700">
                    AI insights
                  </span>
                </div>
                <button
                  type="button"
                  className="rounded-lg border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-100"
                  onClick={closePipelineModal}
                  disabled={pipelineModalBusy}
                >
                  Close
                </button>
              </div>

              <div className="grid gap-4 p-4 lg:grid-cols-[360px_1fr]">
                <div className="space-y-4">
                  <div className="rounded-xl border border-emerald-200 bg-emerald-50/20">
                    <div className="border-b border-slate-200 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Process insights</h4>
                    </div>
                    <div className="p-4">
                      {(() => {
                        const signals = buildPipelineSignals(activePipelineRecord);
                        return (
                          <div className="rounded-xl border border-slate-200 bg-white p-4">
                            <p className="text-sm font-semibold text-slate-900">{signals.negative.label}</p>
                            {signals.negative.items.length === 0 ? (
                              <p className="mt-1 text-sm text-slate-500">No negative signals detected.</p>
                            ) : (
                              <ul className="mt-1 space-y-1 text-sm text-rose-600">
                                {signals.negative.items.map((item) => (
                                  <li key={item}>⚠ {item}</li>
                                ))}
                              </ul>
                            )}
                            <p className="mt-4 text-sm font-semibold text-slate-900">{signals.positive.label}</p>
                            {signals.positive.items.length === 0 ? (
                              <p className="mt-1 text-sm text-slate-500">No positive signals yet.</p>
                            ) : (
                              <ul className="mt-1 space-y-1 text-sm text-emerald-700">
                                {signals.positive.items.map((item) => (
                                  <li key={item}>✓ {item}</li>
                                ))}
                              </ul>
                            )}
                          </div>
                        );
                      })()}
                    </div>
                  </div>

                  <div className="rounded-xl border border-slate-200 bg-white">
                    <div className="border-b border-slate-100 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Summary</h4>
                    </div>
                    <div className="space-y-3 p-4">
                      <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                        Summary
                      </label>
                      <Textarea
                        rows={4}
                        value={pipelineSummary}
                        onChange={(event) => setPipelineSummary(event.target.value)}
                        placeholder="Process context and notes."
                      />
                      <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                        Team Tree member
                      </label>
                      <select
                        value={pipelineLinkedMemberId}
                        onChange={(event) => setPipelineLinkedMemberId(event.target.value)}
                        className="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm"
                      >
                        <option value="none">Not linked</option>
                        {teamMemberRecords.map((member) => (
                          <option key={member.id} value={member.id}>
                            {(member.displayName || member.title).trim()}
                          </option>
                        ))}
                      </select>
                      <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                        Next step
                      </label>
                      <Input
                        value={pipelineNextStep}
                        onChange={(event) => setPipelineNextStep(event.target.value)}
                        placeholder="Book next call / proposal / follow up..."
                      />
                    </div>
                  </div>
                </div>

                <div className="space-y-4">
                  <div className="rounded-xl border border-slate-200 bg-white">
                    <div className="border-b border-slate-100 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Process stages</h4>
                    </div>
                    <div className="p-4">
                      <div className="mb-3 flex flex-wrap items-center gap-2 text-xs">
                        <span className="rounded bg-emerald-50 px-2 py-1 font-semibold text-emerald-700">
                          Stage: {pipelineStage.replaceAll("_", " ")}
                        </span>
                        <span className="rounded bg-slate-100 px-2 py-1 text-slate-600">
                          Updated {formatTimelineDate(activePipelineRecord.updatedAt)}
                        </span>
                      </div>
                      <div className="mb-3 grid gap-1 sm:grid-cols-3 lg:grid-cols-4">
                        {networkMarketingStages.map((stageId) => (
                          <button
                            key={stageId}
                            type="button"
                            className={`rounded px-2 py-2 text-xs font-semibold ${
                              pipelineStage === stageId
                                ? "bg-emerald-600 text-white"
                                : "bg-emerald-100 text-emerald-800 hover:bg-emerald-200"
                            }`}
                            onClick={() => void movePipelineModalToStage(stageId)}
                            disabled={pipelineModalBusy}
                          >
                            {stageId.replaceAll("_", " ")}
                          </button>
                        ))}
                      </div>
                      <div className="flex flex-wrap items-center gap-2">
                        <button
                          type="button"
                          className="rounded bg-emerald-600 px-3 py-1 text-xs font-semibold text-white hover:bg-emerald-700"
                          onClick={() => void movePipelineModalToStage("launched")}
                          disabled={pipelineModalBusy}
                        >
                          Launched
                        </button>
                        <button
                          type="button"
                          className="rounded bg-rose-600 px-3 py-1 text-xs font-semibold text-white hover:bg-rose-700"
                          onClick={() => void movePipelineModalToStage("dropped_out")}
                          disabled={pipelineModalBusy}
                        >
                          Dropped Out
                        </button>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-xl border border-slate-200 bg-white">
                    <div className="border-b border-slate-100 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Follow up</h4>
                    </div>
                    <div className="space-y-3 p-4">
                      <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                        Follow up date
                      </label>
                      <Input
                        type="date"
                        value={pipelineFollowUpDate}
                        onChange={(event) => {
                          const nextDate = event.target.value;
                          const didChange = nextDate !== pipelineFollowUpDate;
                          setPipelineFollowUpDate(nextDate);
                          if (didChange) {
                            setPipelineFollowUpCompleted(false);
                            setPipelineFollowUpCompletedAt(null);
                          }
                        }}
                        disabled={pipelineModalBusy}
                      />
                      <label className="flex items-center gap-2 text-sm text-slate-700">
                        <input
                          type="checkbox"
                          checked={pipelineFollowUpCompleted}
                          onChange={(event) => {
                            const checked = event.target.checked;
                            setPipelineFollowUpCompleted(checked);
                            setPipelineFollowUpCompletedAt(checked ? new Date().toISOString() : null);
                          }}
                          disabled={pipelineModalBusy}
                        />
                        Mark follow up completed
                      </label>
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
                        {(() => {
                          const today = new Date().toISOString().slice(0, 10);
                          const due = pipelineFollowUpDate.trim();
                          if (pipelineFollowUpCompleted) {
                            const completedLabel = pipelineFollowUpCompletedAt
                              ? formatTimelineDate(pipelineFollowUpCompletedAt)
                              : "Completed";
                            return `Status: Completed (${completedLabel})`;
                          }
                          if (!due) return "Status: No follow up date set";
                          if (due < today) return `Status: Overdue (${due})`;
                          if (due === today) return `Status: Due today (${due})`;
                          return `Status: Scheduled (${due})`;
                        })()}
                      </div>
                    </div>
                  </div>

                  <div className="rounded-xl border border-slate-200 bg-white">
                    <div className="border-b border-slate-100 px-4 py-3">
                      <h4 className="text-2xl font-semibold text-slate-900">Journey</h4>
                    </div>
                    <div className="p-4">
                      <div className="mb-3 flex flex-wrap items-center gap-2">
                        <Input
                          value={pipelineJourneyDraft}
                          onChange={(event) => setPipelineJourneyDraft(event.target.value)}
                          placeholder="Add journey update..."
                        />
                        <button
                          type="button"
                          className="rounded bg-blue-600 px-3 py-2 text-xs font-semibold text-white hover:bg-blue-700"
                          onClick={addPipelineJourneyEntry}
                        >
                          Add update
                        </button>
                      </div>
                      <div className="space-y-3">
                        {pipelineJourneyEntries.length === 0 ? (
                          <div className="rounded-lg border border-dashed border-slate-300 p-3 text-sm text-slate-500">
                            No journey updates yet.
                          </div>
                        ) : (
                          groupJourneyEntriesByDay(pipelineJourneyEntries).map((group) => (
                            <div key={group.day} className="rounded-lg border border-slate-200 p-3">
                              <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                                {group.day}
                              </p>
                              <div className="space-y-2">
                                {group.entries.map((entry, index) => (
                                  <div
                                    key={`${group.day}-${entry.at}-${index}`}
                                    className="rounded-md border border-slate-100 bg-slate-50 px-2 py-1.5"
                                  >
                                    <div className="flex items-start justify-between gap-3">
                                      <p className="text-xs text-slate-500">
                                        {formatTimelineDate(entry.at)}
                                      </p>
                                      <button
                                        type="button"
                                        className="text-[11px] font-medium text-rose-600 hover:text-rose-700 disabled:text-slate-400"
                                        onClick={() => void deletePipelineJourneyEntry(entry.at, entry.note)}
                                        disabled={pipelineModalBusy}
                                      >
                                        Delete
                                      </button>
                                    </div>
                                    <p className="text-sm text-slate-700">{entry.note}</p>
                                  </div>
                                ))}
                              </div>
                            </div>
                          ))
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex items-center justify-end gap-2 border-t border-slate-100 px-5 py-4">
                <Button type="button" variant="secondary" onClick={closePipelineModal} disabled={pipelineModalBusy}>
                  Cancel
                </Button>
                <Button type="button" onClick={() => void savePipelineModal()} disabled={pipelineModalBusy}>
                  {pipelineModalBusy ? "Saving..." : "Save process"}
                </Button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
