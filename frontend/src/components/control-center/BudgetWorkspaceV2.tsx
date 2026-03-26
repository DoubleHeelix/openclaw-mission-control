"use client";

import { Fragment, useEffect, useMemo, useRef, useState } from "react";

import { customFetch } from "@/api/mutator";
import { Button } from "@/components/ui/button";

type ParserInfo = { name: string; banks: string[]; formats: string[] };

type ImportSummary = {
  import_id: string;
  status: string;
  source_bank?: string | null;
  parser_name?: string | null;
  parser_confidence?: number | null;
  parser_warnings?: string[] | null;
  coverage_estimate?: number | null;
  suspected_missing_pages?: boolean | null;
  duplicate_rows_detected?: number | null;
  overlap_status?: string | null;
  statement_start_date?: string | null;
  statement_end_date?: string | null;
  transaction_count: number;
  scope_warnings?: string[] | null;
  parser_coverage_warnings?: string[] | null;
  parsed_debit_count?: number;
  parsed_credit_count?: number;
  parsed_debit_total?: number;
  parsed_credit_total?: number;
  opening_balance?: number | null;
  closing_balance?: number | null;
  statement_total_debits?: number | null;
  statement_total_credits?: number | null;
  expected_closing_balance?: number | null;
  reconciliation_status?: string;
  reconciliation_reason?: string | null;
  reconciliation_difference?: number;
  warning_reasons?: string[] | null;
  document_type?: string | null;
  document_reconcilable?: boolean | null;
  document_warnings?: string[] | null;
  budget_model?: {
    recurring_income_monthly?: number;
    recurring_baseline_monthly?: number;
    variable_discretionary_monthly?: number;
    observed_one_off_total?: number;
    observed_transfer_total?: number;
    irregular_income_total?: number;
    core_net?: number;
    observed_net?: number;
    modeling_allowed?: boolean;
    modeling_restrictions?: string[] | null;
  } | null;
  trust?: {
    modeling_allowed?: boolean;
    modeling_restrictions?: string[] | null;
    totals_trust_level?: string;
  } | null;
};

type TxRow = {
  id: string;
  transaction_date?: string | null;
  amount: number;
  direction: "credit" | "debit";
  raw_description: string;
  normalized_description: string;
  direction_source: string;
  movement_type: string;
  amount_source?: string | null;
  date_source?: string | null;
  balance_source?: string | null;
  description_continuation_detected?: boolean;
  classification_version?: string | null;
  mapping_source?: string | null;
  payment_rail?: string | null;
  merchant_candidate?: string | null;
  interpretation_type: string;
  interpretation_confidence: number;
  interpretation_reason: string;
  category: string;
  subcategory: string;
  confidence: number;
  explanation: string;
  bucket_assignment: string;
  confidence_label: string;
  inferred_cadence?: string | null;
  cadence_confidence?: number | null;
  cadence_reason?: string | null;
  merchant_confidence?: number;
  bucket_confidence?: number;
  impact_on_baseline: "included" | "excluded" | "reserve_only";
  included: boolean;
  observed_only: boolean;
  review_reasons: string[];
  group_key?: string | null;
  group_transaction_count: number;
  review_priority?: number;
  likely_merge_targets?: Array<{ group_key?: string; group_label?: string; similarity?: number } | string>;
  likely_payroll_candidate?: boolean;
};

type LineRow = {
  id: string;
  group_key: string;
  group_label: string;
  final_bucket?: string | null;
  line_type: string;
  category: string;
  subcategory: string;
  inferred_cadence: string;
  cadence_confidence: number;
  cadence_reason: string;
  observed_only: boolean;
  bucket_assignment: string;
  bucket_suggestion: string;
  base_amount: number;
  base_period: string;
  authoritative_field: string;
  source_amount: number;
  source_period: string;
  observed_window_total: number;
  normalized_weekly: number;
  normalized_fortnightly: number;
  normalized_monthly: number;
  normalized_yearly: number;
  impact_on_baseline: string;
  included: boolean;
  transaction_count: number;
  confidence_label: string;
  explanation: string;
  baseline_decision_reason?: string | null;
  notes?: string | null;
  review_reasons: string[];
  modeling_status: string;
  recurrence_state: string;
  is_modeled: boolean;
  modeled_by_default: boolean;
  merchant_confidence: number;
  bucket_confidence: number;
  observed_amount: number;
  observational_monthly_estimate?: number | null;
  observed_frequency_label: string;
  line_trust_level?: string;
  modeling_eligible?: boolean;
  modeling_block_reason?: string | null;
  classification_version?: string | null;
  mapping_source?: string | null;
  line_integrity_status?: string | null;
  duplicate_group_candidates: Array<{ group_key?: string; group_label?: string; similarity?: number } | string>;
  merge_candidate_confidence: number;
};

type MerchantMemoryRow = {
  id: string;
  merchant_key: string;
  merchant_fingerprint?: string | null;
  category: string;
  subcategory: string;
  confidence: number;
  source: string;
  mapping_source?: string | null;
  scope?: string | null;
  usage_count?: number;
  active: boolean;
};

type TxEdit = {
  category: string;
  subcategory: string;
  bucketAssignment: string;
  included: boolean;
  rememberMapping: boolean;
};

type LineEdit = {
  category: string;
  subcategory: string;
  cadence: string;
  included: boolean;
  bucketAssignment: string;
  baseAmount: string;
  basePeriod: string;
  weekly: string;
  fortnightly: string;
  monthly: string;
  yearly: string;
  authoritativeField: string;
  notes: string;
  rememberMapping: boolean;
};

type SafeSection =
  | "income_recurring"
  | "income_irregular"
  | "recurring_baseline"
  | "variable_observed"
  | "one_off"
  | "transfer"
  | "fees"
  | "uncategorized";
type ReviewActionState = {
  pendingAction?: string | null;
  successMessage?: string | null;
  errorMessage?: string | null;
  stillNeedsReview?: boolean;
  remainingReasons?: string[];
};

type ResolvedReviewNotice = {
  key: string;
  label: string;
  message: string;
};

const CADENCE_OPTIONS = ["weekly", "fortnightly", "monthly", "quarterly", "yearly", "irregular", "unknown"];
const BASE_PERIOD_OPTIONS = ["weekly", "fortnightly", "monthly", "quarterly", "yearly"];
const BUCKET_OPTIONS = [
  "recurring_baseline",
  "variable_discretionary",
  "one_off_exceptional",
  "transfer_money_movement",
  "income_recurring",
  "income_irregular",
];
const AUTHORITATIVE_OPTIONS = ["base_amount", "weekly", "fortnightly", "monthly", "yearly"];
const ACTIVE_BUDGET_IMPORT_KEY = "budget_v2_active_import_id";
const BUDGET_V2_SECTION_STATE_KEY = "budget_v2_section_state";
const EXPENSE_TAXONOMY: Record<string, string[]> = {
  "General / Home": ["Rent", "Grocery Shopping", "Power", "Mobile Phone", "Internet", "Water"],
  "Motor Vehicle / Travel": ["Registration", "Wof / Vehicle inspection costs", "Fuel", "General Maintenance", "Public Transport", "Parking"],
  "Insurance": ["Home and Contents", "Health Insurance", "Life Insurance", "Motor Vehicle Insurance", "Income Protection Insurance"],
  "Entertainment": ["Eating out / takeaways", "Movies / Activities"],
  "Health & Fitness": ["Gym membership", "Supplements"],
  "Financing Costs": ["Student loan"],
  "Discretionary": ["Clothing", "Presents", "Charity", "Animals", "youtube", "Chatgpt"],
};
const NON_EXPENSE_CATEGORY_OPTIONS = ["Income", "Transfer / Money Movement", "Refund / Reversal", "Cash Withdrawal", "Debt / Credit", "Expenses"];

type FinancialSectionKey =
  | "income"
  | "baseline"
  | "variable"
  | "oneoff"
  | "transfer"
  | "fees"
  | "uncategorized";

function money(value: number): string {
  return Number(value).toLocaleString("en-AU", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function parseDateValue(value: string | null | undefined): Date | null {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function percent(value: number | null | undefined): string {
  return `${Math.round(Number(value ?? 0) * 100)}%`;
}

function provisionalRecurringIncomeAmount(line: Pick<LineRow, "normalized_monthly" | "base_amount" | "bucket_assignment">): number {
  const normalizedMonthly = Number(line.normalized_monthly ?? 0);
  if (normalizedMonthly > 0) return normalizedMonthly;
  if (line.bucket_assignment === "income_recurring") {
    return Number(line.base_amount ?? 0);
  }
  return 0;
}

export function initialBudgetAmountDraft(line: Pick<LineRow, "base_amount">): string {
  const amount = Number(line.base_amount ?? 0);
  if (amount <= 0) return "";
  return String(amount);
}

function modeledVariableAmount(line: Pick<LineRow, "normalized_monthly" | "base_amount" | "base_period" | "is_modeled">): number {
  if (!line.is_modeled) return 0;
  const normalizedMonthly = Number(line.normalized_monthly ?? 0);
  if (normalizedMonthly > 0) return normalizedMonthly;
  const baseAmount = Number(line.base_amount ?? 0);
  if (baseAmount <= 0) return 0;
  switch (String(line.base_period || "monthly")) {
    case "weekly":
      return baseAmount * (30.44 / 7);
    case "fortnightly":
      return baseAmount * (30.44 / 14);
    case "quarterly":
      return baseAmount / 3;
    case "yearly":
      return baseAmount / 12;
    default:
      return baseAmount;
  }
}

function reviewCardKey(tx: Pick<TxRow, "id" | "group_key">): string {
  return tx.group_key ?? tx.id;
}

function labelTone(label: string): string {
  if (label === "High confidence") return "border-emerald-200 bg-emerald-50 text-emerald-700";
  if (label === "Medium confidence") return "border-amber-200 bg-amber-50 text-amber-700";
  return "border-rose-200 bg-rose-50 text-rose-700";
}

function bucketLabel(bucket: string): string {
  switch (bucket) {
    case "recurring_baseline":
      return "Recurring baseline";
    case "variable_discretionary":
      return "Variable spending";
    case "one_off_exceptional":
      return "One-off / exceptional spending";
    case "transfer_money_movement":
      return "Transfers";
    case "income_recurring":
      return "Recurring income";
    case "income_irregular":
      return "Irregular income";
    default:
      return bucket.replaceAll("_", " ");
  }
}

type DirectionTone = "income" | "expense" | "transfer" | "review";

function directionToneClasses(tone: DirectionTone): { rail: string; badge: string; amount: string; amountChip: string } {
  switch (tone) {
    case "income":
      return {
        rail: "border-l-4 border-l-emerald-500",
        badge: "border-emerald-200 bg-emerald-50 text-emerald-700",
        amount: "text-emerald-900",
        amountChip: "border-emerald-200 bg-emerald-50",
      };
    case "transfer":
      return {
        rail: "border-l-4 border-l-indigo-500",
        badge: "border-indigo-200 bg-indigo-50 text-indigo-700",
        amount: "text-indigo-900",
        amountChip: "border-indigo-200 bg-indigo-50",
      };
    case "review":
      return {
        rail: "border-l-4 border-l-amber-500",
        badge: "border-amber-200 bg-amber-50 text-amber-700",
        amount: "text-slate-950",
        amountChip: "border-amber-200 bg-amber-50",
      };
    case "expense":
    default:
      return {
        rail: "border-l-4 border-l-orange-500",
        badge: "border-orange-200 bg-orange-50 text-orange-700",
        amount: "text-orange-950",
        amountChip: "border-orange-200 bg-orange-50",
      };
  }
}

function reviewCardTone(tx: Pick<TxRow, "movement_type" | "category" | "bucket_assignment">): DirectionTone {
  if (isIncomeReviewItem(tx)) return "income";
  if (isMoneyMovementReviewItem(tx)) return "transfer";
  if (String(tx.movement_type || "").includes("review")) return "review";
  return "expense";
}

function reviewCardTypeLabel(tx: Pick<TxRow, "movement_type" | "category" | "bucket_assignment">): string {
  if (isIncomeReviewItem(tx)) return "Income";
  if (isMoneyMovementReviewItem(tx)) return "Transfer";
  if (String(tx.movement_type || "").includes("fee")) return "Fee";
  return "Expense";
}

function lineToneForSection(section: SafeSection): DirectionTone {
  if (section === "income_recurring" || section === "income_irregular") return "income";
  if (section === "transfer") return "transfer";
  if (section === "uncategorized") return "review";
  return "expense";
}

function lineTypeLabel(section: SafeSection): string {
  if (section === "income_recurring" || section === "income_irregular") return "Income";
  if (section === "transfer") return "Transfer";
  if (section === "fees") return "Fee";
  if (section === "uncategorized") return "Needs review";
  return "Expense";
}

function categoryOptions(currentCategory: string): string[] {
  const options = [...Object.keys(EXPENSE_TAXONOMY), ...NON_EXPENSE_CATEGORY_OPTIONS];
  return options.includes(currentCategory) ? options : [...options, currentCategory];
}

function subcategoryOptions(category: string, currentSubcategory: string): string[] {
  const options = EXPENSE_TAXONOMY[category] ? [...EXPENSE_TAXONOMY[category]] : [];
  if (!currentSubcategory) return options;
  return options.includes(currentSubcategory) ? options : [...options, currentSubcategory];
}

function nextSubcategoryForCategory(category: string, currentSubcategory: string): string {
  const options = subcategoryOptions(category, currentSubcategory);
  return options[0] ?? currentSubcategory;
}

function cadenceLabel(cadence: string, observationalOnly = false): string {
  if (observationalOnly || cadence === "unknown") return "No recurring pattern";
  if (cadence === "fortnightly") return "Fortnightly";
  if (cadence === "weekly") return "Weekly";
  if (cadence === "monthly") return "Monthly";
  if (cadence === "quarterly") return "Quarterly";
  if (cadence === "yearly") return "Yearly";
  if (cadence === "irregular") return "Irregular";
  return cadence;
}

function reasonLabel(reason: string): string {
  switch (reason) {
    case "low_confidence":
      return "Low confidence";
    case "unknown_merchant":
      return "Unknown merchant";
    case "likely_payroll_candidate":
      return "Likely payroll";
    case "duplicate_group_candidate":
      return "Possible duplicate group";
    case "single_occurrence_only":
      return "Single occurrence only";
    case "weak_cadence_evidence":
      return "Weak recurrence evidence";
    case "cadence_ambiguous_material":
      return "Material cadence ambiguity";
    case "parser_anomaly":
      return "Parser anomaly";
    case "suspected_leakage":
      return "Parser leakage detected";
    case "likely_one_off":
      return "Likely one-off";
    default:
      if (reason.includes("transfer")) return "Transfer detection needs confirmation";
      return reason.replaceAll("_", " ");
  }
}

function shortSuggestion(tx: TxRow): string {
  if (tx.likely_payroll_candidate) return "Likely recurring income";
  if (tx.bucket_assignment === "transfer_money_movement") return "Likely transfer or reimbursement";
  if (tx.bucket_assignment === "recurring_baseline") return "Suggested recurring baseline";
  if (tx.bucket_assignment === "one_off_exceptional") return "Likely one-off spend";
  if (tx.bucket_assignment === "variable_discretionary") return "Suggested discretionary spend";
  if (tx.bucket_assignment === "income_irregular") return "Likely irregular income";
  return bucketLabel(tx.bucket_assignment);
}

function hasCadenceReviewReason(tx: TxRow): boolean {
  return tx.review_reasons.some((reason) => {
    const normalized = reasonLabel(String(reason || "")).trim().toLowerCase();
    return normalized.includes("cadence ambiguity") || normalized.includes("recurrence evidence");
  }) || tx.cadence_reason === "insufficient_observations";
}

function cadenceActionLabel(cadence: string): string {
  return cadence === "irregular" ? "Set irregular cadence" : `Set ${cadence} cadence`;
}

function recurringActionLabel(tx: Pick<TxRow, "category" | "bucket_assignment" | "movement_type">): string {
  return isIncomeReviewItem(tx) ? "Treat as recurring income" : "Treat as baseline";
}

function oneOffActionLabel(): string {
  return "Treat as one-off";
}

export function isIncomeReviewItem(tx: Pick<TxRow, "category" | "bucket_assignment" | "movement_type">): boolean {
  return tx.category === "Income" || tx.bucket_assignment === "income_recurring" || tx.bucket_assignment === "income_irregular" || tx.movement_type === "income";
}

export function isMoneyMovementReviewItem(tx: Pick<TxRow, "bucket_assignment" | "movement_type">): boolean {
  return tx.bucket_assignment === "transfer_money_movement" || ["internal_transfer", "debt_payment", "refund", "fee", "cash_withdrawal"].includes(tx.movement_type);
}

export function availableReviewActions(tx: Pick<TxRow, "category" | "bucket_assignment" | "movement_type" | "likely_payroll_candidate" | "group_key" | "review_reasons">): string[] {
  const actions = ["view_details"];
  const income = isIncomeReviewItem(tx);
  const moneyMovement = isMoneyMovementReviewItem(tx);
  if (moneyMovement) {
    if (tx.group_key && hasCadenceReviewReason(tx as TxRow)) {
      actions.unshift("set_cadence");
    }
    return actions;
  }
  actions.unshift("mark_one_off");
  if (income) {
    actions.unshift("mark_recurring");
  } else {
    actions.unshift("keep_discretionary");
    actions.unshift("mark_recurring");
  }
  if (tx.group_key && hasCadenceReviewReason(tx as TxRow)) {
    actions.unshift("set_cadence");
  }
  return actions;
}

function compactDescription(text: string): string {
  return text.length > 92 ? `${text.slice(0, 92)}...` : text;
}

function toneForSection(kind: "neutral" | "income" | "expense" | "warn" | "muted") {
  switch (kind) {
    case "income":
      return "border-emerald-200 bg-emerald-50 text-emerald-900";
    case "expense":
      return "border-sky-200 bg-sky-50 text-sky-950";
    case "warn":
      return "border-amber-200 bg-amber-50 text-amber-900";
    case "muted":
      return "border-slate-200 bg-slate-50 text-slate-700";
    default:
      return "border-slate-200 bg-white text-slate-900";
  }
}

function DisclosureChevron({ expanded, className = "" }: { expanded: boolean; className?: string }) {
  return (
    <span
      aria-hidden="true"
      className={`inline-flex h-7 w-7 items-center justify-center rounded-full border border-slate-200 bg-slate-100/90 text-[15px] font-black leading-none text-slate-800 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] transition-transform duration-200 ${expanded ? "rotate-90" : "rotate-0"} ${className}`}
    >
      ›
    </span>
  );
}

function lineBadges(line: LineRow): string[] {
  const badges: string[] = [];
  if (line.is_modeled) badges.push("Modeled");
  if (line.observed_only) badges.push("No recurring pattern");
  if (!line.included) badges.push("Excluded");
  if (line.modeling_eligible === false && line.bucket_assignment === "recurring_baseline") badges.push("Blocked by trust");
  if (line.mapping_source === "merchant_memory" || line.mapping_source === "manual_override") badges.push("Mapped by memory");
  if (line.mapping_source === "manual_override") badges.push("User-corrected");
  if (line.line_integrity_status === "needs_repair") badges.push("Needs repair");
  return badges;
}

function badgeTone(badge: string): string {
  if (badge === "Modeled") return "border-emerald-200 bg-emerald-50 text-emerald-700";
  if (badge === "No recurring pattern") return "border-sky-200 bg-sky-50 text-sky-700";
  if (badge === "Excluded" || badge === "Blocked by trust") return "border-amber-200 bg-amber-50 text-amber-700";
  if (badge === "Needs repair") return "border-rose-200 bg-rose-50 text-rose-700";
  return "border-slate-200 bg-slate-100 text-slate-700";
}

function categoryGroupTone(category: string): {
  shell: string;
  divider: string;
  headerHint: string;
  footerPill: string;
} {
  const normalized = category.trim().toLowerCase();

  if (normalized.includes("general") || normalized.includes("home") || normalized.includes("utilities")) {
    return {
      shell: "border-sky-200 bg-sky-100/60",
      divider: "border-sky-200/90",
      headerHint: "text-sky-700/80",
      footerPill: "border border-sky-200 bg-white/90 text-sky-900 shadow-[0_8px_24px_rgba(125,170,225,0.12)]",
    };
  }
  if (normalized.includes("discretionary") || normalized.includes("shopping") || normalized.includes("presents")) {
    return {
      shell: "border-amber-200 bg-amber-100/60",
      divider: "border-amber-200/90",
      headerHint: "text-amber-700/80",
      footerPill: "border border-amber-200 bg-white/90 text-amber-900 shadow-[0_8px_24px_rgba(245,181,91,0.12)]",
    };
  }
  if (normalized.includes("entertainment") || normalized.includes("dining")) {
    return {
      shell: "border-rose-200 bg-rose-100/55",
      divider: "border-rose-200/90",
      headerHint: "text-rose-700/80",
      footerPill: "border border-rose-200 bg-white/90 text-rose-900 shadow-[0_8px_24px_rgba(244,166,184,0.12)]",
    };
  }
  if (normalized.includes("health") || normalized.includes("fitness") || normalized.includes("medical")) {
    return {
      shell: "border-emerald-200 bg-emerald-100/55",
      divider: "border-emerald-200/90",
      headerHint: "text-emerald-700/80",
      footerPill: "border border-emerald-200 bg-white/90 text-emerald-900 shadow-[0_8px_24px_rgba(124,221,186,0.12)]",
    };
  }
  if (normalized.includes("income") || normalized.includes("salary") || normalized.includes("payroll")) {
    return {
      shell: "border-emerald-200 bg-emerald-100/55",
      divider: "border-emerald-200/90",
      headerHint: "text-emerald-700/80",
      footerPill: "border border-emerald-200 bg-white/90 text-emerald-900 shadow-[0_8px_24px_rgba(124,221,186,0.12)]",
    };
  }
  if (normalized.includes("transfer") || normalized.includes("money movement") || normalized.includes("reimbursement")) {
    return {
      shell: "border-indigo-200 bg-indigo-100/50",
      divider: "border-indigo-200/90",
      headerHint: "text-indigo-700/80",
      footerPill: "border border-indigo-200 bg-white/90 text-indigo-900 shadow-[0_8px_24px_rgba(165,180,252,0.12)]",
    };
  }
  if (normalized.includes("fee")) {
    return {
      shell: "border-slate-300 bg-slate-100/70",
      divider: "border-slate-200/90",
      headerHint: "text-slate-600",
      footerPill: "border border-slate-200 bg-white/90 text-slate-800 shadow-[0_8px_24px_rgba(148,163,184,0.10)]",
    };
  }
  return {
    shell: "border-violet-200 bg-violet-100/50",
    divider: "border-violet-200/90",
    headerHint: "text-violet-700/75",
    footerPill: "border border-violet-200 bg-white/90 text-violet-900 shadow-[0_8px_24px_rgba(196,181,253,0.12)]",
  };
}

function isTransferLikeLine(line: LineRow): boolean {
  return line.bucket_assignment === "transfer_money_movement" || line.category.toLowerCase().includes("transfer");
}

function isOneOffLikeLine(line: LineRow): boolean {
  return (
    line.bucket_assignment === "one_off_exceptional" ||
    line.recurrence_state === "one_off_candidate" ||
    line.review_reasons.includes("likely_one_off")
  );
}

function isUnsafeForRecurring(line: LineRow): boolean {
  return (
    line.confidence_label === "Needs review" ||
    line.inferred_cadence === "unknown" ||
    line.transaction_count <= 1 ||
    line.review_reasons.includes("single_occurrence_only") ||
    line.review_reasons.includes("weak_cadence_evidence") ||
    line.review_reasons.includes("cadence_ambiguous_material") ||
    line.cadence_confidence < 0.55
  );
}

export function safeSectionForLine(line: LineRow): SafeSection {
  if (line.final_bucket === "income") {
    return line.bucket_assignment === "income_recurring" ? "income_recurring" : "income_irregular";
  }
  if (line.final_bucket === "recurring_baseline_expenses") return "recurring_baseline";
  if (line.final_bucket === "variable_spending") return "variable_observed";
  if (line.final_bucket === "one_off_spending") return "one_off";
  if (line.final_bucket === "transfers") return "transfer";
  if (line.final_bucket === "fees") return "fees";
  if (line.final_bucket === "uncategorized") return "uncategorized";
  if (line.bucket_assignment === "income_recurring") {
    return "income_recurring";
  }
  if (line.bucket_assignment === "income_irregular") return "income_irregular";
  if (line.bucket_assignment === "one_off_exceptional") return "one_off";
  if (line.bucket_assignment === "recurring_baseline") return "recurring_baseline";
  if (line.bucket_assignment === "variable_discretionary") return "variable_observed";
  if (line.bucket_assignment === "transfer_money_movement") return "transfer";
  if (isTransferLikeLine(line)) return "transfer";
  if (isOneOffLikeLine(line) || line.inferred_cadence === "irregular") return "one_off";
  return "variable_observed";
}

export function overviewTotalsFromSummary(summary: ImportSummary | null) {
  const budgetModel = summary?.budget_model;
  return {
    recurringIncome: Number(budgetModel?.recurring_income_monthly ?? 0),
    irregularIncome: Number(budgetModel?.irregular_income_total ?? 0),
    recurringBaseline: Number(budgetModel?.recurring_baseline_monthly ?? 0),
    variableMonthly: Number(budgetModel?.variable_discretionary_monthly ?? 0),
    oneOffObserved: Number(budgetModel?.observed_one_off_total ?? 0),
    transferObserved: Number(budgetModel?.observed_transfer_total ?? 0),
    coreNet: Number(budgetModel?.core_net ?? 0),
    observedNet: Number(budgetModel?.observed_net ?? 0),
    modelingAllowed: Boolean(summary?.trust?.modeling_allowed ?? budgetModel?.modeling_allowed ?? false),
    modelingRestrictions: Array.isArray(summary?.trust?.modeling_restrictions)
      ? summary?.trust?.modeling_restrictions
      : Array.isArray(budgetModel?.modeling_restrictions)
        ? budgetModel?.modeling_restrictions
        : [],
  };
}

export function displayBucketTotalsFromLines(lines: LineRow[]) {
  const totals = {
    income: 0,
    baseline: 0,
    variable: 0,
    oneoff: 0,
    transfer: 0,
    fees: 0,
    uncategorized: 0,
  };

  for (const line of lines) {
    const section = safeSectionForLine(line);
    const observedAmount = Number(line.observed_amount ?? line.observed_window_total ?? 0);
    const modeledVariable = modeledVariableAmount(line);
    const amount = Number(
      section === "income_recurring"
        ? provisionalRecurringIncomeAmount(line)
        : section === "recurring_baseline"
          ? line.normalized_monthly ?? 0
        : section === "variable_observed"
          ? (modeledVariable > 0 ? modeledVariable : observedAmount)
        : observedAmount,
    );
    if (section === "income_recurring" || section === "income_irregular") totals.income += amount;
    else if (section === "recurring_baseline") totals.baseline += amount;
    else if (section === "variable_observed") totals.variable += amount;
    else if (section === "one_off") totals.oneoff += amount;
    else if (section === "transfer") totals.transfer += amount;
    else if (section === "fees") totals.fees += amount;
    else if (section === "uncategorized") totals.uncategorized += amount;
  }

  return totals;
}

export function statementPeriodTotalsFromLines(lines: LineRow[]) {
  const totals = {
    income: 0,
    baseline: 0,
    variable: 0,
    oneoff: 0,
    transfer: 0,
    fees: 0,
    uncategorized: 0,
  };

  for (const line of lines) {
    const section = safeSectionForLine(line);
    const observedAmount = Number(line.observed_amount ?? line.observed_window_total ?? 0);
    if (section === "income_recurring" || section === "income_irregular") totals.income += observedAmount;
    else if (section === "recurring_baseline") totals.baseline += observedAmount;
    else if (section === "variable_observed") totals.variable += observedAmount;
    else if (section === "one_off") totals.oneoff += observedAmount;
    else if (section === "transfer") totals.transfer += observedAmount;
    else if (section === "fees") totals.fees += observedAmount;
    else if (section === "uncategorized") totals.uncategorized += observedAmount;
  }

  return totals;
}


export function needsAttentionItemCount(reviewCards: TxRow[], uncategorizedLines: LineRow[]): number {
  return reviewCards.length + uncategorizedLines.length;
}

export function groupLinesByCategory(lines: LineRow[]): Array<{ category: string; items: LineRow[] }> {
  const groups: Array<{ category: string; items: LineRow[] }> = [];
  const groupIndex = new Map<string, number>();
  for (const line of lines) {
    const category = String(line.category || "Uncategorized");
    const existing = groupIndex.get(category);
    if (existing == null) {
      groupIndex.set(category, groups.length);
      groups.push({ category, items: [line] });
    } else {
      groups[existing].items.push(line);
    }
  }
  return groups;
}

export function preserveReviewCardOrder(previous: TxRow[], next: TxRow[]): TxRow[] {
  if (!previous.length) {
    return [...next].sort((a, b) => Number(b.review_priority ?? 0) - Number(a.review_priority ?? 0));
  }

  const previousIndex = new Map(previous.map((item, index) => [item.group_key || item.id, index]));
  const nextByKey = new Map(next.map((item) => [item.group_key || item.id, item]));

  const preserved: TxRow[] = [];
  for (const previousItem of previous) {
    const key = previousItem.group_key || previousItem.id;
    const current = nextByKey.get(key);
    if (current) preserved.push(current);
  }

  const newItems = next
    .filter((item) => !previousIndex.has(item.group_key || item.id))
    .sort((a, b) => Number(b.review_priority ?? 0) - Number(a.review_priority ?? 0));

  return [...preserved, ...newItems];
}

export function BudgetWorkspaceV2() {
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [importId, setImportId] = useState<string | null>(null);

  const [parsers, setParsers] = useState<ParserInfo[]>([]);
  const [summary, setSummary] = useState<ImportSummary | null>(null);
  const [transactions, setTransactions] = useState<TxRow[]>([]);
  const [needsReview, setNeedsReview] = useState<TxRow[]>([]);
  const [lines, setLines] = useState<LineRow[]>([]);
  const [totals, setTotals] = useState<Record<string, number>>({});
  const [merchantMemory, setMerchantMemory] = useState<MerchantMemoryRow[]>([]);

  const [txEdits, setTxEdits] = useState<Record<string, TxEdit>>({});
  const [lineEdits, setLineEdits] = useState<Record<string, LineEdit>>({});
  const [advancedLine, setAdvancedLine] = useState<Record<string, boolean>>({});
  const [expandedLine, setExpandedLine] = useState<Record<string, TxRow[]>>({});
  const [loadingExpandedLine, setLoadingExpandedLine] = useState<Record<string, boolean>>({});
  const [openLinePanels, setOpenLinePanels] = useState<Record<string, boolean>>({});
  const [openReviewPanels, setOpenReviewPanels] = useState<Record<string, boolean>>({});
  const [reviewActionState, setReviewActionState] = useState<Record<string, ReviewActionState>>({});
  const [resolvedReviewNotices, setResolvedReviewNotices] = useState<ResolvedReviewNotice[]>([]);
  const [needsAttentionOpen, setNeedsAttentionOpen] = useState(false);
  const [importDetailsOpen, setImportDetailsOpen] = useState(false);
  const [fullAuditOpen, setFullAuditOpen] = useState(false);
  const [observedEvidenceOpen, setObservedEvidenceOpen] = useState(false);
  const [categoryGroupsOpen, setCategoryGroupsOpen] = useState<Record<string, boolean>>({});
  const [financialSectionsOpen, setFinancialSectionsOpen] = useState<Record<FinancialSectionKey, boolean>>({
    income: true,
    baseline: true,
    variable: false,
    oneoff: false,
    transfer: false,
    fees: false,
    uncategorized: true,
  });
  const [showExpertTools, setShowExpertTools] = useState(false);

  const [mergeSource, setMergeSource] = useState("");
  const [mergeTarget, setMergeTarget] = useState("");
  const [splitGroup, setSplitGroup] = useState("");
  const [reassignSource, setReassignSource] = useState("");
  const [reassignTarget, setReassignTarget] = useState("");
  const [reassignTxIds, setReassignTxIds] = useState("");
  const sectionRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const reviewCardRefs = useRef<Record<string, HTMLDivElement | null>>({});

  const safeSections = useMemo(
    () =>
      lines.reduce<Record<SafeSection, LineRow[]>>(
        (acc, line) => {
          acc[safeSectionForLine(line)].push(line);
          return acc;
        },
        {
          income_recurring: [],
          income_irregular: [],
          recurring_baseline: [],
          variable_observed: [],
          one_off: [],
          transfer: [],
          fees: [],
          uncategorized: [],
        },
      ),
    [lines],
  );
  const recurringIncomeLines = safeSections.income_recurring;
  const irregularIncomeLines = safeSections.income_irregular;
  const recurringBaselineLines = safeSections.recurring_baseline;
  const discretionaryLines = safeSections.variable_observed;
  const oneOffLines = safeSections.one_off;
  const transferLines = safeSections.transfer;
  const feeLines = safeSections.fees;
  const uncategorizedLines = safeSections.uncategorized;
  const guardedTotals = useMemo(() => overviewTotalsFromSummary(summary), [summary]);
  const displayBucketTotals = useMemo(() => displayBucketTotalsFromLines(lines), [lines]);
  const statementPeriodTotals = useMemo(() => statementPeriodTotalsFromLines(lines), [lines]);
  const reviewCards = useMemo(() => needsReview, [needsReview]);
  const hasAdvancedTools = useMemo(
    () =>
      merchantMemory.length > 0 ||
      lines.some((line) => line.duplicate_group_candidates.length > 0) ||
      reviewCards.some((item) => (item.likely_merge_targets?.length ?? 0) > 0) ||
      transactions.length > 0,
    [lines, merchantMemory.length, reviewCards, transactions.length],
  );

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const raw = window.localStorage.getItem(BUDGET_V2_SECTION_STATE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as {
        needsAttentionOpen?: boolean;
        importDetailsOpen?: boolean;
        fullAuditOpen?: boolean;
        observedEvidenceOpen?: boolean;
        categoryGroupsOpen?: Record<string, boolean>;
        showExpertTools?: boolean;
        financialSectionsOpen?: Partial<Record<FinancialSectionKey, boolean>>;
      };
      if (typeof parsed.needsAttentionOpen === "boolean") setNeedsAttentionOpen(parsed.needsAttentionOpen);
      if (typeof parsed.importDetailsOpen === "boolean") setImportDetailsOpen(parsed.importDetailsOpen);
      if (typeof parsed.fullAuditOpen === "boolean") setFullAuditOpen(parsed.fullAuditOpen);
      if (typeof parsed.observedEvidenceOpen === "boolean") setObservedEvidenceOpen(parsed.observedEvidenceOpen);
      if (parsed.categoryGroupsOpen && typeof parsed.categoryGroupsOpen === "object") setCategoryGroupsOpen(parsed.categoryGroupsOpen);
      if (typeof parsed.showExpertTools === "boolean") setShowExpertTools(parsed.showExpertTools);
      if (parsed.financialSectionsOpen) {
        setFinancialSectionsOpen((prev) => ({ ...prev, ...parsed.financialSectionsOpen }));
      }
    } catch {
      // Ignore malformed persisted UI state.
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(
      BUDGET_V2_SECTION_STATE_KEY,
      JSON.stringify({
        needsAttentionOpen,
        importDetailsOpen,
        fullAuditOpen,
        observedEvidenceOpen,
        categoryGroupsOpen,
        showExpertTools,
        financialSectionsOpen,
      }),
    );
  }, [categoryGroupsOpen, financialSectionsOpen, fullAuditOpen, importDetailsOpen, needsAttentionOpen, observedEvidenceOpen, showExpertTools]);

  useEffect(() => {
    let cancelled = false;

    async function restoreActiveImport() {
      try {
        await loadParsers();
        await loadMerchantMemory();
        const storedImportId =
          typeof window !== "undefined" ? window.localStorage.getItem(ACTIVE_BUDGET_IMPORT_KEY) : null;
        if (storedImportId) {
          try {
            await loadImportState(storedImportId);
            if (!cancelled) setImportId(storedImportId);
            return;
          } catch {
            if (typeof window !== "undefined") {
              window.localStorage.removeItem(ACTIVE_BUDGET_IMPORT_KEY);
            }
          }
        }

        const latest = await customFetch<{ data: ImportSummary }>(
          "/api/v1/control-center/budget/imports/latest",
          { method: "GET" },
        );
        if (latest.data?.import_id && !cancelled) {
          setImportId(latest.data.import_id);
          if (typeof window !== "undefined") {
            window.localStorage.setItem(ACTIVE_BUDGET_IMPORT_KEY, latest.data.import_id);
          }
          await loadImportState(latest.data.import_id);
        }
      } catch {
        // No existing budget import yet; keep the workspace empty.
      }
    }

    void restoreActiveImport();
    return () => {
      cancelled = true;
    };
  }, []);

  async function loadParsers() {
    const res = await customFetch<{ data: { parsers: ParserInfo[] } }>("/api/v1/control-center/budget/parsers", {
      method: "GET",
    });
    setParsers(res.data.parsers);
  }

  async function loadMerchantMemory() {
    const res = await customFetch<{ data: { items: MerchantMemoryRow[] } }>("/api/v1/control-center/budget/merchant-memory", {
      method: "GET",
    });
    setMerchantMemory(Array.isArray(res.data?.items) ? res.data.items : []);
  }

  async function loadImportStateWithRetry(id: string, attempts = 3) {
    let lastError: unknown = null;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        await loadImportState(id);
        return;
      } catch (err) {
        lastError = err;
        if (attempt >= attempts) break;
        await new Promise((resolve) => window.setTimeout(resolve, 400 * attempt));
      }
    }
    throw lastError instanceof Error ? lastError : new Error("Failed to load imported budget state");
  }

  async function loadImportState(id: string) {
    const [summaryRes, txRes, reviewRes, lineRes] = await Promise.all([
      customFetch<{ data: ImportSummary }>(`/api/v1/control-center/budget/imports/${id}`, { method: "GET" }),
      customFetch<{ data: { items: TxRow[] } }>(`/api/v1/control-center/budget/imports/${id}/transactions`, {
        method: "GET",
      }),
      customFetch<{ data: { items: TxRow[] } }>(`/api/v1/control-center/budget/imports/${id}/needs-review`, {
        method: "GET",
      }),
      customFetch<{ data: { items: LineRow[]; totals: Record<string, number> } }>(
        `/api/v1/control-center/budget/imports/${id}/lines`,
        { method: "GET" },
      ),
    ]);
    setSummary(summaryRes.data);
    setTransactions(Array.isArray(txRes.data?.items) ? txRes.data.items : []);
    setNeedsReview((prev) => preserveReviewCardOrder(prev, Array.isArray(reviewRes.data?.items) ? reviewRes.data.items : []));
    setLines(Array.isArray(lineRes.data?.items) ? lineRes.data.items : []);
    setTotals(lineRes.data?.totals || {});
    setTxEdits({});
    setLineEdits({});
    setExpandedLine({});
    setLoadingExpandedLine({});
    setOpenLinePanels({});
    setOpenReviewPanels({});
    setReviewActionState((prev) => {
      const next: Record<string, ReviewActionState> = {};
      for (const [key, value] of Object.entries(prev)) {
        if (value.pendingAction || value.successMessage || value.errorMessage) {
          next[key] = value;
        }
      }
      return next;
    });
    setImportId(id);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(ACTIVE_BUDGET_IMPORT_KEY, id);
    }
    return {
      summary: summaryRes.data,
      transactions: Array.isArray(txRes.data?.items) ? txRes.data.items : [],
      needsReview: Array.isArray(reviewRes.data?.items) ? reviewRes.data.items : [],
      lines: Array.isArray(lineRes.data?.items) ? lineRes.data.items : [],
      totals: lineRes.data?.totals || {},
    };
  }

  async function applyOverrides(
    operations: Array<{
      target_type: "transaction" | "group";
      target_id: string;
      operation: string;
      payload: Record<string, unknown>;
    }>,
    successMessage: string,
  ) {
    if (!importId) return;
    setBusy(true);
    setError(null);
    try {
      await customFetch<{ data: { applied: number } }>(`/api/v1/control-center/budget/imports/${importId}/overrides`, {
        method: "PATCH",
        body: JSON.stringify({ operations }),
      });
      await customFetch(`/api/v1/control-center/budget/imports/${importId}/recompute`, { method: "POST" });
      await loadImportState(importId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to apply override");
    } finally {
      setBusy(false);
    }
  }

  function setReviewCardRef(key: string, node: HTMLDivElement | null) {
    reviewCardRefs.current[key] = node;
  }

  function queueResolvedReviewNotice(key: string, label: string, message: string) {
    setResolvedReviewNotices((prev) => [...prev.filter((item) => item.key !== key), { key, label, message }]);
    window.setTimeout(() => {
      setResolvedReviewNotices((prev) => prev.filter((item) => item.key !== key));
    }, 2800);
  }

  async function applyReviewAction(
    tx: TxRow,
    operations: Array<{
      target_type: "transaction" | "group";
      target_id: string;
      operation: string;
      payload: Record<string, unknown>;
    }>,
    successMessage: string,
    actionLabel: string,
  ) {
    if (!importId) return;
    const key = reviewCardKey(tx);
    setReviewActionState((prev) => ({
      ...prev,
      [key]: { pendingAction: actionLabel, successMessage: null, errorMessage: null, stillNeedsReview: false },
    }));
    setError(null);
    try {
      await customFetch<{ data: { applied: number } }>(`/api/v1/control-center/budget/imports/${importId}/overrides`, {
        method: "PATCH",
        body: JSON.stringify({ operations }),
      });
      await customFetch(`/api/v1/control-center/budget/imports/${importId}/recompute`, { method: "POST" });
      const refreshed = await loadImportState(importId);

      const matchingReviewItem = refreshed.needsReview.find(
        (item) => item.id === tx.id || (tx.group_key && item.group_key === tx.group_key),
      );
      if (matchingReviewItem) {
        const remainingReasons = (matchingReviewItem.review_reasons || [])
          .filter((reason) => reason !== "likely_payroll_candidate")
          .map((reason) => reasonLabel(reason));
        const remainingReasonsText = remainingReasons.length
          ? `Saved. Still needs review: ${remainingReasons.join(", ")}.`
          : "Saved, but this item still needs review.";
        setReviewActionState((prev) => ({
          ...prev,
          [key]: {
            pendingAction: null,
            successMessage: remainingReasonsText,
            errorMessage: null,
            stillNeedsReview: true,
            remainingReasons,
          },
        }));
      } else {
        setReviewActionState((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
        queueResolvedReviewNotice(key, tx.merchant_candidate || compactDescription(tx.raw_description), successMessage);
      }
    } catch (err) {
      setReviewActionState((prev) => ({
        ...prev,
        [key]: {
          pendingAction: null,
          successMessage: null,
          errorMessage: err instanceof Error ? err.message : "Failed to apply review action",
          stillNeedsReview: false,
        },
      }));
    }
  }

  async function onImport() {
    if (!file || busy) return;
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const form = new FormData();
      form.append("file", file);
      const result = await customFetch<{ data: { import_id: string; transaction_count: number } }>(
        "/api/v1/control-center/budget/imports",
        {
          method: "POST",
          body: form,
        },
      );
      setImportId(result.data.import_id);
      if (typeof window !== "undefined") {
        window.localStorage.setItem(ACTIVE_BUDGET_IMPORT_KEY, result.data.import_id);
      }
      try {
        await loadImportStateWithRetry(result.data.import_id);
      } catch (loadErr) {
        setMessage(`Import finished, but loading the budget view failed. Refresh to reopen import ${result.data.import_id}.`);
        throw loadErr;
      }
      await loadParsers();
      await loadMerchantMemory();
      setMessage(`Imported ${result.data.transaction_count} transactions.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Import failed");
    } finally {
      setBusy(false);
    }
  }

  async function onResetBudget() {
    if (busy) return;
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const result = await customFetch<{ data: { reset: boolean; deleted_import_count: number } }>(
        "/api/v1/control-center/budget/reset",
        { method: "POST" },
      );
      if (typeof window !== "undefined") {
        window.localStorage.removeItem(ACTIVE_BUDGET_IMPORT_KEY);
      }
      setImportId(null);
      setSummary(null);
      setTransactions([]);
      setNeedsReview([]);
      setLines([]);
      setTotals({});
      setMerchantMemory([]);
      setTxEdits({});
      setLineEdits({});
      setExpandedLine({});
      setLoadingExpandedLine({});
      setOpenLinePanels({});
      setOpenReviewPanels({});
      setMessage(`Budget reset. Deleted ${result.data.deleted_import_count} saved import(s).`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Reset failed");
    } finally {
      setBusy(false);
    }
  }

  async function retireMerchantMemory(memoryId: string) {
    if (busy) return;
    setBusy(true);
    setError(null);
    try {
      await customFetch(`/api/v1/control-center/budget/merchant-memory/${memoryId}`, { method: "DELETE" });
      await loadMerchantMemory();
      setMessage("Merchant memory retired.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to retire merchant memory");
    } finally {
      setBusy(false);
    }
  }

  function txDraft(tx: TxRow): TxEdit {
    return txEdits[tx.id] ?? {
      category: tx.category,
      subcategory: tx.subcategory,
      bucketAssignment: tx.bucket_assignment,
      included: tx.included,
      rememberMapping: false,
    };
  }

  function lineDraft(row: LineRow): LineEdit {
    return (
      lineEdits[row.group_key] ?? {
        category: row.category,
        subcategory: row.subcategory,
        cadence: row.inferred_cadence,
        included: row.included,
        bucketAssignment: row.bucket_assignment,
        baseAmount: initialBudgetAmountDraft(row),
        basePeriod: row.base_period,
        weekly: String(row.normalized_weekly),
        fortnightly: String(row.normalized_fortnightly),
        monthly: String(row.normalized_monthly),
        yearly: String(row.normalized_yearly),
        authoritativeField: row.authoritative_field,
        notes: row.notes ?? "",
        rememberMapping: false,
      }
    );
  }

  async function loadLineTransactions(groupKey: string) {
    if (!importId) return;
    if (expandedLine[groupKey]) return;
    setLoadingExpandedLine((prev) => ({ ...prev, [groupKey]: true }));
    try {
      const res = await customFetch<{ data: { items: TxRow[] } }>(
        `/api/v1/control-center/budget/imports/${importId}/lines/${encodeURIComponent(groupKey)}/transactions`,
        { method: "GET" },
      );
      setExpandedLine((prev) => ({ ...prev, [groupKey]: res.data.items || [] }));
    } finally {
      setLoadingExpandedLine((prev) => ({ ...prev, [groupKey]: false }));
    }
  }

  async function toggleLinePanel(groupKey: string) {
    const opening = !openLinePanels[groupKey];
    setOpenLinePanels((prev) => ({ ...prev, [groupKey]: opening }));
    if (opening) {
      await loadLineTransactions(groupKey);
    }
  }

  async function toggleReviewPanel(tx: TxRow) {
    const panelKey = reviewCardKey(tx);
    const opening = !openReviewPanels[panelKey];
    setOpenReviewPanels((prev) => ({ ...prev, [panelKey]: opening }));
    if (opening && tx.group_key) {
      await loadLineTransactions(tx.group_key);
    }
  }

  function renderImportBanner(title: string, items: string[], tone: "warn" | "muted") {
    if (!items.length) return null;
    return (
      <details className={`rounded-xl border p-3 ${toneForSection(tone)}`}>
        <summary className="cursor-pointer text-sm font-semibold">{title}</summary>
        <ul className="mt-2 list-disc space-y-1 pl-4 text-sm">
          {items.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      </details>
    );
  }

  function setSectionRef(key: string, node: HTMLDivElement | null) {
    sectionRefs.current[key] = node;
  }

  function runAnchoredToggle(key: string, mutate: () => void) {
    const sectionNode = sectionRefs.current[key];
    const beforeTop = sectionNode?.getBoundingClientRect().top ?? null;
    mutate();
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        const nextNode = sectionRefs.current[key];
        if (!nextNode || beforeTop === null) return;
        const afterTop = nextNode.getBoundingClientRect().top;
        const delta = afterTop - beforeTop;
        if (Math.abs(delta) > 1) {
          window.scrollBy({ top: delta, behavior: "auto" });
        }
      });
    });
  }

  function toggleAnchoredSection(
    key: string,
    setter: (updater: (previous: boolean) => boolean) => void,
  ) {
    runAnchoredToggle(key, () => setter((previous) => !previous));
  }

  function toggleAnchoredFinancialSection(key: FinancialSectionKey) {
    runAnchoredToggle(key, () => {
      setFinancialSectionsOpen((prev) => ({ ...prev, [key]: !prev[key] }));
    });
  }

  function toggleCategoryGroup(groupKey: string) {
    setCategoryGroupsOpen((prev) => ({ ...prev, [groupKey]: !(prev[groupKey] ?? false) }));
  }

  function renderOverviewCard(
    cardKey: string,
    title: string,
    value: number,
    subtitle: string,
    tone: "income" | "expense" | "warn" | "muted",
  ) {
    return (
      <div data-cy="budget-summary-card" data-card-key={cardKey} className={`rounded-2xl border p-3 sm:p-4 ${toneForSection(tone)}`}>
        <div className="text-[11px] font-semibold uppercase tracking-[0.18em] opacity-80">{title}</div>
        <div className="mt-2 text-xl font-semibold sm:text-2xl">{money(Number(value ?? 0))}</div>
        <div className="mt-1 text-xs opacity-80">{subtitle}</div>
      </div>
    );
  }

  function financialSectionTone(kind: "income" | "baseline" | "variable" | "oneoff" | "transfer" | "fees" | "uncategorized") {
    switch (kind) {
      case "income":
        return "text-emerald-700";
      case "baseline":
        return "text-sky-700";
      case "variable":
        return "text-amber-700";
      case "oneoff":
        return "text-rose-700";
      case "transfer":
        return "text-violet-700";
      case "fees":
        return "text-slate-700";
      case "uncategorized":
        return "text-amber-700";
    }
  }

  function linePrimaryAmount(line: LineRow, kind: "income" | "baseline" | "variable" | "oneoff" | "transfer" | "fees" | "uncategorized") {
    const safeSection = safeSectionForLine(line);
    const observedAmount = Number(line.observed_amount ?? line.observed_window_total ?? 0);
    const modeledVariable = modeledVariableAmount(line);
    if (safeSection === "income_recurring") return provisionalRecurringIncomeAmount(line);
    if (safeSection === "income_irregular") return Number(line.observed_amount ?? 0);
    if (safeSection === "recurring_baseline") return Number(line.normalized_monthly ?? 0);
    if (safeSection === "variable_observed") return modeledVariable > 0 ? modeledVariable : observedAmount;
    if (safeSection === "fees") return observedAmount;
    if (safeSection === "uncategorized") return observedAmount;
    return Number(line.observed_amount ?? 0);
  }

  function linePrimaryAmountLabel(line: LineRow, kind: "income" | "baseline" | "variable" | "oneoff" | "transfer" | "fees" | "uncategorized") {
    const safeSection = safeSectionForLine(line);
    if (safeSection === "income_recurring") return "Monthly income";
    if (safeSection === "income_irregular") return "Observed irregular income";
    if (safeSection === "recurring_baseline") return "Monthly budget";
    if (safeSection === "variable_observed") return modeledVariableAmount(line) > 0 ? "Monthly budget" : "Observed in statement";
    if (safeSection === "one_off") return "Observed amount";
    if (safeSection === "fees") return "Observed fees";
    if (safeSection === "uncategorized") return "Needs review";
    return "Observed transfer";
  }

  function sectionSubtotal(items: LineRow[], kind: "income" | "baseline" | "variable" | "oneoff" | "transfer" | "fees" | "uncategorized") {
    switch (kind) {
      case "income":
        return displayBucketTotals.income;
      case "baseline":
        return displayBucketTotals.baseline;
      case "variable":
        return displayBucketTotals.variable;
      case "oneoff":
        return displayBucketTotals.oneoff;
      case "transfer":
        return displayBucketTotals.transfer;
      case "fees":
        return displayBucketTotals.fees;
      case "uncategorized":
        return displayBucketTotals.uncategorized;
      default:
        return items.reduce((sum, line) => sum + linePrimaryAmount(line, kind), 0);
    }
  }

  function renderFinancialLineCard(
    line: LineRow,
    kind: "income" | "baseline" | "variable" | "oneoff" | "transfer" | "fees" | "uncategorized",
  ) {
    const draft = lineDraft(line);
    const isExpanded = !!openLinePanels[line.group_key];
    const advanced = !!advancedLine[line.group_key];
    const mergeCandidates = line.duplicate_group_candidates.map((candidate) =>
      typeof candidate === "string" ? candidate : candidate.group_label || candidate.group_key || "Possible merge",
    );
    const safeSection = safeSectionForLine(line);
    const primaryAmount = linePrimaryAmount(line, kind);
    const primaryLabel = linePrimaryAmountLabel(line, kind);
    const observedMonthlyEstimate =
      safeSection === "variable_observed" && line.observational_monthly_estimate != null
        ? Number(line.observational_monthly_estimate)
        : null;
    const badges = lineBadges(line);
    const tone = directionToneClasses(lineToneForSection(safeSection));
    const typeLabel = lineTypeLabel(safeSection);
    const inlineBudgetDraftChanged =
      draft.baseAmount !== initialBudgetAmountDraft(line) || draft.basePeriod !== line.base_period;

    return (
      <div
        key={line.id}
        data-cy="budget-line-row"
        data-group-key={line.group_key}
        className={`rounded-2xl border border-slate-200 bg-slate-50/60 ${tone.rail}`}
      >
        <div className="px-4 py-3">
          <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(360px,1.1fr)_288px] lg:items-center">
            <div className="min-w-0">
              <div className="flex flex-wrap items-start gap-2">
                <div className="min-w-0 flex-1">
                  <button
                    type="button"
                    data-cy="budget-line-edit-toggle"
                    aria-expanded={isExpanded}
                    onClick={() => void toggleLinePanel(line.group_key)}
                    className="inline-flex max-w-full items-center gap-2 truncate text-left text-base font-semibold text-slate-900 transition hover:text-slate-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-300 focus-visible:ring-offset-2"
                  >
                    <span className="truncate">{line.group_label}</span>
                    <DisclosureChevron expanded={isExpanded} className="h-6 w-6 shrink-0" />
                  </button>
                  <div className="mt-1 text-sm text-slate-500">
                    {line.transaction_count} transactions
                    {safeSection === "income_irregular" ? " • Irregular income" : ""}
                  </div>
                </div>
              </div>

              {badges.length ? (
                <div className="mt-3 flex flex-wrap gap-1.5">
                  {badges.map((badge) => (
                    <span key={badge} className={`rounded-full border px-2 py-0.5 text-[11px] ${badgeTone(badge)}`}>{badge}</span>
                  ))}
                </div>
              ) : null}

              <div className="mt-3 grid gap-2">
                <div className="min-w-0">
                  <div className="text-sm font-medium text-slate-700">{line.category}</div>
                  <div className="mt-0.5 text-sm text-slate-500">{line.subcategory}</div>
                </div>
              </div>
            </div>

            <div className="rounded-2xl border border-slate-200 bg-white/80 p-3">
              <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_200px_auto] md:items-end">
                <div className="space-y-1">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Budget amount</div>
                  <input
                    data-cy="budget-line-inline-base-amount"
                    className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm"
                    value={draft.baseAmount}
                    onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, baseAmount: e.target.value } }))}
                    placeholder="Set manual budget amount"
                  />
                </div>
                <div className="space-y-1">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Budget period</div>
                  <select
                    data-cy="budget-line-inline-base-period"
                    className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm"
                    value={draft.basePeriod}
                    onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, basePeriod: e.target.value } }))}
                  >
                    {BASE_PERIOD_OPTIONS.map((option) => <option key={option} value={option}>{cadenceLabel(option)}</option>)}
                  </select>
                </div>
                <Button
                  data-cy="budget-line-inline-save"
                  type="button"
                  size="sm"
                  variant="secondary"
                  className="md:min-w-[84px]"
                  disabled={busy}
                  onClick={() => {
                    void applyOverrides(
                      [
                        {
                          target_type: "group",
                          target_id: line.group_key,
                          operation: "set_base_amount_period",
                          payload: { base_amount: Number(draft.baseAmount || 0), base_period: draft.basePeriod },
                        },
                      ],
                      `Saved budget amount for ${line.group_label}.`,
                    );
                  }}
                >
                  Save
                </Button>
              </div>
              <div className="mt-1 text-[11px] text-slate-500">
                {inlineBudgetDraftChanged ? "Unsaved changes" : "Using current saved values"}
              </div>
            </div>

            <div className="w-full rounded-2xl border border-slate-200 bg-white/85 p-3 lg:w-[288px] lg:flex-shrink-0">
              <div className="min-w-0">
                <div className={`inline-flex rounded-full border px-4 py-1.5 text-[1.15rem] font-semibold leading-none ${tone.amountChip} ${tone.amount}`}>
                  {money(Number(primaryAmount))}
                </div>
                <div className="mt-2 text-sm font-semibold text-slate-700">
                  {primaryLabel}
                  {kind === "variable" ? " • observational" : ""}
                </div>
                <div className="mt-1 text-[12px] leading-4 text-slate-500">
                  {!line.included
                    ? "Visible, but excluded from calculations"
                    : kind === "variable"
                      ? observedMonthlyEstimate !== null
                        ? `Monthly estimate ${money(observedMonthlyEstimate)}`
                        : "Statement-period estimate only"
                      : kind === "oneoff"
                        ? "Observed once-off amount"
                        : kind === "transfer"
                          ? "Observed transfer amount"
                          : safeSection === "income_irregular"
                            ? line.observed_frequency_label || "Irregular income outside core net"
                            : line.is_modeled
                              ? `${cadenceLabel(line.inferred_cadence)} budget line`
                              : line.observed_frequency_label || "Observed only"}
                </div>
                <div className="mt-2">
                  <span className={`rounded-full border px-2.5 py-1 text-[11px] ${labelTone(line.confidence_label)}`}>
                    {line.confidence_label}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {isExpanded ? (
          <div data-cy="budget-line-editor" className="border-t border-slate-200 bg-white px-3 py-3 sm:px-4 sm:py-4">
            <div className="grid gap-3 sm:gap-4 xl:grid-cols-[1.1fr_0.9fr]">
              <div className="space-y-3 sm:space-y-4">
                <div data-cy="budget-line-underlying-panel" className="rounded-xl border border-slate-200 p-3 sm:p-4">
                  <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Why this line exists</div>
                  <div className="mt-2 space-y-2 text-sm text-slate-700">
                    <div>{line.explanation}</div>
                    <div>Cadence reasoning: {line.cadence_reason || "No cadence explanation available."}</div>
                    <div>{line.baseline_decision_reason ?? "This line has not been explicitly promoted beyond its current bucket."}</div>
                    <div>Trust: {line.line_trust_level || "needs_review"}{line.modeling_block_reason ? ` • ${line.modeling_block_reason}` : ""}</div>
                    <div>Integrity: {line.line_integrity_status || "verified"} • Mapping source: {line.mapping_source || "rule"} • Classification version: {line.classification_version || "-"}</div>
                    {mergeCandidates.length ? <div>Possible merge candidates: {mergeCandidates.join(", ")}</div> : null}
                  </div>
                </div>

                <div className="grid gap-3 sm:grid-cols-3">
                  <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Merchant confidence</div>
                    <div className="mt-1 text-lg font-semibold text-slate-900">{percent(line.merchant_confidence)}</div>
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Cadence confidence</div>
                    <div className="mt-1 text-lg font-semibold text-slate-900">{percent(line.cadence_confidence)}</div>
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Bucket confidence</div>
                    <div className="mt-1 text-lg font-semibold text-slate-900">{percent(line.bucket_confidence)}</div>
                  </div>
                </div>

                <div className="rounded-xl border border-slate-200 p-3 sm:p-4">
                  <div className="mb-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Edit this line</div>
                  <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Classification</div>
                  <div className="grid gap-3 md:grid-cols-2">
                    <select
                      data-cy="budget-line-edit-category"
                      className="rounded-xl border border-slate-300 px-3 py-2 text-sm"
                      value={draft.category}
                      onChange={(e) => {
                        const nextCategory = e.target.value;
                        setLineEdits((prev) => ({
                          ...prev,
                          [line.group_key]: {
                            ...draft,
                            category: nextCategory,
                            subcategory: nextSubcategoryForCategory(nextCategory, draft.subcategory),
                          },
                        }));
                      }}
                    >
                      {categoryOptions(draft.category).map((option) => <option key={option} value={option}>{option}</option>)}
                    </select>
                    <select
                      data-cy="budget-line-edit-subcategory"
                      className="rounded-xl border border-slate-300 px-3 py-2 text-sm"
                      value={draft.subcategory}
                      onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, subcategory: e.target.value } }))}
                    >
                      {subcategoryOptions(draft.category, draft.subcategory).map((option) => <option key={option} value={option}>{option}</option>)}
                    </select>
                    <select data-cy="budget-line-edit-bucket" className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.bucketAssignment} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, bucketAssignment: e.target.value } }))}>
                      {BUCKET_OPTIONS.map((option) => <option key={option} value={option}>{bucketLabel(option)}</option>)}
                    </select>
                    <select data-cy="budget-line-edit-cadence" className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.cadence} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, cadence: e.target.value } }))}>
                      {CADENCE_OPTIONS.map((option) => <option key={option} value={option}>{cadenceLabel(option)}</option>)}
                    </select>
                  </div>
                  <div className="mt-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Budget math</div>
                  <div className="mt-3 flex flex-col gap-2 text-sm text-slate-600 sm:flex-row sm:flex-wrap sm:gap-3">
                    <label className="flex items-center gap-2">
                      <input data-cy="budget-line-edit-include" type="checkbox" checked={draft.included} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, included: e.target.checked } }))} />
                      Include in budget
                    </label>
                    <label className="flex items-center gap-2">
                      <input data-cy="budget-line-edit-remember" type="checkbox" checked={draft.rememberMapping} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, rememberMapping: e.target.checked } }))} />
                      Remember this merchant mapping
                    </label>
                  </div>
                  <textarea data-cy="budget-line-edit-notes" className="mt-3 min-h-[88px] w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" placeholder="Why you changed it" value={draft.notes} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, notes: e.target.value } }))} />
                  <div className="mt-3 flex flex-col gap-2 sm:flex-row sm:flex-wrap">
                    <Button
                      className="w-full sm:w-auto"
                      data-cy="budget-line-save"
                      type="button"
                      size="sm"
                      variant="secondary"
                      disabled={busy}
                      onClick={() => {
                        const ops: Array<{ target_type: "transaction" | "group"; target_id: string; operation: string; payload: Record<string, unknown> }> = [
                          { target_type: "group", target_id: line.group_key, operation: "set_category", payload: { category: draft.category } },
                          { target_type: "group", target_id: line.group_key, operation: "set_subcategory", payload: { subcategory: draft.subcategory } },
                          { target_type: "group", target_id: line.group_key, operation: "set_bucket_assignment", payload: { bucket_assignment: draft.bucketAssignment } },
                          { target_type: "group", target_id: line.group_key, operation: "set_cadence", payload: { cadence: draft.cadence } },
                          { target_type: "group", target_id: line.group_key, operation: "set_include", payload: { included: draft.included } },
                          { target_type: "group", target_id: line.group_key, operation: "set_base_amount_period", payload: { base_amount: Number(draft.baseAmount || 0), base_period: draft.basePeriod } },
                          { target_type: "group", target_id: line.group_key, operation: "set_notes", payload: { notes: draft.notes } },
                        ];
                        if (advanced) {
                          ops.push({
                            target_type: "group",
                            target_id: line.group_key,
                            operation: "set_authoritative_period_values",
                            payload: {
                              authoritative_field: draft.authoritativeField,
                              weekly: Number(draft.weekly || 0),
                              fortnightly: Number(draft.fortnightly || 0),
                              monthly: Number(draft.monthly || 0),
                              yearly: Number(draft.yearly || 0),
                            },
                          });
                        }
                        if (draft.rememberMapping) {
                          ops.push({
                            target_type: "group",
                            target_id: line.group_key,
                            operation: "remember_mapping",
                            payload: { category: draft.category, subcategory: draft.subcategory },
                          });
                        }
                        void applyOverrides(ops, `Saved overrides for ${line.group_label}.`);
                      }}
                    >
                      Save changes
                    </Button>
                  </div>
                </div>
              </div>

              <div className="space-y-4">
                <details className="rounded-xl border border-slate-200 p-3 sm:p-4" open={advanced}>
                  <summary className="cursor-pointer text-xs font-semibold uppercase tracking-[0.16em] text-slate-500" onClick={(e) => { e.preventDefault(); setAdvancedLine((prev) => ({ ...prev, [line.group_key]: !prev[line.group_key] })); }}>
                    Advanced overrides
                  </summary>
                  {advanced ? (
                    <div className="mt-3 grid gap-2 sm:grid-cols-2">
                      <select className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.authoritativeField} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, authoritativeField: e.target.value } }))}>
                        {AUTHORITATIVE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                      </select>
                      <div />
                      <input className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.weekly} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, weekly: e.target.value } }))} placeholder="Weekly" />
                      <input className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.fortnightly} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, fortnightly: e.target.value } }))} placeholder="Fortnightly" />
                      <input className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.monthly} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, monthly: e.target.value } }))} placeholder="Monthly" />
                      <input className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.yearly} onChange={(e) => setLineEdits((prev) => ({ ...prev, [line.group_key]: { ...draft, yearly: e.target.value } }))} placeholder="Yearly" />
                    </div>
                  ) : (
                    <div className="mt-3 space-y-1 text-sm text-slate-600">
                      <div>Weekly: {money(Number(line.normalized_weekly))}</div>
                      <div>Fortnightly: {money(Number(line.normalized_fortnightly))}</div>
                      <div>Yearly: {money(Number(line.normalized_yearly))}</div>
                      <div>Observed amount: {money(Number(line.observed_amount))}</div>
                      <div>Observed frequency: {line.observed_frequency_label || "-"}</div>
                    </div>
                  )}
                </details>

                <div className="rounded-xl border border-slate-200 p-3 sm:p-4">
                  <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Underlying transactions</div>
                  <div className="mt-3 max-h-80 space-y-2 overflow-auto">
                    {loadingExpandedLine[line.group_key] ? (
                      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-4 text-sm text-slate-500">
                        Loading underlying transactions...
                      </div>
                    ) : (expandedLine[line.group_key] || []).length > 0 ? (
                      (expandedLine[line.group_key] || []).map((tx) => (
                        <div key={tx.id} className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-700">
                          <div className="flex items-center justify-between gap-3">
                            <div className="font-medium text-slate-900">{tx.transaction_date ?? "-"}</div>
                            <div className={Number(tx.amount) >= 0 ? "font-medium text-emerald-900" : "font-medium text-orange-900"}>{money(Number(tx.amount))}</div>
                          </div>
                          <div className="mt-2 text-slate-700">{tx.raw_description}</div>
                          <div className="mt-2 text-xs text-slate-500">{tx.category} / {tx.subcategory}</div>
                        </div>
                      ))
                    ) : (
                      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-4 text-sm text-slate-500">
                        No underlying transactions were returned for this line. If the line count above is non-zero, the group membership is out of sync and needs recompute.
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    );
  }

  function renderFinancialSection(
    sectionKey: FinancialSectionKey,
    title: string,
    subtitle: string,
    items: LineRow[],
    kind: "income" | "baseline" | "variable" | "oneoff" | "transfer" | "fees" | "uncategorized",
  ) {
    const accent = financialSectionTone(kind);
    const subtotal = sectionSubtotal(items, kind);
    const categoryGroups = groupLinesByCategory(items).map((group) => ({
      ...group,
      subtotal: group.items.reduce((sum, line) => sum + linePrimaryAmount(line, kind), 0),
    }));
    const isOpen = financialSectionsOpen[sectionKey];
    const contentId = `budget-section-${sectionKey}`;
    return (
      <div
        ref={(node) => setSectionRef(sectionKey, node)}
        data-cy="budget-section"
        data-section-key={sectionKey}
        className="rounded-[28px] border border-slate-200 bg-white shadow-sm"
      >
        <button
          type="button"
          data-cy="budget-section-toggle"
          className="w-full px-4 py-3 text-left sm:py-4"
          aria-expanded={isOpen}
          aria-controls={contentId}
          onClick={() => toggleAnchoredFinancialSection(sectionKey)}
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className={`inline-flex items-center gap-2 text-sm font-semibold ${accent}`}>
                <span>{title}</span>
                <DisclosureChevron expanded={isOpen} className="h-6 w-6" />
              </div>
              <div className="mt-1 text-xs text-slate-500">{subtitle}</div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <div className="rounded-full bg-slate-100 px-3 py-1 text-xs text-slate-600">{items.length} lines</div>
              <div className="rounded-full bg-slate-950 px-3 py-1 text-xs text-white">{money(subtotal)}</div>
            </div>
          </div>
        </button>
        {isOpen ? <div className="border-t border-slate-200" /> : null}
        <div id={contentId} hidden={!isOpen} className="px-3 py-3 sm:px-4 sm:py-4">
          {items.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-5 text-sm text-slate-500">
              Nothing to show in this section for the current import.
            </div>
          ) : (
            <div className="space-y-3">
              {categoryGroups.map((group) => (
                (() => {
                  const groupKey = `${sectionKey}:${group.category}`;
                  const isGroupOpen = categoryGroupsOpen[groupKey] ?? false;
                  const groupTone = categoryGroupTone(group.category);
                  return (
                <div
                  key={`${sectionKey}-${group.category}`}
                  data-cy="budget-category-group"
                  className={`rounded-[24px] border ${groupTone.shell} shadow-[0_12px_32px_rgba(15,23,42,0.03)] backdrop-blur-[1px] sm:rounded-[28px]`}
                >
                  <button
                    type="button"
                    className="w-full px-3 py-1.5 text-center sm:px-4 sm:py-2"
                    aria-expanded={isGroupOpen}
                    data-cy="budget-category-group-toggle"
                    onClick={() => toggleCategoryGroup(groupKey)}
                  >
                  <div className={`flex min-h-[82px] flex-col items-center justify-center gap-1 border-b px-3 py-1.5 sm:min-h-[92px] sm:px-4 sm:py-2 ${isGroupOpen ? groupTone.divider : "border-transparent"}`}>
                    <div>
                      <div className="inline-flex items-center gap-2 text-[15px] font-semibold text-slate-900 sm:text-base">
                        <span>{group.category}</span>
                        <DisclosureChevron expanded={isGroupOpen} className="h-6 w-6" />
                      </div>
                      <div className={`mt-0.5 text-[13px] ${groupTone.headerHint}`}>{group.items.length} lines in this classification</div>
                    </div>
                    <div className={`text-[12px] ${groupTone.headerHint}`}>{isGroupOpen ? "Hide" : "Show"}</div>
                  </div>
                  </button>
                  {isGroupOpen ? (
                    <>
                      <div className="space-y-3 px-3 py-3 sm:px-4">
                        {group.items.map((line) => renderFinancialLineCard(line, kind))}
                      </div>
                      <div className={`flex justify-center border-t ${groupTone.divider} px-3 py-3 sm:px-4 sm:py-4`}>
                        <div className={`rounded-full px-4 py-2 text-sm font-semibold sm:px-5 sm:text-base ${groupTone.footerPill}`}>
                          Total {money(group.subtotal)}
                        </div>
                      </div>
                    </>
                  ) : (
                    <div className={`flex justify-center border-t ${groupTone.divider} px-3 py-2 sm:px-4 sm:py-3`}>
                    <div className={`rounded-full px-3.5 py-1.5 text-sm font-semibold sm:px-4 sm:text-[15px] ${groupTone.footerPill}`}>
                      Total {money(group.subtotal)}
                    </div>
                  </div>
                  )}
                </div>
                  );
                })()
              ))}
            </div>
          )}
        </div>
      </div>
    );
  }

  function renderReviewCard(tx: TxRow) {
    const draft = txDraft(tx);
    const panelKey = reviewCardKey(tx);
    const isExpanded = !!openReviewPanels[panelKey];
    const actionState = reviewActionState[panelKey];
    const reviewBusy = !!actionState?.pendingAction;
    const actionSet = new Set(availableReviewActions(tx));
    const canSetCadence = Boolean(
      tx.group_key
      && (
        hasCadenceReviewReason(tx)
        || isMoneyMovementReviewItem(tx)
        || String(tx.category || "").toLowerCase().includes("transfer")
        || String(tx.cadence_reason || "").trim().length > 0
      ),
    );
    const mergeTargets = (tx.likely_merge_targets || []).map((candidate) =>
      typeof candidate === "string" ? candidate : candidate.group_label || candidate.group_key || "Possible merge",
    );
    const tone = directionToneClasses(reviewCardTone(tx));
    const typeLabel = reviewCardTypeLabel(tx);
    return (
      <div
        key={tx.id}
        ref={(node) => setReviewCardRef(panelKey, node)}
        data-cy="budget-review-card"
        data-review-key={panelKey}
        className={`rounded-2xl border border-slate-200 bg-white p-3 sm:p-4 ${tone.rail}`}
      >
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <div className="text-base font-semibold text-slate-900">{tx.merchant_candidate || compactDescription(tx.raw_description)}</div>
              <span className={`rounded-full border px-2 py-0.5 text-[11px] ${tone.badge}`}>{typeLabel}</span>
              <span className={`rounded-full border px-2 py-0.5 text-[11px] ${labelTone(tx.confidence_label)}`}>{tx.confidence_label}</span>
            </div>
            <div className="mt-2 text-sm text-slate-700">{compactDescription(tx.raw_description)}</div>
            <div className="mt-3 flex flex-wrap gap-2">
              {tx.review_reasons.map((reason) => (
                <span key={reason} className="rounded-full bg-amber-50 px-2 py-1 text-[11px] text-amber-700">
                  {reasonLabel(reason)}
                </span>
              ))}
            </div>
            <div className="mt-3 space-y-1 text-sm text-slate-600">
              <div>Current suggestion: {shortSuggestion(tx)}</div>
              <div className="text-xs text-slate-500">
                {tx.group_transaction_count} related transaction{tx.group_transaction_count === 1 ? "" : "s"} • {cadenceLabel(tx.inferred_cadence || "unknown", tx.observed_only)}
              </div>
            </div>
          </div>

          <div className={`flex w-full flex-col gap-3 rounded-2xl border px-4 py-3 lg:min-w-[220px] lg:w-auto ${tone.amountChip}`}>
            <div>
              <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Amount</div>
              <div className={`mt-1 text-xl font-semibold ${tone.amount}`}>{money(Number(tx.amount))}</div>
            </div>
            <div className="text-xs text-slate-600">Priority {Math.round(Number(tx.review_priority ?? 0))}</div>
          </div>
        </div>

        {actionState?.stillNeedsReview && actionState.remainingReasons?.length ? (
          <div data-cy="budget-review-remaining-reasons" className="mt-3 rounded-xl border border-amber-200 bg-amber-50/60 px-3 py-3">
            <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-700">
              Remaining Review Reasons
            </div>
            <div className="mt-2 flex flex-wrap gap-2">
              {actionState.remainingReasons.map((reason) => (
                <span key={reason} className="rounded-full border border-amber-200 bg-white px-2 py-1 text-[11px] text-amber-800">
                  {reason}
                </span>
              ))}
            </div>
          </div>
        ) : null}
        {actionState?.errorMessage ? (
          <div className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
            {actionState.errorMessage}
          </div>
        ) : null}

        <div data-cy="budget-review-actions" className="mt-4 flex flex-col gap-2 sm:flex-row sm:flex-wrap">
          {actionSet.has("mark_recurring") ? (
            <Button className="w-full sm:w-auto" type="button" size="sm" variant="secondary" disabled={reviewBusy} onClick={() => {
              const label = recurringActionLabel(tx);
              void applyReviewAction(tx, [{ target_type: tx.group_key ? "group" : "transaction", target_id: tx.group_key ?? tx.id, operation: "mark_recurring", payload: {} }], isIncomeReviewItem(tx) ? "Treated as recurring income." : "Treated as recurring baseline spending.", label);
            }}>
              {actionState?.pendingAction === recurringActionLabel(tx) ? "Saving..." : recurringActionLabel(tx)}
            </Button>
          ) : null}
          {actionSet.has("mark_one_off") ? (
            <Button className="w-full sm:w-auto" type="button" size="sm" variant="secondary" disabled={reviewBusy} onClick={() => void applyReviewAction(tx, [{ target_type: tx.group_key ? "group" : "transaction", target_id: tx.group_key ?? tx.id, operation: "mark_one_off", payload: {} }], "Moved to one-off / irregular spending. Excluded from net calculations.", oneOffActionLabel())}>
              {actionState?.pendingAction === oneOffActionLabel() ? "Saving..." : oneOffActionLabel()}
            </Button>
          ) : null}
          {actionSet.has("keep_discretionary") ? (
            <Button className="w-full sm:w-auto" type="button" size="sm" variant="secondary" disabled={reviewBusy} onClick={() => void applyReviewAction(tx, [{ target_type: tx.group_key ? "group" : "transaction", target_id: tx.group_key ?? tx.id, operation: "set_bucket_assignment", payload: { bucket_assignment: "variable_discretionary" } }], "Kept in variable observed spending.", "Keep in variable spending")}>
              {actionState?.pendingAction === "Keep in variable spending" ? "Saving..." : "Keep in variable spending"}
            </Button>
          ) : null}
          {tx.group_key ? (
            <Button className="w-full sm:w-auto" type="button" size="sm" variant="secondary" onClick={() => void toggleReviewPanel(tx)}>
              {isExpanded ? "Hide details" : "Review related transactions"}
            </Button>
          ) : (
            <Button className="w-full sm:w-auto" type="button" size="sm" variant="secondary" onClick={() => void toggleReviewPanel(tx)}>
              {isExpanded ? "Hide details" : "Edit details"}
            </Button>
          )}
        </div>

        <details data-cy="budget-review-diagnostics" className="mt-3 rounded-xl border border-slate-200 bg-slate-50/70">
          <summary className="cursor-pointer list-none px-3 py-3 text-sm font-medium text-slate-700">
            Why the model is unsure
          </summary>
          <div className="border-t border-slate-200 px-3 py-3">
            <div className="grid gap-2 text-xs leading-5 text-slate-500 sm:grid-cols-2">
              <div>Related transactions: {tx.group_transaction_count}</div>
              <div>Interpretation: {tx.interpretation_reason}</div>
              <div>Direction source: {tx.direction_source}</div>
              <div>Cadence: {cadenceLabel(tx.inferred_cadence || "unknown", tx.observed_only)}</div>
              <div className="sm:col-span-2">Cadence reason: {tx.cadence_reason || "No cadence override applied"}</div>
            </div>

            {canSetCadence ? (
              <div data-cy="budget-cadence-actions" className="mt-4">
                <div className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                  Cadence only
                </div>
                <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap">
                  {!actionSet.has("mark_one_off") ? (
                    <Button
                      className="w-full sm:w-auto"
                      type="button"
                      size="sm"
                      variant="secondary"
                      disabled={reviewBusy}
                      onClick={() =>
                        void applyReviewAction(
                          tx,
                          [{ target_type: tx.group_key ? "group" : "transaction", target_id: tx.group_key ?? tx.id, operation: "mark_one_off", payload: {} }],
                          "Moved to one-off / irregular spending. Excluded from net calculations.",
                          oneOffActionLabel(),
                        )
                      }
                    >
                      {actionState?.pendingAction === oneOffActionLabel() ? "Saving..." : oneOffActionLabel()}
                    </Button>
                  ) : null}
                  {["weekly", "fortnightly", "monthly", "quarterly", "irregular"].map((cadence) => {
                    const actionLabel = cadenceActionLabel(cadence);
                    return (
                      <Button
                        className="w-full sm:w-auto"
                        key={cadence}
                        type="button"
                        size="sm"
                        variant="secondary"
                        disabled={reviewBusy}
                        onClick={() =>
                          void applyReviewAction(
                            tx,
                            [{ target_type: "group", target_id: tx.group_key!, operation: "set_cadence", payload: { cadence } }],
                            cadence === "irregular" ? "Marked as irregular cadence." : `Marked as ${cadence} cadence.`,
                            actionLabel,
                          )
                        }
                      >
                        {actionState?.pendingAction === actionLabel ? "Saving..." : actionLabel}
                      </Button>
                    );
                  })}
                </div>
              </div>
            ) : null}
          </div>
        </details>

        {isExpanded ? (
          <div data-cy="budget-review-editor" className="mt-4 rounded-xl border border-slate-200 p-3 sm:p-4">
            <div className="mb-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
              Review decision
            </div>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              <select
                className="rounded-xl border border-slate-300 px-3 py-2 text-sm"
                value={draft.category}
                onChange={(e) => {
                  const nextCategory = e.target.value;
                  setTxEdits((prev) => ({
                    ...prev,
                    [tx.id]: {
                      ...draft,
                      category: nextCategory,
                      subcategory: nextSubcategoryForCategory(nextCategory, draft.subcategory),
                    },
                  }));
                }}
              >
                {categoryOptions(draft.category).map((option) => <option key={option} value={option}>{option}</option>)}
              </select>
              <select
                className="rounded-xl border border-slate-300 px-3 py-2 text-sm"
                value={draft.subcategory}
                onChange={(e) => setTxEdits((prev) => ({ ...prev, [tx.id]: { ...draft, subcategory: e.target.value } }))}
              >
                {subcategoryOptions(draft.category, draft.subcategory).map((option) => <option key={option} value={option}>{option}</option>)}
              </select>
              <select className="rounded-xl border border-slate-300 px-3 py-2 text-sm" value={draft.bucketAssignment} onChange={(e) => setTxEdits((prev) => ({ ...prev, [tx.id]: { ...draft, bucketAssignment: e.target.value } }))}>
                {BUCKET_OPTIONS.map((option) => <option key={option} value={option}>{bucketLabel(option)}</option>)}
              </select>
            </div>
            <div className="mt-3 flex flex-col gap-2 text-sm text-slate-600 sm:flex-row sm:flex-wrap sm:gap-3">
              <label className="flex items-center gap-2"><input type="checkbox" checked={draft.included} onChange={(e) => setTxEdits((prev) => ({ ...prev, [tx.id]: { ...draft, included: e.target.checked } }))} />Include in budget</label>
              <label className="flex items-center gap-2"><input type="checkbox" checked={draft.rememberMapping} onChange={(e) => setTxEdits((prev) => ({ ...prev, [tx.id]: { ...draft, rememberMapping: e.target.checked } }))} />Apply to future matches</label>
            </div>
            <div className="mt-3 flex flex-col gap-2 sm:flex-row sm:flex-wrap">
              <Button
                className="w-full sm:w-auto"
                type="button"
                size="sm"
                variant="secondary"
                disabled={busy}
                onClick={() => {
                  const bucketTargetType = tx.group_key ? "group" : "transaction";
                  const bucketTargetId = tx.group_key ?? tx.id;
                  const ops: Array<{ target_type: "transaction" | "group"; target_id: string; operation: string; payload: Record<string, unknown> }> = [
                    { target_type: "transaction", target_id: tx.id, operation: "set_category", payload: { category: draft.category } },
                    { target_type: "transaction", target_id: tx.id, operation: "set_subcategory", payload: { subcategory: draft.subcategory } },
                    { target_type: bucketTargetType, target_id: bucketTargetId, operation: "set_bucket_assignment", payload: { bucket_assignment: draft.bucketAssignment } },
                    { target_type: "transaction", target_id: tx.id, operation: "set_include", payload: { included: draft.included } },
                  ];
                  void applyOverrides(ops, "Saved review decision and recalculated budget.");
                }}
              >
                Save
              </Button>
              {draft.rememberMapping ? (
                <Button
                  className="w-full sm:w-auto"
                  type="button"
                  size="sm"
                  variant="secondary"
                  disabled={busy}
                  onClick={() =>
                    void applyOverrides(
                      [{ target_type: "transaction", target_id: tx.id, operation: "remember_mapping", payload: { category: draft.category, subcategory: draft.subcategory } }],
                      "Saved mapping for future matches.",
                    )
                  }
                >
                  Save mapping
                </Button>
              ) : null}
            </div>
            {mergeTargets.length ? <div className="mt-3 text-xs text-amber-700">Merge candidates: {mergeTargets.join(", ")}</div> : null}
            {tx.group_key && loadingExpandedLine[tx.group_key] ? (
              <div className="mt-4 rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-4 text-sm text-slate-500">
                Loading related transactions...
              </div>
            ) : tx.group_key && expandedLine[tx.group_key] ? (
              <div data-cy="budget-review-underlying-panel" className="mt-4 max-h-64 space-y-2 overflow-auto">
                {expandedLine[tx.group_key].map((groupedTx) => (
                  <div key={groupedTx.id} className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-700">
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-medium text-slate-900">{groupedTx.transaction_date ?? "-"}</div>
                      <div className={Number(groupedTx.amount) >= 0 ? "font-medium text-emerald-900" : "font-medium text-orange-900"}>{money(Number(groupedTx.amount))}</div>
                    </div>
                    <div className="mt-2">{groupedTx.raw_description}</div>
                  </div>
                ))}
              </div>
            ) : tx.group_key ? (
              <div className="mt-4 rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-4 text-sm text-slate-500">
                <div>No related transactions were returned for this group.</div>
                {tx.group_transaction_count > 0 ? (
                  <div className="mt-2 text-amber-700">
                    Group membership appears out of sync. Recompute required.
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    );
  }

  return (
    <div data-cy="budget-workspace-root" className="space-y-6">
      <section className="rounded-[28px] border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Import</div>
            <h3 className="mt-2 text-2xl font-semibold text-slate-950">Budget Engine V2</h3>
            <p className="mt-2 max-w-2xl text-sm text-slate-600">
              Import a statement, review only the items that need a decision, and keep the main budget focused on what is recurring and material.
            </p>
          </div>
          {summary ? (
            <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
              <div><span className="font-medium text-slate-900">{summary.source_bank ?? "Unknown bank"}</span> via {summary.parser_name ?? "unknown parser"}</div>
              <div className="mt-1">{summary.statement_start_date ?? "-"} to {summary.statement_end_date ?? "-"}</div>
              <div className="mt-1">{summary.transaction_count} transactions parsed</div>
            </div>
          ) : null}
        </div>

        <div className="mt-5 flex flex-col gap-3 md:flex-row md:flex-wrap md:items-center">
          <input className="w-full text-sm md:w-auto" type="file" accept=".pdf,application/pdf" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
          <Button className="w-full md:w-auto" type="button" onClick={() => void onImport()} disabled={!file || busy}>
            {busy ? "Importing..." : "Import statement"}
          </Button>
          <Button className="w-full md:w-auto" type="button" variant="secondary" onClick={() => void loadParsers()} disabled={busy}>
            Refresh parsers
          </Button>
          <Button className="w-full md:w-auto" type="button" variant="secondary" onClick={() => void onResetBudget()} disabled={busy}>
            Reset budget
          </Button>
        </div>

        {message ? <div className="mt-4 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">{message}</div> : null}
        {error ? <div className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div> : null}

        {summary ? (
          <div className="mt-4 space-y-3">
            {renderImportBanner("Parser and reconciliation warnings", summary.parser_coverage_warnings || [], "warn")}
            {renderImportBanner("Import scope warnings", summary.scope_warnings || [], "warn")}
            {!!summary.parser_warnings?.length && !summary.parser_coverage_warnings?.length ? renderImportBanner("Parser notices", summary.parser_warnings || [], "muted") : null}

            <div ref={(node) => setSectionRef("import-details", node)} className="rounded-xl border border-slate-200 p-3 text-sm text-slate-600">
              <button
                type="button"
                className="inline-flex w-full items-center gap-2 text-left font-medium text-slate-800"
                aria-expanded={importDetailsOpen}
                aria-controls="budget-import-details"
                onClick={() => toggleAnchoredSection("import-details", setImportDetailsOpen)}
              >
                <span>Import details</span>
                <DisclosureChevron expanded={importDetailsOpen} className="h-6 w-6" />
              </button>
              <div id="budget-import-details" hidden={!importDetailsOpen} className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                <div>Status: {summary.status}</div>
                <div>Parser confidence: {percent(summary.parser_confidence)}</div>
                <div>Coverage estimate: {summary.coverage_estimate != null ? percent(summary.coverage_estimate) : "-"}</div>
                <div>Overlap status: {summary.overlap_status ?? "clear"}</div>
                <div>Parsed debits: {summary.parsed_debit_count ?? 0} ({money(Number(summary.parsed_debit_total ?? 0))})</div>
                <div>Parsed credits: {summary.parsed_credit_count ?? 0} ({money(Number(summary.parsed_credit_total ?? 0))})</div>
                <div>Opening balance: {money(Number(summary.opening_balance ?? 0))}</div>
                <div>Closing balance: {money(Number(summary.closing_balance ?? 0))}</div>
                <div>Expected closing: {money(Number(summary.expected_closing_balance ?? 0))}</div>
                <div>Difference: {money(Number(summary.reconciliation_difference ?? 0))}</div>
                <div>Missing pages suspected: {summary.suspected_missing_pages ? "Yes" : "No"}</div>
                <div>Duplicate rows detected: {summary.duplicate_rows_detected ?? 0}</div>
              </div>
            </div>
          </div>
        ) : null}

        {parsers.length > 0 ? (
          <div className="mt-4 text-xs text-slate-500">Available parsers: {parsers.map((parser) => parser.name).join(", ")}</div>
        ) : null}
      </section>

      <section className="rounded-[28px] border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
        <div className="flex flex-col gap-2 md:flex-row md:items-end md:justify-between">
          <div>
            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Budget Overview</div>
            <h4 className="mt-2 text-lg font-semibold text-slate-950 sm:text-xl">Monthly budget view</h4>
            <p className="mt-1 text-sm text-slate-600">This top row stays on a monthly basis so income, core commitments, living costs, and net can be compared on the same footing.</p>
          </div>
        </div>

        <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
          {renderOverviewCard("monthly-income", "Monthly income", guardedTotals.recurringIncome, `${money(guardedTotals.irregularIncome)} irregular income stays in the observed row below.`, "income")}
          {renderOverviewCard("recurring-baseline", "Monthly recurring baseline", guardedTotals.recurringBaseline, "Core recurring commitments only", "expense")}
          {renderOverviewCard(
            "monthly-variable-spending",
            "Monthly variable spending",
            guardedTotals.variableMonthly,
            guardedTotals.modelingAllowed
              ? "Monthly equivalent of day-to-day living costs."
              : guardedTotals.modelingRestrictions[0] || "Monthly equivalent shown provisionally while trust blocks full modeling.",
            "warn",
          )}
          {renderOverviewCard("core-net", "Core net", guardedTotals.coreNet, "Recurring income minus recurring baseline", "muted")}
          {renderOverviewCard(
            "observed-net",
            "Observed net",
            guardedTotals.observedNet,
            guardedTotals.modelingAllowed
              ? "Recurring income minus baseline and variable spending"
              : "Provisional while modeling is blocked",
            "muted",
          )}
          {renderOverviewCard(
            "one-off-observed-summary",
            "One-off observed",
            guardedTotals.oneOffObserved,
            "Statement-period one-offs kept visible, excluded from monthly net",
            "warn",
          )}
        </div>
        <div className="mt-4 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
          These headline cards now stay on one monthly basis. They use authoritative backend budget-model totals, while irregular income, one-offs, transfers, fees, and review items remain visible in the observed statement summary below.
        </div>

        <div className="mt-6 rounded-2xl border border-slate-200 bg-slate-50/60">
          <button
            type="button"
            data-cy="budget-observed-evidence-toggle"
            className="w-full px-3 py-3 text-left sm:px-4 sm:py-4"
            aria-expanded={observedEvidenceOpen}
            aria-controls="budget-observed-evidence"
            onClick={() => setObservedEvidenceOpen((previous) => !previous)}
          >
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Statement Evidence</div>
                <h4 className="mt-2 inline-flex items-center gap-2 text-lg font-semibold text-slate-950 sm:text-xl">
                  <span>Observed over this uploaded statement</span>
                  <DisclosureChevron expanded={observedEvidenceOpen} />
                </h4>
                <p className="mt-1 text-sm text-slate-600">Audit what actually happened across the uploaded window before deciding what belongs in the monthly budget.</p>
              </div>
              <div className="inline-flex items-center gap-2 self-start rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-600">
                <span>{observedEvidenceOpen ? "Hide details" : "Show details"}</span>
                <DisclosureChevron expanded={observedEvidenceOpen} className="h-6 w-6" />
              </div>
            </div>
          </button>

          <div id="budget-observed-evidence" hidden={!observedEvidenceOpen} className="border-t border-slate-200 px-3 py-3 sm:px-4 sm:py-4">
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
              {renderOverviewCard("income-observed", "Income observed", statementPeriodTotals.income, "Visible income across the uploaded statement period", "income")}
              {renderOverviewCard("variable-observed", "Variable observed", statementPeriodTotals.variable, "Statement-period day-to-day spend before monthly normalization", "warn")}
              {renderOverviewCard("one-off-observed", "One-off observed", statementPeriodTotals.oneoff, "Visible, but excluded from the monthly net figures above", "warn")}
              {renderOverviewCard("transfers-observed", "Transfers observed", statementPeriodTotals.transfer, "Money movement observed in the uploaded period", "muted")}
              {renderOverviewCard("fees-observed", "Fees observed", statementPeriodTotals.fees, "Observed charges kept separate from living costs", "muted")}
            </div>
            <div className="mt-4 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-600">
              The observed row stays on the statement-period basis, whether the statement covers 30, 60, or 90 days. This is the audit trail; the monthly budget row above is the planning view.
            </div>
          </div>
        </div>
      </section>

      <div
        ref={(node) => setSectionRef("needs-attention", node)}
        data-cy="budget-needs-attention"
        className="rounded-[28px] border border-slate-200 bg-white p-4 shadow-sm sm:p-5"
      >
        <button
          type="button"
          data-cy="budget-needs-attention-toggle"
          className="w-full text-left"
          aria-expanded={needsAttentionOpen}
          aria-controls="budget-needs-attention"
          onClick={() => toggleAnchoredSection("needs-attention", setNeedsAttentionOpen)}
        >
          <div className="flex flex-col gap-2 md:flex-row md:items-end md:justify-between">
            <div>
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Needs Attention</div>
              <h4 className="mt-2 inline-flex items-center gap-2 text-lg font-semibold text-slate-950 sm:text-xl">
                <span>Items needing confirmation</span>
                <DisclosureChevron expanded={needsAttentionOpen} />
              </h4>
              <p className="mt-1 text-sm text-slate-600">Review stays separate from the main financial picture. Open this only when you need to make a decision.</p>
            </div>
            <div className="rounded-full bg-slate-100 px-3 py-1 text-sm text-slate-600">{needsAttentionItemCount(reviewCards, uncategorizedLines)} items</div>
          </div>
        </button>
        <div id="budget-needs-attention" hidden={!needsAttentionOpen} className="mt-4 space-y-3 border-t border-slate-200 pt-4 sm:mt-5 sm:pt-5">
          {reviewCards.length ? reviewCards.map((tx) => renderReviewCard(tx)) : null}
          {uncategorizedLines.length ? renderFinancialSection(
            "uncategorized",
            "Uncategorized lines",
            "These line groups still need a decision before they belong in a stronger budget bucket.",
            uncategorizedLines,
            "uncategorized",
          ) : null}
          {!reviewCards.length && !uncategorizedLines.length ? (
            <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-5 text-sm text-slate-500">
              No items currently need review.
            </div>
          ) : null}
        </div>
      </div>

      <div className="space-y-4">
        {renderFinancialSection("income", "Income", "Recurring income supports the net figures. Irregular income stays visible here, clearly flagged, but is excluded from both net calculations.", [...recurringIncomeLines, ...irregularIncomeLines], "income")}
        {renderFinancialSection("baseline", "Recurring Baseline Expenses", "Only stable baseline obligations belong here. This is the expense layer used for the core budget.", recurringBaselineLines, "baseline")}
        {renderFinancialSection("variable", "Variable / Observed Spending", "These lines use observational monthly estimates derived from statement-period activity and stay out of baseline totals.", discretionaryLines, "variable")}
        {renderFinancialSection("oneoff", "One-off / Irregular Expenses", "Exceptional spend stays separate and never rolls into recurring budget totals.", oneOffLines, "oneoff")}
        {renderFinancialSection("transfer", "Transfers / Money Movement", "Transfers and reimbursements are isolated from lifestyle budgeting entirely.", transferLines, "transfer")}
        {renderFinancialSection("fees", "Fees", "Fees stay visible separately so they do not silently inflate lifestyle spend.", feeLines, "fees")}
      </div>

      {(hasAdvancedTools || showExpertTools) ? (
        <section className="rounded-[28px] border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div>
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Advanced Tools</div>
              <h4 className="mt-2 text-lg font-semibold text-slate-950 sm:text-xl">Expert cleanup and diagnostics</h4>
              <p className="mt-1 text-sm text-slate-600">Use these only when you need to repair groups, inspect parser output, or run manual transaction reassignment.</p>
            </div>
            <Button type="button" size="sm" variant="secondary" onClick={() => setShowExpertTools((prev) => !prev)}>
              {showExpertTools ? "Hide expert tools" : "Show expert tools"}
            </Button>
          </div>

          {showExpertTools ? (
            <div className="mt-5 space-y-4">
              <div className="rounded-2xl border border-slate-200 p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Merchant memory</div>
                <div className="mt-2 text-sm text-slate-600">Review remembered mappings, estimate their blast radius, and retire bad mappings without touching the current import manually.</div>
                <div className="mt-4 space-y-2">
                  {merchantMemory.length ? merchantMemory.map((item) => (
                    <div key={item.id} className="flex flex-col gap-2 rounded-xl border border-slate-200 bg-slate-50 px-3 py-3 md:flex-row md:items-center md:justify-between">
                      <div className="min-w-0">
                        <div className="truncate text-sm font-semibold text-slate-900">{item.merchant_key}</div>
                        <div className="mt-1 text-xs text-slate-500">{item.category} • {item.subcategory}</div>
                        <div className="mt-1 text-xs text-slate-500">
                          {item.mapping_source || item.source} • {item.scope || "organization"} • used by {item.usage_count ?? 0} transaction{(item.usage_count ?? 0) === 1 ? "" : "s"}
                        </div>
                      </div>
                      <Button type="button" size="sm" variant="secondary" disabled={busy} onClick={() => void retireMerchantMemory(item.id)}>
                        Retire mapping
                      </Button>
                    </div>
                  )) : (
                    <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-4 text-sm text-slate-500">
                      No merchant memory mappings are currently stored.
                    </div>
                  )}
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Split / merge / reassign</div>
                <div className="mt-3 grid gap-3 lg:grid-cols-2">
                  <div className="space-y-3 rounded-xl border border-slate-200 bg-slate-50 p-4">
                    <input className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" placeholder="Split group key" value={splitGroup} onChange={(e) => setSplitGroup(e.target.value)} />
                    <Button type="button" size="sm" variant="secondary" disabled={!splitGroup || busy} onClick={() => void applyOverrides([{ target_type: "group", target_id: splitGroup, operation: "split_group", payload: {} }], "Split group request submitted.")}>Split group</Button>
                  </div>
                  <div className="space-y-3 rounded-xl border border-slate-200 bg-slate-50 p-4">
                    <input className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" placeholder="Merge source group" value={mergeSource} onChange={(e) => setMergeSource(e.target.value)} />
                    <input className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" placeholder="Merge target group" value={mergeTarget} onChange={(e) => setMergeTarget(e.target.value)} />
                    <Button type="button" size="sm" variant="secondary" disabled={!mergeSource || !mergeTarget || busy} onClick={() => void applyOverrides([{ target_type: "group", target_id: mergeSource, operation: "merge_group", payload: { target_group: mergeTarget } }], "Merge group request submitted.")}>Merge group</Button>
                  </div>
                  <div className="space-y-3 rounded-xl border border-slate-200 bg-slate-50 p-4 lg:col-span-2">
                    <input className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" placeholder="Reassign source group" value={reassignSource} onChange={(e) => setReassignSource(e.target.value)} />
                    <input className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" placeholder="Reassign target group" value={reassignTarget} onChange={(e) => setReassignTarget(e.target.value)} />
                    <input className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" placeholder="Transaction IDs CSV" value={reassignTxIds} onChange={(e) => setReassignTxIds(e.target.value)} />
                    <Button
                      type="button"
                      size="sm"
                      variant="secondary"
                      disabled={!reassignSource || !reassignTarget || !reassignTxIds || busy}
                      onClick={() =>
                        void applyOverrides(
                          [{
                            target_type: "group",
                            target_id: reassignSource,
                            operation: "reassign_transactions",
                            payload: {
                              target_group: reassignTarget,
                              transaction_ids: reassignTxIds.split(",").map((item) => item.trim()).filter(Boolean),
                            },
                          }],
                          "Reassign request submitted.",
                        )
                      }
                    >
                      Reassign transactions
                    </Button>
                  </div>
                </div>
              </div>

              <div ref={(node) => setSectionRef("full-audit", node)} className="rounded-2xl border border-slate-200 p-4">
                <button
                  type="button"
                  className="inline-flex w-full items-center gap-2 text-left text-sm font-medium text-slate-800"
                  aria-expanded={fullAuditOpen}
                  aria-controls="budget-full-audit"
                  onClick={() => toggleAnchoredSection("full-audit", setFullAuditOpen)}
                >
                  <span>Full transaction audit</span>
                  <DisclosureChevron expanded={fullAuditOpen} className="h-6 w-6" />
                </button>
                <div id="budget-full-audit" hidden={!fullAuditOpen} className="mt-4 max-h-72 space-y-2 overflow-auto">
                  {transactions.map((tx) => (
                    <div key={tx.id} className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-700">
                      <div className="flex items-center justify-between gap-3">
                        <div className="font-medium text-slate-900">{tx.transaction_date ?? "-"}</div>
                        <div className={Number(tx.amount) >= 0 ? "font-medium text-emerald-900" : "font-medium text-orange-900"}>{money(Number(tx.amount))}</div>
                      </div>
                      <div className="mt-2">{tx.raw_description}</div>
                      <div className="mt-2 text-xs text-slate-500">{tx.interpretation_type} / {tx.category} / {tx.subcategory} / {tx.direction_source}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : null}
        </section>
      ) : null}

      {resolvedReviewNotices.length ? (
        <div className="pointer-events-none fixed bottom-6 right-6 z-50 flex w-full max-w-sm flex-col gap-3">
          {resolvedReviewNotices.map((notice) => (
            <div key={notice.key} className="pointer-events-auto rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-800 shadow-lg">
              <div className="font-medium text-emerald-900">{notice.label}</div>
              <div className="mt-1">{notice.message}</div>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
