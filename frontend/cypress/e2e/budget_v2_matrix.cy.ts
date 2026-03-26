/// <reference types="cypress" />

Cypress.on("uncaught:exception", (err) => {
  if (err.message?.includes("Hydration failed") || err.message?.includes("Minified React error #418")) {
    return false;
  }
  return true;
});

type BudgetModel = {
  recurring_income_monthly: number;
  recurring_baseline_monthly: number;
  variable_discretionary_monthly: number;
  observed_one_off_total: number;
  observed_transfer_total: number;
  irregular_income_total: number;
  core_net: number;
  observed_net: number;
  modeling_allowed: boolean;
  modeling_restrictions: string[];
};

type Summary = {
  import_id: string;
  status: string;
  source_bank: string;
  parser_name: string;
  parser_confidence: number;
  parser_warnings: string[];
  statement_start_date: string;
  statement_end_date: string;
  transaction_count: number;
  scope_warnings: string[];
  parser_coverage_warnings: string[];
  parsed_debit_count: number;
  parsed_credit_count: number;
  parsed_debit_total: number;
  parsed_credit_total: number;
  opening_balance: number;
  closing_balance: number;
  statement_total_debits: number;
  statement_total_credits: number;
  expected_closing_balance: number;
  reconciliation_status: string;
  reconciliation_reason: string | null;
  reconciliation_difference: number;
  warning_reasons: string[];
  document_type: string;
  document_reconcilable: boolean;
  document_warnings: string[];
  budget_model: BudgetModel;
  trust: {
    reconciliation_status: string;
    totals_trust_level: string;
    truth_trust_level: string;
    modeling_allowed: boolean;
    modeling_restrictions: string[];
    trust_reasons: string[];
  };
};

type ReviewRow = Record<string, unknown>;
type LineRow = Record<string, unknown>;
type TxRow = Record<string, unknown>;

type PhaseState = {
  summary: Summary;
  reviewItems: ReviewRow[];
  lines: LineRow[];
  lineTransactions?: Record<string, TxRow[]>;
};

const apiBase = "https://api.echoheelixmissioncontrol.com";
const localToken = "x".repeat(64);
const importId = "99999999-1111-2222-3333-444444444444";
const budgetModule = {
  id: "budget",
  slug: "budget",
  title: "Budget",
  description: "Statement-backed budgeting",
  category: "finance",
  enabled: true,
  order: 1,
};

function money(value: number): string {
  return value.toLocaleString("en-AU", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function makeBudgetModel(overrides: Partial<BudgetModel> = {}): BudgetModel {
  const recurringIncome = overrides.recurring_income_monthly ?? 4000;
  const recurringBaseline = overrides.recurring_baseline_monthly ?? 1500;
  const variableMonthly = overrides.variable_discretionary_monthly ?? 0;
  const modelingAllowed = overrides.modeling_allowed ?? true;
  return {
    recurring_income_monthly: recurringIncome,
    recurring_baseline_monthly: recurringBaseline,
    variable_discretionary_monthly: variableMonthly,
    observed_one_off_total: overrides.observed_one_off_total ?? 0,
    observed_transfer_total: overrides.observed_transfer_total ?? 0,
    irregular_income_total: overrides.irregular_income_total ?? 0,
    core_net: overrides.core_net ?? (modelingAllowed ? recurringIncome - recurringBaseline : 0),
    observed_net:
      overrides.observed_net ?? (modelingAllowed ? recurringIncome - recurringBaseline - variableMonthly : 0),
    modeling_allowed: modelingAllowed,
    modeling_restrictions: overrides.modeling_restrictions ?? [],
  };
}

function makeSummary(model: Partial<BudgetModel> = {}, trust: Partial<Summary["trust"]> = {}): Summary {
  const budgetModel = makeBudgetModel(model);
  return {
    import_id: importId,
    status: "completed",
    source_bank: "NAB",
    parser_name: "nab",
    parser_confidence: 0.94,
    parser_warnings: [],
    statement_start_date: "2026-01-01",
    statement_end_date: "2026-01-31",
    transaction_count: 8,
    scope_warnings: [],
    parser_coverage_warnings: [],
    parsed_debit_count: 6,
    parsed_credit_count: 2,
    parsed_debit_total: 2631.98,
    parsed_credit_total: 4850,
    opening_balance: 1000,
    closing_balance: 3218.02,
    statement_total_debits: 2631.98,
    statement_total_credits: 4850,
    expected_closing_balance: 3218.02,
    reconciliation_status: trust.reconciliation_status ?? "reconciled",
    reconciliation_reason: null,
    reconciliation_difference: 0,
    warning_reasons: [],
    document_type: "statement",
    document_reconcilable: true,
    document_warnings: [],
    budget_model: budgetModel,
    trust: {
      reconciliation_status: trust.reconciliation_status ?? "reconciled",
      totals_trust_level: trust.totals_trust_level ?? "verified",
      truth_trust_level: trust.truth_trust_level ?? "verified",
      modeling_allowed: trust.modeling_allowed ?? budgetModel.modeling_allowed,
      modeling_restrictions: trust.modeling_restrictions ?? budgetModel.modeling_restrictions,
      trust_reasons: trust.trust_reasons ?? [],
    },
  };
}

function recurringIncomeLine(overrides: Partial<LineRow> = {}): LineRow {
  return {
    id: "line-income",
    group_key: "Income|Payroll|ACCENTURE",
    group_label: "ACCENTURE",
    line_type: "income",
    category: "Income",
    subcategory: "Payroll",
    inferred_cadence: "monthly",
    cadence_confidence: 0.95,
    cadence_reason: "intervals_match_monthly",
    observed_only: false,
    bucket_assignment: "income_recurring",
    bucket_suggestion: "income_recurring",
    base_amount: 4000,
    base_period: "monthly",
    authoritative_field: "base_amount",
    source_amount: 4000,
    source_period: "monthly",
    observed_window_total: 4000,
    normalized_weekly: 923.08,
    normalized_fortnightly: 1846.15,
    normalized_monthly: 4000,
    normalized_yearly: 48000,
    impact_on_baseline: "included",
    included: true,
    transaction_count: 1,
    confidence_label: "High confidence",
    explanation: "Recurring payroll-like credits contribute to recurring income totals.",
    review_reasons: [],
    modeling_status: "modeled_recurring",
    recurrence_state: "recurring_candidate",
    is_modeled: true,
    modeled_by_default: true,
    merchant_confidence: 0.95,
    bucket_confidence: 0.95,
    observed_amount: 4000,
    observational_monthly_estimate: null,
    observed_frequency_label: "1 occurrence",
    duplicate_group_candidates: [],
    merge_candidate_confidence: 0,
    ...overrides,
  };
}

function baselineLine(overrides: Partial<LineRow> = {}): LineRow {
  return {
    id: "line-baseline",
    group_key: "General / Home|Rent|LANDLORD",
    group_label: "LANDLORD",
    line_type: "expense",
    category: "General / Home",
    subcategory: "Rent",
    inferred_cadence: "monthly",
    cadence_confidence: 0.95,
    cadence_reason: "intervals_match_monthly",
    observed_only: false,
    bucket_assignment: "recurring_baseline",
    bucket_suggestion: "recurring_baseline",
    base_amount: 1500,
    base_period: "monthly",
    authoritative_field: "base_amount",
    source_amount: 1500,
    source_period: "monthly",
    observed_window_total: 1500,
    normalized_weekly: 346.15,
    normalized_fortnightly: 692.31,
    normalized_monthly: 1500,
    normalized_yearly: 18000,
    impact_on_baseline: "included",
    included: true,
    transaction_count: 1,
    confidence_label: "High confidence",
    explanation: "Stable baseline obligation.",
    review_reasons: [],
    modeling_status: "modeled_recurring",
    recurrence_state: "recurring_candidate",
    is_modeled: true,
    modeled_by_default: true,
    merchant_confidence: 0.95,
    bucket_confidence: 0.95,
    observed_amount: 1500,
    observational_monthly_estimate: null,
    observed_frequency_label: "1 occurrence",
    duplicate_group_candidates: [],
    merge_candidate_confidence: 0,
    ...overrides,
  };
}

function variableLine(overrides: Partial<LineRow> = {}): LineRow {
  return {
    id: "line-variable",
    group_key: "Entertainment|Movies / Activities|STREAMING SERVICE",
    group_label: "STREAMING SERVICE",
    line_type: "expense",
    category: "Entertainment",
    subcategory: "Movies / Activities",
    inferred_cadence: "monthly",
    cadence_confidence: 0.52,
    cadence_reason: "intervals_match_monthly",
    observed_only: true,
    bucket_assignment: "variable_discretionary",
    bucket_suggestion: "variable_discretionary",
    base_amount: 25.99,
    base_period: "monthly",
    authoritative_field: "base_amount",
    source_amount: 25.99,
    source_period: "monthly",
    observed_window_total: 51.98,
    normalized_weekly: 6,
    normalized_fortnightly: 12,
    normalized_monthly: 0,
    normalized_yearly: 0,
    impact_on_baseline: "included",
    included: true,
    transaction_count: 2,
    confidence_label: "Needs review",
    explanation: "Repeated discretionary spend remains observational until promoted.",
    review_reasons: ["weak_cadence_evidence"],
    modeling_status: "observational_only",
    recurrence_state: "discretionary_candidate",
    is_modeled: false,
    modeled_by_default: false,
    merchant_confidence: 0.81,
    bucket_confidence: 0.64,
    observed_amount: 25.99,
    observational_monthly_estimate: 25.94,
    observed_frequency_label: "2 observations, monthly candidate",
    duplicate_group_candidates: [],
    merge_candidate_confidence: 0,
    ...overrides,
  };
}

function oneOffLine(overrides: Partial<LineRow> = {}): LineRow {
  return variableLine({
    id: "line-oneoff",
    bucket_assignment: "one_off_exceptional",
    inferred_cadence: "irregular",
    recurrence_state: "one_off_candidate",
    confidence_label: "Medium confidence",
    review_reasons: [],
    observational_monthly_estimate: 0,
    normalized_monthly: 0,
    ...overrides,
  });
}

function transferLine(overrides: Partial<LineRow> = {}): LineRow {
  return {
    id: "line-transfer",
    group_key: "Transfer / Money Movement|Internal|WISE TRANSFER",
    group_label: "WISE TRANSFER",
    line_type: "expense",
    category: "Transfer / Money Movement",
    subcategory: "Internal",
    inferred_cadence: "unknown",
    cadence_confidence: 0.25,
    cadence_reason: "insufficient_observations",
    observed_only: true,
    bucket_assignment: "transfer_money_movement",
    bucket_suggestion: "transfer_money_movement",
    base_amount: 500,
    base_period: "monthly",
    authoritative_field: "base_amount",
    source_amount: 500,
    source_period: "monthly",
    observed_window_total: 500,
    normalized_weekly: 0,
    normalized_fortnightly: 0,
    normalized_monthly: 0,
    normalized_yearly: 0,
    impact_on_baseline: "excluded",
    included: true,
    transaction_count: 1,
    confidence_label: "Needs review",
    explanation: "Transfer-like money movement remains isolated from spending totals.",
    review_reasons: ["cadence_ambiguous_material"],
    modeling_status: "observational_only",
    recurrence_state: "transfer_candidate",
    is_modeled: false,
    modeled_by_default: false,
    merchant_confidence: 0.72,
    bucket_confidence: 0.89,
    observed_amount: 500,
    observational_monthly_estimate: 0,
    observed_frequency_label: "Observed only",
    duplicate_group_candidates: [],
    merge_candidate_confidence: 0,
    ...overrides,
  };
}

function incomeIrregularLine(overrides: Partial<LineRow> = {}): LineRow {
  return recurringIncomeLine({
    id: "line-income-irregular",
    group_key: "Income|Payroll|CONSULTING BONUS",
    group_label: "CONSULTING BONUS",
    bucket_assignment: "income_irregular",
    observed_only: true,
    is_modeled: false,
    modeled_by_default: false,
    inferred_cadence: "irregular",
    cadence_confidence: 0.4,
    normalized_monthly: 0,
    observed_amount: 850,
    observed_window_total: 850,
    observed_frequency_label: "Irregular credit",
    confidence_label: "Needs review",
    ...overrides,
  });
}

function expenseReviewRow(overrides: Partial<ReviewRow> = {}): ReviewRow {
  return {
    id: "review-expense",
    transaction_date: "2026-02-01",
    amount: 25.99,
    signed_amount: -25.99,
    direction: "debit",
    movement_type: "expense",
    raw_description: "STREAMING SERVICE MELBOURNE",
    normalized_description: "STREAMING SERVICE",
    direction_source: "balance_delta",
    interpretation_type: "consumer_spend",
    interpretation_confidence: 0.72,
    interpretation_reason: "merchant spend",
    category: "Entertainment",
    subcategory: "Movies / Activities",
    confidence: 0.66,
    explanation: "Repeated discretionary spend remains observational until promoted.",
    bucket_assignment: "variable_discretionary",
    confidence_label: "Needs review",
    inferred_cadence: "monthly",
    cadence_confidence: 0.52,
    cadence_reason: "intervals_match_monthly",
    merchant_confidence: 0.81,
    bucket_confidence: 0.64,
    impact_on_baseline: "included",
    included: true,
    observed_only: true,
    review_reasons: ["weak_cadence_evidence", "likely_one_off"],
    group_key: "Entertainment|Movies / Activities|STREAMING SERVICE",
    group_transaction_count: 2,
    review_priority: 51.98,
    likely_merge_targets: [],
    likely_payroll_candidate: false,
    ...overrides,
  };
}

function incomeReviewRow(overrides: Partial<ReviewRow> = {}): ReviewRow {
  return {
    id: "review-income",
    transaction_date: "2026-02-02",
    amount: 850,
    signed_amount: 850,
    direction: "credit",
    movement_type: "income",
    raw_description: "CONSULTING BONUS PTY LTD",
    normalized_description: "CONSULTING BONUS",
    direction_source: "explicit_column",
    interpretation_type: "income_candidate",
    interpretation_confidence: 0.7,
    interpretation_reason: "credit from employer-like entity",
    category: "Income",
    subcategory: "Payroll",
    confidence: 0.7,
    explanation: "Likely payroll-like income needs confirmation.",
    bucket_assignment: "income_irregular",
    confidence_label: "Needs review",
    inferred_cadence: "monthly",
    cadence_confidence: 0.44,
    cadence_reason: "insufficient_observations",
    merchant_confidence: 0.88,
    bucket_confidence: 0.62,
    impact_on_baseline: "included",
    included: true,
    observed_only: true,
    review_reasons: ["likely_payroll_candidate"],
    group_key: "Income|Payroll|CONSULTING BONUS",
    group_transaction_count: 1,
    review_priority: 850,
    likely_merge_targets: [],
    likely_payroll_candidate: true,
    ...overrides,
  };
}

function transferReviewRow(overrides: Partial<ReviewRow> = {}): ReviewRow {
  return {
    id: "review-transfer",
    transaction_date: "2026-02-03",
    amount: 500,
    signed_amount: -500,
    direction: "debit",
    movement_type: "internal_transfer",
    raw_description: "WISE AUSTRALIA PTY 123",
    normalized_description: "WISE AUSTRALIA PTY",
    direction_source: "balance_delta",
    interpretation_type: "internal_transfer",
    interpretation_confidence: 0.82,
    interpretation_reason: "Transfer/payment-rail semantics found.",
    category: "Transfer / Money Movement",
    subcategory: "Internal",
    confidence: 0.74,
    explanation: "Likely transfer or reimbursement.",
    bucket_assignment: "transfer_money_movement",
    confidence_label: "Needs review",
    inferred_cadence: "unknown",
    cadence_confidence: 0.25,
    cadence_reason: "insufficient_observations",
    merchant_confidence: 0.72,
    bucket_confidence: 0.89,
    impact_on_baseline: "excluded",
    included: true,
    observed_only: true,
    review_reasons: ["cadence_ambiguous_material"],
    group_key: "Transfer / Money Movement|Internal|WISE TRANSFER",
    group_transaction_count: 1,
    review_priority: 500,
    likely_merge_targets: [],
    likely_payroll_candidate: false,
    ...overrides,
  };
}

function makeTransactions(groupKey: string, amount: number, count: number, rawPrefix: string, category = "Entertainment", subcategory = "Movies / Activities"): TxRow[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `${groupKey}-${index + 1}`,
    transaction_date: `2026-02-${String(index + 1).padStart(2, "0")}`,
    amount,
    raw_description: `${rawPrefix} ${index + 1}`,
    category,
    subcategory,
    group_key: groupKey,
  }));
}

function makeLinesPayload(state: PhaseState) {
  const model = state.summary.budget_model;
  return {
    items: state.lines,
    totals: {
      monthly_recurring_income: model.recurring_income_monthly,
      observed_irregular_income_total: model.irregular_income_total,
      monthly_recurring_baseline: model.recurring_baseline_monthly,
      monthly_variable_discretionary: model.variable_discretionary_monthly,
      observed_variable_discretionary_total: state.lines
        .filter((line) => String(line.bucket_assignment) === "variable_discretionary" && line.included !== false)
        .reduce((sum, line) => sum + Number(line.observed_amount ?? 0), 0),
      observed_one_off_exceptional_total: model.observed_one_off_total,
      observed_transfer_total: model.observed_transfer_total,
      monthly_income: model.recurring_income_monthly,
      monthly_expenses: model.recurring_baseline_monthly + model.variable_discretionary_monthly,
      monthly_net: model.observed_net,
      net_recurring_monthly: model.core_net,
      net_observed_monthly: model.observed_net,
    },
    statement_truth: {
      total_credits: state.summary.statement_total_credits,
      total_debits: state.summary.statement_total_debits,
      net_movement: state.summary.statement_total_credits - state.summary.statement_total_debits,
    },
    budget_model: model,
    trust: state.summary.trust,
  };
}

function stubShell() {
  cy.intercept("GET", `${apiBase}/api/v1/users/me`, {
    statusCode: 200,
    body: {
      id: "user-1",
      email: "budget@example.com",
      name: "Budget Operator",
      preferred_name: "Budget Operator",
      timezone: "Australia/Melbourne",
    },
  }).as("usersMe");

  cy.intercept("GET", `${apiBase}/api/v1/organizations/me/member*`, {
    statusCode: 200,
    body: { organization_id: "org-1", role: "owner" },
  }).as("orgMeMember");

  cy.intercept("GET", `${apiBase}/api/v1/organizations/me/list*`, {
    statusCode: 200,
    body: [{ id: "org-1", name: "Mission Control", is_active: true }],
  }).as("orgList");

  cy.intercept("GET", `${apiBase}/healthz`, {
    statusCode: 200,
    body: { ok: true },
  }).as("healthz");

  cy.intercept("GET", `${apiBase}/api/v1/control-center`, {
    statusCode: 200,
    body: {
      version: 1,
      modules: [budgetModule],
      network_marketing_view_mode: "pipeline",
    },
  }).as("controlCenterConfig");

  cy.intercept("GET", `${apiBase}/api/v1/control-center/records`, {
    statusCode: 200,
    body: { items: [] },
  }).as("controlCenterRecords");

  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/parsers`, {
    statusCode: 200,
    body: { parsers: [{ name: "nab", banks: ["NAB"], formats: ["pdf"] }] },
  }).as("budgetParsers");
  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/merchant-memory`, {
    statusCode: 200,
    body: { items: [] },
  }).as("budgetMerchantMemory");
}

function stubScenario(
  phases: Record<string, PhaseState>,
  transition: (currentPhase: string, operations: Array<{ operation: string; payload?: Record<string, unknown> }>) => string,
) {
  let phase = "initial";
  stubShell();

  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/latest`, {
    statusCode: 200,
    body: { import_id: importId },
  }).as("budgetLatest");

  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}`, (req) => {
    req.reply({ statusCode: 200, body: phases[phase].summary });
  }).as("budgetSummary");

  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/transactions`, {
    statusCode: 200,
    body: { items: [] },
  }).as("budgetTransactions");

  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/needs-review`, (req) => {
    req.reply({ statusCode: 200, body: { items: phases[phase].reviewItems } });
  }).as("budgetNeedsReview");

  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/lines`, (req) => {
    req.reply({ statusCode: 200, body: makeLinesPayload(phases[phase]) });
  }).as("budgetLines");

  cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/lines/**/transactions`, (req) => {
    const encodedGroupKey = req.url.split("/lines/")[1].replace("/transactions", "");
    const groupKey = decodeURIComponent(encodedGroupKey);
    req.reply({
      statusCode: 200,
      body: {
        items: phases[phase].lineTransactions?.[groupKey] ?? [],
      },
    });
  }).as("budgetLineTransactions");

  cy.intercept("PATCH", `${apiBase}/api/v1/control-center/budget/imports/${importId}/overrides`, (req) => {
    const operations = Array.isArray(req.body?.operations) ? req.body.operations : [];
    phase = transition(phase, operations);
    req.reply({ statusCode: 200, body: { applied: operations.length || 1 } });
  }).as("budgetOverrides");

  cy.intercept("POST", `${apiBase}/api/v1/control-center/budget/imports/${importId}/recompute`, {
    statusCode: 200,
    body: { ok: true },
  }).as("budgetRecompute");
}

function summaryCard(cardKey: string) {
  return cy.get(`[data-cy='budget-summary-card'][data-card-key='${cardKey}']`);
}

function expectSummary(cardKey: string, amount: number) {
  summaryCard(cardKey).should("contain.text", money(amount));
}

function openNeedsAttention() {
  cy.get("[data-cy='budget-needs-attention-toggle']").then(($button) => {
    if ($button.attr("aria-expanded") === "false") {
      cy.wrap($button).click();
    }
  });
}

function openSection(sectionKey: string) {
  cy.get(`[data-cy='budget-section'][data-section-key='${sectionKey}']`)
    .find("[data-cy='budget-section-toggle']")
    .then(($button) => {
      if ($button.attr("aria-expanded") === "false") {
        cy.wrap($button).click();
      }
    });
}

function reviewCard(label: string) {
  return cy.contains("[data-cy='budget-review-card']", label);
}

function lineRow(groupKey: string) {
  return cy.get(`[data-cy='budget-line-row'][data-group-key='${groupKey}']:visible`);
}

function startScenario(phases: Record<string, PhaseState>, transition: (currentPhase: string, operations: Array<{ operation: string; payload?: Record<string, unknown> }>) => string) {
  stubScenario(phases, transition);
  cy.loginWithLocalToken(localToken);
  cy.waitForBudgetWorkspaceLoaded();
  cy.wait(["@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);
}

describe("Budget V2 fixture matrix", () => {
  it("moves an expense to one-off and excludes it from both nets while keeping it visible", () => {
    const groupKey = "Entertainment|Movies / Activities|STREAMING SERVICE";
    startScenario(
      {
        initial: {
          summary: makeSummary({ variable_discretionary_monthly: 25.94 }),
          reviewItems: [expenseReviewRow()],
          lines: [recurringIncomeLine(), baselineLine(), variableLine()],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
        oneOff: {
          summary: makeSummary({ observed_one_off_total: 51.98 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), oneOffLine()],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
      },
      (_phase, operations) => (operations.some((item) => item.operation === "mark_one_off") ? "oneOff" : "initial"),
    );

    openNeedsAttention();
    reviewCard("STREAMING SERVICE").within(() => {
      cy.contains("button", "Treat as one-off").click();
    });
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("variable-spending", 0);
    expectSummary("observed-net", 2500);
    expectSummary("one-off-spending", 25.99);

    cy.get("[data-cy='budget-needs-attention']").should("contain.text", "No items currently need review.");
    openSection("oneoff");
    lineRow(groupKey).should("contain.text", "STREAMING SERVICE").and("contain.text", "Observed amount");
  });

  it("keeps a discretionary expense in variable spending and resolves cadence via monthly selection", () => {
    const groupKey = "Entertainment|Movies / Activities|STREAMING SERVICE";
    startScenario(
      {
        initial: {
          summary: makeSummary({ variable_discretionary_monthly: 25.94 }),
          reviewItems: [expenseReviewRow({ review_reasons: ["cadence_ambiguous_material"], cadence_reason: "insufficient_observations" })],
          lines: [recurringIncomeLine(), baselineLine(), variableLine({ review_reasons: ["cadence_ambiguous_material"], cadence_reason: "insufficient_observations" })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
        monthlyVariable: {
          summary: makeSummary({ variable_discretionary_monthly: 25.94 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), variableLine({ inferred_cadence: "monthly", cadence_reason: "manual_override", review_reasons: [], confidence_label: "Medium confidence" })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
      },
      (_phase, operations) => (operations.some((item) => item.operation === "set_cadence") ? "monthlyVariable" : "initial"),
    );

    openNeedsAttention();
    reviewCard("STREAMING SERVICE").within(() => {
      cy.get("[data-cy='budget-review-diagnostics']").contains("Why the model is unsure").click();
      cy.contains("button", "Set monthly cadence").click();
    });
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("variable-spending", 25.99);
    expectSummary("observed-net", 2474.06);
    openSection("variable");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").should("have.value", "variable_discretionary");
    lineRow(groupKey).find("[data-cy='budget-line-edit-cadence']").should("have.value", "monthly");
    lineRow(groupKey).should("contain.text", "Observed monthly estimate");
  });

  it("moves an expense to baseline via mark recurring and then back to discretionary with last-write-wins totals", () => {
    const groupKey = "Entertainment|Movies / Activities|STREAMING SERVICE";
    startScenario(
      {
        initial: {
          summary: makeSummary({ variable_discretionary_monthly: 25.94 }),
          reviewItems: [expenseReviewRow()],
          lines: [recurringIncomeLine(), baselineLine(), variableLine()],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
        baseline: {
          summary: makeSummary({ recurring_baseline_monthly: 1525.99, core_net: 2474.01, observed_net: 2474.01 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), variableLine({ bucket_assignment: "recurring_baseline", observed_only: false, is_modeled: true, modeled_by_default: true, normalized_monthly: 25.99, normalized_yearly: 311.88, cadence_confidence: 0.92, cadence_reason: "manual_override", review_reasons: [], confidence_label: "High confidence", recurrence_state: "recurring_candidate" })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
        discretionary: {
          summary: makeSummary({ variable_discretionary_monthly: 25.94 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), variableLine({ review_reasons: [], confidence_label: "Medium confidence" })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
      },
      (phase, operations) => {
        if (operations.some((item) => item.operation === "mark_recurring")) return "baseline";
        if (operations.some((item) => item.operation === "set_bucket_assignment" && item.payload?.bucket_assignment === "recurring_baseline")) return "baseline";
        if (operations.some((item) => item.operation === "set_bucket_assignment" && item.payload?.bucket_assignment === "variable_discretionary")) return "discretionary";
        return phase;
      },
    );

    openNeedsAttention();
    reviewCard("STREAMING SERVICE").within(() => {
      cy.contains("button", "Treat as baseline").click();
    });
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);
    expectSummary("recurring-baseline", 25.99);
    expectSummary("core-net", 2474.01);

    openSection("baseline");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").should("have.value", "recurring_baseline");
    lineRow(groupKey).find("[data-cy='budget-line-edit-cadence']").should("have.value", "monthly");
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").select("variable_discretionary");
    lineRow(groupKey).find("[data-cy='budget-line-save']").click();
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("recurring-baseline", 0);
    expectSummary("variable-spending", 25.99);
    openSection("variable");
    lineRow(groupKey).should("contain.text", "STREAMING SERVICE");
  });

  it("marks income recurring and then saves it back to one-off, keeping irregular income out of both nets", () => {
    const groupKey = "Income|Payroll|CONSULTING BONUS";
    startScenario(
      {
        initial: {
          summary: makeSummary({ irregular_income_total: 850 }),
          reviewItems: [incomeReviewRow()],
          lines: [recurringIncomeLine(), baselineLine(), incomeIrregularLine()],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 850, 1, "CONSULTING BONUS", "Income", "Payroll") },
        },
        recurringIncome: {
          summary: makeSummary({ recurring_income_monthly: 4850, core_net: 3350, observed_net: 3350 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), recurringIncomeLine({ id: "line-income-2", group_key: groupKey, group_label: "CONSULTING BONUS", base_amount: 850, normalized_monthly: 850, observed_amount: 850 })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 850, 1, "CONSULTING BONUS", "Income", "Payroll") },
        },
        oneOffIncome: {
          summary: makeSummary({ irregular_income_total: 850 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), incomeIrregularLine({ group_key: groupKey, confidence_label: "Medium confidence" })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 850, 1, "CONSULTING BONUS", "Income", "Payroll") },
        },
      },
      (phase, operations) => {
        if (operations.some((item) => item.operation === "mark_recurring")) return "recurringIncome";
        if (operations.some((item) => item.operation === "set_bucket_assignment" && item.payload?.bucket_assignment === "income_recurring")) return "recurringIncome";
        if (operations.some((item) => item.operation === "set_bucket_assignment" && item.payload?.bucket_assignment === "income_irregular")) return "oneOffIncome";
        if (operations.some((item) => item.operation === "mark_one_off")) return "oneOffIncome";
        return phase;
      },
    );

    openNeedsAttention();
    reviewCard("CONSULTING BONUS").within(() => {
      cy.contains("button", "Treat as recurring income").click();
    });
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);
    expectSummary("income", 4850);
    expectSummary("core-net", 3350);

    openSection("income");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").should("have.value", "income_recurring");
    lineRow(groupKey).find("[data-cy='budget-line-edit-cadence']").should("have.value", "monthly");
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").select("income_irregular");
    lineRow(groupKey).find("[data-cy='budget-line-save']").click();
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("income", 4850);
    expectSummary("core-net", 2500);
    expectSummary("observed-net", 2500);
    openSection("income");
    lineRow(groupKey).should("contain.text", "Irregular income");
  });

  ["weekly", "fortnightly", "monthly", "quarterly", "irregular"].forEach((cadence) => {
    it(`applies ${cadence} cadence to a transfer review item without leaking it into spending totals`, () => {
      const groupKey = "Transfer / Money Movement|Internal|WISE TRANSFER";
      startScenario(
        {
          initial: {
            summary: makeSummary({ observed_transfer_total: 500 }),
            reviewItems: [transferReviewRow()],
            lines: [recurringIncomeLine(), baselineLine(), transferLine()],
            lineTransactions: { [groupKey]: makeTransactions(groupKey, 500, 1, "WISE TRANSFER", "Transfer / Money Movement", "Internal") },
          },
          resolved: {
            summary: makeSummary({ observed_transfer_total: 500 }),
            reviewItems: [],
            lines: [recurringIncomeLine(), baselineLine(), transferLine({ inferred_cadence: cadence, cadence_reason: "manual_override", review_reasons: [], confidence_label: "Medium confidence" })],
            lineTransactions: { [groupKey]: makeTransactions(groupKey, 500, 1, "WISE TRANSFER", "Transfer / Money Movement", "Internal") },
          },
        },
        (_phase, operations) => (operations.some((item) => item.operation === "set_cadence") ? "resolved" : "initial"),
      );

      openNeedsAttention();
      reviewCard("WISE AUSTRALIA PTY").within(() => {
        cy.get("[data-cy='budget-review-diagnostics']").contains("Why the model is unsure").click();
        const cadenceLabel = cadence === "irregular" ? "Set irregular cadence" : `Set ${cadence} cadence`;
        cy.contains("button", cadenceLabel).click();
      });
      cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

      expectSummary("core-net", 2500);
      expectSummary("observed-net", 2500);
      expectSummary("one-off-spending", 0);
      openSection("transfer");
      lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
      lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").should("have.value", "transfer_money_movement");
      lineRow(groupKey).find("[data-cy='budget-line-edit-cadence']").should("have.value", cadence);
    });
  });

  it("can move a transfer-style item to one-off from the cadence action block", () => {
    const groupKey = "Transfer / Money Movement|Internal|WISE TRANSFER";
    startScenario(
      {
        initial: {
          summary: makeSummary({ observed_transfer_total: 500 }),
          reviewItems: [transferReviewRow()],
          lines: [recurringIncomeLine(), baselineLine(), transferLine()],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 500, 1, "WISE TRANSFER", "Transfer / Money Movement", "Internal") },
        },
        oneOffTransfer: {
          summary: makeSummary({ observed_one_off_total: 500 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), oneOffLine({ group_key: groupKey, group_label: "WISE TRANSFER", category: "Transfer / Money Movement", subcategory: "Internal", observed_amount: 500, observed_window_total: 500 })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 500, 1, "WISE TRANSFER", "Transfer / Money Movement", "Internal") },
        },
      },
      (_phase, operations) => (operations.some((item) => item.operation === "mark_one_off") ? "oneOffTransfer" : "initial"),
    );

    openNeedsAttention();
    reviewCard("WISE AUSTRALIA PTY").within(() => {
      cy.contains("button", "Treat as one-off").click();
    });
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("one-off-spending", 500);
    expectSummary("observed-net", 2500);
    openSection("oneoff");
    lineRow(groupKey).should("contain.text", "WISE TRANSFER");
  });

  it("lets a client exclude a visible one-off expense from calculations via the line editor", () => {
    const groupKey = "Entertainment|Movies / Activities|FLIGHTS";
    startScenario(
      {
        initial: {
          summary: makeSummary({ observed_one_off_total: 220 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), oneOffLine({ group_key: groupKey, group_label: "FLIGHTS", category: "Entertainment", subcategory: "Movies / Activities", observed_amount: 220, observed_window_total: 220 })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 220, 1, "FLIGHTS", "Expenses", "Travel") },
        },
        excluded: {
          summary: makeSummary({ observed_one_off_total: 0 }),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), oneOffLine({ group_key: groupKey, group_label: "FLIGHTS", category: "Entertainment", subcategory: "Movies / Activities", observed_amount: 220, observed_window_total: 220, included: false, impact_on_baseline: "excluded" })],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 220, 1, "FLIGHTS", "Expenses", "Travel") },
        },
      },
      (_phase, operations) => (operations.some((item) => item.operation === "set_include" && item.payload?.included === false) ? "excluded" : "initial"),
    );

    openSection("oneoff");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-include']").uncheck({ force: true });
    lineRow(groupKey).find("[data-cy='budget-line-save']").click();
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("one-off-spending", 0);
    openSection("oneoff");
    lineRow(groupKey).should("contain.text", "FLIGHTS").and("contain.text", "Visible, but excluded from calculations");
  });

  it("keeps an explicit line-editor bucket choice even when cadence is irregular", () => {
    const groupKey = "General / Home|Grocery Shopping|WOOLWORTHS";
    startScenario(
      {
        initial: {
          summary: makeSummary({ variable_discretionary_monthly: 141.9, observed_net: 2358.1 }),
          reviewItems: [],
          lines: [
            recurringIncomeLine(),
            baselineLine(),
            variableLine({
              id: "line-groceries",
              group_key: groupKey,
              group_label: "WOOLWORTHS",
              category: "General / Home",
              subcategory: "Grocery Shopping",
              observed_window_total: 141.9,
              observed_amount: 141.9,
              observational_monthly_estimate: 141.9,
              base_amount: 23.65,
              source_amount: 23.65,
              transaction_count: 6,
              review_reasons: ["cadence_ambiguous_material", "low_confidence"],
              inferred_cadence: "irregular",
              cadence_confidence: 0.52,
              cadence_reason: "intervals_inconsistent",
            }),
          ],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 23.65, 6, "WOOLWORTHS", "General / Home", "Grocery Shopping") },
        },
        savedBaseline: {
          summary: makeSummary({ recurring_baseline_monthly: 1523.65, variable_discretionary_monthly: 0, core_net: 2476.35, observed_net: 2476.35 }),
          reviewItems: [],
          lines: [
            recurringIncomeLine(),
            baselineLine(),
            variableLine({
              id: "line-groceries",
              group_key: groupKey,
              group_label: "WOOLWORTHS",
              category: "General / Home",
              subcategory: "Grocery Shopping",
              bucket_assignment: "recurring_baseline",
              observed_only: false,
              is_modeled: true,
              modeled_by_default: false,
              modeling_status: "user_forced_recurring",
              normalized_monthly: 23.65,
              normalized_yearly: 283.8,
              observed_window_total: 141.9,
              observed_amount: 141.9,
              base_amount: 23.65,
              source_amount: 23.65,
              source_period: "monthly",
              base_period: "monthly",
              transaction_count: 6,
              review_reasons: ["cadence_ambiguous_material", "low_confidence"],
              inferred_cadence: "irregular",
              cadence_confidence: 0.52,
              cadence_reason: "intervals_inconsistent",
            }),
          ],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 23.65, 6, "WOOLWORTHS", "General / Home", "Grocery Shopping") },
        },
        savedVariable: {
          summary: makeSummary({ recurring_baseline_monthly: 1500, variable_discretionary_monthly: 141.9, core_net: 2500, observed_net: 2358.1 }),
          reviewItems: [],
          lines: [
            recurringIncomeLine(),
            baselineLine(),
            variableLine({
              id: "line-groceries",
              group_key: groupKey,
              group_label: "WOOLWORTHS",
              category: "General / Home",
              subcategory: "Grocery Shopping",
              bucket_assignment: "variable_discretionary",
              observed_only: true,
              is_modeled: false,
              modeled_by_default: false,
              modeling_status: "observational_only",
              normalized_monthly: 0,
              observed_window_total: 141.9,
              observed_amount: 141.9,
              observational_monthly_estimate: 141.9,
              base_amount: 23.65,
              source_amount: 23.65,
              source_period: "monthly",
              base_period: "monthly",
              transaction_count: 6,
              review_reasons: ["cadence_ambiguous_material", "low_confidence"],
              inferred_cadence: "irregular",
              cadence_confidence: 0.52,
              cadence_reason: "intervals_inconsistent",
            }),
          ],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 23.65, 6, "WOOLWORTHS", "General / Home", "Grocery Shopping") },
        },
      },
      (phase, operations) => {
        const bucketOp = operations.find((item) => item.operation === "set_bucket_assignment");
        if (!bucketOp) return phase;
        const bucket = String(bucketOp.payload?.bucket_assignment ?? "");
        if (bucket === "recurring_baseline") return "savedBaseline";
        if (bucket === "variable_discretionary") return "savedVariable";
        return phase;
      },
    );

    openSection("variable");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-cadence']").select("irregular");
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").select("recurring_baseline");
    lineRow(groupKey).find("[data-cy='budget-line-save']").click();
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("recurring-baseline", 1523.65);
    openSection("baseline");
    lineRow(groupKey).should("contain.text", "WOOLWORTHS");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").should("have.value", "recurring_baseline");
    lineRow(groupKey).find("[data-cy='budget-line-edit-cadence']").should("have.value", "irregular");

    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").select("variable_discretionary");
    lineRow(groupKey).find("[data-cy='budget-line-save']").click();
    cy.wait(["@budgetOverrides", "@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("recurring-baseline", 1500);
    expectSummary("variable-spending", 141.9);
    openSection("variable");
    lineRow(groupKey).should("contain.text", "WOOLWORTHS");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-bucket']").should("have.value", "variable_discretionary");
  });

  it("saves category, subcategory, notes, and future-match mapping from the line editor", () => {
    const groupKey = "General / Home|Grocery Shopping|WOOLWORTHS";
    startScenario(
      {
        initial: {
          summary: makeSummary({ variable_discretionary_monthly: 141.9, observed_net: 2358.1 }),
          reviewItems: [],
          lines: [
            recurringIncomeLine(),
            variableLine({
              id: "line-groceries",
              group_key: groupKey,
              group_label: "WOOLWORTHS",
              category: "General / Home",
              subcategory: "Grocery Shopping",
              observed_window_total: 141.9,
              observed_amount: 141.9,
              observational_monthly_estimate: 141.9,
              base_amount: 23.65,
              source_amount: 23.65,
              transaction_count: 6,
            }),
          ],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 23.65, 6, "WOOLWORTHS", "General / Home", "Grocery Shopping") },
        },
        retagged: {
          summary: makeSummary({ variable_discretionary_monthly: 141.9, observed_net: 2358.1 }),
          reviewItems: [],
          lines: [
            recurringIncomeLine(),
            variableLine({
              id: "line-groceries",
              group_key: groupKey,
              group_label: "WOOLWORTHS",
              category: "Entertainment",
              subcategory: "Eating out / takeaways",
              observed_window_total: 141.9,
              observed_amount: 141.9,
              observational_monthly_estimate: 141.9,
              base_amount: 23.65,
              source_amount: 23.65,
              notes: "Client retagged after audit",
              transaction_count: 6,
            }),
          ],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 23.65, 6, "WOOLWORTHS", "Entertainment", "Eating out / takeaways") },
        },
      },
      (phase, operations) => {
        const opNames = operations.map((item) => item.operation);
        const hasRetag = opNames.includes("set_category") && opNames.includes("set_subcategory") && opNames.includes("set_notes");
        const hasRemember = opNames.includes("remember_mapping");
        return hasRetag && hasRemember ? "retagged" : phase;
      },
    );

    openSection("variable");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-category']").select("Entertainment");
    lineRow(groupKey).find("[data-cy='budget-line-edit-subcategory']").select("Eating out / takeaways");
    lineRow(groupKey).find("[data-cy='budget-line-edit-remember']").check({ force: true });
    lineRow(groupKey).find("[data-cy='budget-line-edit-notes']").clear().type("Client retagged after audit");
    lineRow(groupKey).find("[data-cy='budget-line-save']").click();
    cy.wait("@budgetOverrides").then((interception) => {
      const operations = interception.request.body.operations as Array<{ operation: string }>;
      expect(operations.map((item) => item.operation)).to.include.members([
        "set_category",
        "set_subcategory",
        "set_notes",
        "remember_mapping",
      ]);
    });
    cy.wait(["@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    lineRow(groupKey).should("contain.text", "Entertainment").and("contain.text", "Eating out / takeaways");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-category']").should("have.value", "Entertainment");
    lineRow(groupKey).find("[data-cy='budget-line-edit-subcategory']").should("have.value", "Eating out / takeaways");
    lineRow(groupKey).find("[data-cy='budget-line-edit-notes']").should("have.value", "Client retagged after audit");
    lineRow(groupKey).find("[data-cy='budget-line-edit-remember']").should("not.be.checked");
  });

  it("saves base amount and base period from the line editor and updates modeled totals", () => {
    const groupKey = "Entertainment|Movies / Activities|STREAMING SERVICE";
    startScenario(
      {
        initial: {
          summary: makeSummary({ recurring_baseline_monthly: 1500, core_net: 2500, observed_net: 2500 }),
          reviewItems: [],
          lines: [
            recurringIncomeLine(),
            baselineLine(),
            variableLine({
              group_key: groupKey,
              group_label: "STREAMING SERVICE",
              bucket_assignment: "recurring_baseline",
              observed_only: false,
              is_modeled: true,
              modeled_by_default: false,
              modeling_status: "user_forced_recurring",
              base_amount: 25.99,
              source_amount: 25.99,
              source_period: "monthly",
              base_period: "monthly",
              normalized_monthly: 25.99,
              normalized_yearly: 311.88,
            }),
          ],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
        updatedAmount: {
          summary: makeSummary({ recurring_baseline_monthly: 1560, core_net: 2440, observed_net: 2440 }),
          reviewItems: [],
          lines: [
            recurringIncomeLine(),
            baselineLine(),
            variableLine({
              group_key: groupKey,
              group_label: "STREAMING SERVICE",
              bucket_assignment: "recurring_baseline",
              observed_only: false,
              is_modeled: true,
              modeled_by_default: false,
              modeling_status: "user_forced_recurring",
              base_amount: 60,
              source_amount: 60,
              source_period: "monthly",
              base_period: "monthly",
              normalized_monthly: 60,
              normalized_yearly: 720,
            }),
          ],
          lineTransactions: { [groupKey]: makeTransactions(groupKey, 25.99, 2, "STREAMING SERVICE") },
        },
      },
      (phase, operations) => {
        const hasBaseAmountUpdate = operations.some((item) => item.operation === "set_base_amount_period" && Number(item.payload?.base_amount) === 60);
        return hasBaseAmountUpdate ? "updatedAmount" : phase;
      },
    );

    openSection("baseline");
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-base-amount']").clear().type("60");
    lineRow(groupKey).find("[data-cy='budget-line-edit-base-period']").select("monthly");
    lineRow(groupKey).find("[data-cy='budget-line-save']").click();
    cy.wait("@budgetOverrides").then((interception) => {
      const operations = interception.request.body.operations as Array<{ operation: string; payload?: Record<string, unknown> }>;
      const baseOp = operations.find((item) => item.operation === "set_base_amount_period");
      expect(baseOp?.payload?.base_amount).to.equal(60);
      expect(baseOp?.payload?.base_period).to.equal("monthly");
    });
    cy.wait(["@budgetRecompute", "@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummary("recurring-baseline", 1560);
    lineRow(groupKey).find("[data-cy='budget-line-edit-toggle']").click();
    lineRow(groupKey).find("[data-cy='budget-line-edit-base-amount']").should("have.value", "60");
    lineRow(groupKey).find("[data-cy='budget-line-edit-base-period']").should("have.value", "monthly");
  });

  it("keeps trust-blocked modeling explicit while leaving visible one-off spending intact", () => {
    startScenario(
      {
        initial: {
          summary: makeSummary(
            {
              variable_discretionary_monthly: 0,
              observed_one_off_total: 220,
              modeling_allowed: false,
              modeling_restrictions: ["Statement range is too short for trusted recurrence modeling."],
              core_net: 2500,
              observed_net: 2500,
            },
            {
              modeling_allowed: false,
              modeling_restrictions: ["Statement range is too short for trusted recurrence modeling."],
              totals_trust_level: "provisional",
              truth_trust_level: "provisional",
            },
          ),
          reviewItems: [],
          lines: [recurringIncomeLine(), baselineLine(), oneOffLine({ group_key: "Entertainment|Movies / Activities|HOTEL", group_label: "HOTEL", category: "Entertainment", subcategory: "Movies / Activities", observed_amount: 220, observed_window_total: 220 })],
          lineTransactions: {},
        },
      },
      (phase) => phase,
    );

    expectSummary("core-net", 2500);
    expectSummary("observed-net", 2500);
    summaryCard("variable-spending").should("contain.text", "Statement range is too short for trusted recurrence modeling.");
    expectSummary("one-off-spending", 220);
  });

  it("shows grouped underlying transactions and keeps line totals consistent with the detail list", () => {
    const groupKey = "General / Home|Grocery Shopping|ALDI";
    const txs = [
      ...makeTransactions(groupKey, 40, 1, "ALDI", "General / Home", "Grocery Shopping"),
      ...makeTransactions(groupKey, 60, 1, "ALDI", "General / Home", "Grocery Shopping").map((item, idx) => ({ ...item, id: `aldi-${idx + 2}`, transaction_date: "2026-02-02" })),
    ];
    startScenario(
      {
        initial: {
          summary: makeSummary({ variable_discretionary_monthly: 50 }),
          reviewItems: [expenseReviewRow({ raw_description: "ALDI MELBOURNE", normalized_description: "ALDI", merchant_candidate: "ALDI", category: "General / Home", subcategory: "Grocery Shopping", group_key: groupKey, group_transaction_count: 2, amount: 50, review_priority: 100, review_reasons: ["weak_cadence_evidence"] })],
          lines: [recurringIncomeLine(), baselineLine(), variableLine({ group_key: groupKey, group_label: "ALDI", category: "General / Home", subcategory: "Grocery Shopping", observed_amount: 50, observed_window_total: 100, observational_monthly_estimate: 50, transaction_count: 2 })],
          lineTransactions: { [groupKey]: txs },
        },
      },
      (phase) => phase,
    );

    openNeedsAttention();
    reviewCard("ALDI").within(() => {
      cy.contains("button", "Review related transactions").click();
    });
    cy.wait("@budgetLineTransactions");
    reviewCard("ALDI").find("[data-cy='budget-review-underlying-panel']").within(() => {
      cy.contains("2026-02-01").should("be.visible");
      cy.contains("2026-02-02").should("be.visible");
      cy.contains(money(40)).should("be.visible");
      cy.contains(money(60)).should("be.visible");
      cy.contains("ALDI").should("be.visible");
    });

    openSection("variable");
    lineRow(groupKey).should("contain.text", money(50));
  });
});
