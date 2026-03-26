/// <reference types="cypress" />

Cypress.on("uncaught:exception", (err) => {
  if (err.message?.includes("Hydration failed") || err.message?.includes("Minified React error #418")) {
    return false;
  }
  return true;
});

describe("Budget V2", () => {
  const apiBase = "https://api.echoheelixmissioncontrol.com";
  const localToken = "x".repeat(64);
  const importId = "11111111-1111-1111-1111-111111111111";
  let phase: "initial" | "afterOneOff" = "initial";

  const budgetModule = {
    id: "budget",
    slug: "budget",
    title: "Budget",
    description: "Statement-backed budgeting",
    category: "finance",
    enabled: true,
    order: 1,
  };

  const baseSummary = {
    import_id: importId,
    status: "completed",
    source_bank: "NAB",
    parser_name: "nab",
    parser_confidence: 0.94,
    parser_warnings: [],
    statement_start_date: "2026-01-01",
    statement_end_date: "2026-03-02",
    transaction_count: 4,
    scope_warnings: [],
    parser_coverage_warnings: [],
    parsed_debit_count: 3,
    parsed_credit_count: 1,
    parsed_debit_total: 1551.98,
    parsed_credit_total: 4000.0,
    opening_balance: 1000.0,
    closing_balance: 3448.02,
    statement_total_debits: 1551.98,
    statement_total_credits: 4000.0,
    expected_closing_balance: 3448.02,
    reconciliation_status: "reconciled",
    reconciliation_reason: null,
    reconciliation_difference: 0.0,
    warning_reasons: [],
    document_type: "statement",
    document_reconcilable: true,
    document_warnings: [],
    budget_model: {
      recurring_income_monthly: 4000.0,
      recurring_baseline_monthly: 1500.0,
      variable_discretionary_monthly: 25.94,
      observed_one_off_total: 0.0,
      observed_transfer_total: 0.0,
      irregular_income_total: 0.0,
      core_net: 2500.0,
      observed_net: 2474.06,
      modeling_allowed: true,
      modeling_restrictions: [],
    },
    trust: {
      reconciliation_status: "reconciled",
      totals_trust_level: "verified",
      truth_trust_level: "verified",
      modeling_allowed: true,
      modeling_restrictions: [],
      trust_reasons: [],
    },
  };

  const oneOffSummary = {
    ...baseSummary,
    budget_model: {
      ...baseSummary.budget_model,
      variable_discretionary_monthly: 0.0,
      observed_one_off_total: 51.98,
      observed_net: 2500.0,
    },
  };

  const initialReviewRow = {
    id: "22222222-2222-2222-2222-222222222222",
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
    explanation: "Repeated discretionary spend remains observational until the user promotes it or evidence strengthens.",
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
  };

  const recurringIncomeLine = {
    id: "33333333-3333-3333-3333-333333333333",
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
    base_amount: 4000.0,
    base_period: "monthly",
    authoritative_field: "base_amount",
    source_amount: 4000.0,
    source_period: "monthly",
    observed_window_total: 4000.0,
    normalized_weekly: 923.08,
    normalized_fortnightly: 1846.15,
    normalized_monthly: 4000.0,
    normalized_yearly: 48000.0,
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
    observed_amount: 4000.0,
    observational_monthly_estimate: null,
    observed_frequency_label: "1 occurrence",
    duplicate_group_candidates: [],
    merge_candidate_confidence: 0,
  };

  const recurringBaselineLine = {
    id: "44444444-4444-4444-4444-444444444444",
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
    base_amount: 1500.0,
    base_period: "monthly",
    authoritative_field: "base_amount",
    source_amount: 1500.0,
    source_period: "monthly",
    observed_window_total: 1500.0,
    normalized_weekly: 346.15,
    normalized_fortnightly: 692.31,
    normalized_monthly: 1500.0,
    normalized_yearly: 18000.0,
    impact_on_baseline: "included",
    included: true,
    transaction_count: 1,
    confidence_label: "High confidence",
    explanation: "Baseline-leaning merchant family with sufficient cadence evidence is modeled as recurring.",
    review_reasons: [],
    modeling_status: "modeled_recurring",
    recurrence_state: "recurring_candidate",
    is_modeled: true,
    modeled_by_default: true,
    merchant_confidence: 0.95,
    bucket_confidence: 0.95,
    observed_amount: 1500.0,
    observational_monthly_estimate: null,
    observed_frequency_label: "1 occurrence",
    duplicate_group_candidates: [],
    merge_candidate_confidence: 0,
  };

  const initialVariableLine = {
    id: "55555555-5555-5555-5555-555555555555",
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
    normalized_weekly: 6.0,
    normalized_fortnightly: 12.0,
    normalized_monthly: 0.0,
    normalized_yearly: 0.0,
    impact_on_baseline: "included",
    included: true,
    transaction_count: 2,
    confidence_label: "Needs review",
    explanation: "Repeated discretionary spend remains observational until the user promotes it or evidence strengthens.",
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
  };

  const oneOffLine = {
    ...initialVariableLine,
    bucket_assignment: "one_off_exceptional",
    review_reasons: [],
    observational_monthly_estimate: 0,
    confidence_label: "Medium confidence",
    recurrence_state: "one_off_candidate",
    inferred_cadence: "irregular",
  };

  function expectSummaryCardValue(cardKey: string, amount: number) {
    const formatted = amount.toLocaleString("en-AU", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    cy.get(`[data-cy='budget-summary-card'][data-card-key='${cardKey}']`).should("contain.text", formatted);
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
  }

  function stubBudgetApis() {
    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/parsers`, {
      statusCode: 200,
      body: { parsers: [{ name: "nab", banks: ["NAB"], formats: ["pdf"] }] },
    }).as("budgetParsers");
    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/merchant-memory`, {
      statusCode: 200,
      body: { items: [] },
    }).as("budgetMerchantMemory");

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/latest`, {
      statusCode: 200,
      body: { import_id: importId },
    }).as("budgetLatest");

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}`, (req) => {
      req.reply({ statusCode: 200, body: phase === "initial" ? baseSummary : oneOffSummary });
    }).as("budgetSummary");

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/transactions`, {
      statusCode: 200,
      body: { items: [] },
    }).as("budgetTransactions");

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/needs-review`, (req) => {
      req.reply({ statusCode: 200, body: { items: phase === "initial" ? [initialReviewRow] : [] } });
    }).as("budgetNeedsReview");

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/lines`, (req) => {
      const items = phase === "initial"
        ? [recurringIncomeLine, recurringBaselineLine, initialVariableLine]
        : [recurringIncomeLine, recurringBaselineLine, oneOffLine];
      const totals = phase === "initial"
        ? {
            monthly_recurring_income: 4000.0,
            observed_irregular_income_total: 0.0,
            monthly_recurring_baseline: 1500.0,
            monthly_variable_discretionary: 25.94,
            observed_variable_discretionary_total: 25.99,
            observed_one_off_exceptional_total: 0.0,
            observed_transfer_total: 0.0,
            monthly_income: 4000.0,
            monthly_expenses: 1525.94,
            monthly_net: 2474.06,
            net_recurring_monthly: 2500.0,
            net_observed_monthly: 2474.06,
          }
        : {
            monthly_recurring_income: 4000.0,
            observed_irregular_income_total: 0.0,
            monthly_recurring_baseline: 1500.0,
            monthly_variable_discretionary: 0.0,
            observed_variable_discretionary_total: 0.0,
            observed_one_off_exceptional_total: 51.98,
            observed_transfer_total: 0.0,
            monthly_income: 4000.0,
            monthly_expenses: 1500.0,
            monthly_net: 2500.0,
            net_recurring_monthly: 2500.0,
            net_observed_monthly: 2500.0,
          };
      req.reply({
        statusCode: 200,
        body: {
          items,
          totals,
          statement_truth: { total_credits: 4000.0, total_debits: 1551.98, net_movement: 2448.02 },
          budget_model: phase === "initial" ? baseSummary.budget_model : oneOffSummary.budget_model,
          trust: baseSummary.trust,
        },
      });
    }).as("budgetLines");

    cy.intercept("PATCH", `${apiBase}/api/v1/control-center/budget/imports/${importId}/overrides`, {
      statusCode: 200,
      body: { applied: 1 },
    }).as("budgetOverrides");

    cy.intercept("POST", `${apiBase}/api/v1/control-center/budget/imports/${importId}/recompute`, (req) => {
      phase = "afterOneOff";
      req.reply({ statusCode: 200, body: { ok: true } });
    }).as("budgetRecompute");
  }

  beforeEach(() => {
    phase = "initial";
    stubShell();
    stubBudgetApis();
  });

  it("uses backend totals and moves a reviewed expense into one-off with recalculated summary cards", () => {
    cy.loginWithLocalToken(localToken);
    cy.waitForBudgetWorkspaceLoaded();
    cy.wait(["@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    expectSummaryCardValue("variable-spending", 25.99);
    expectSummaryCardValue("observed-net", 2474.06);

    cy.contains("Needs Attention").click();
    cy.contains("STREAMING SERVICE").should("be.visible");
    cy.contains("button", "Treat as one-off").click();

    cy.wait("@budgetOverrides");
    cy.wait("@budgetRecompute");
    cy.wait(["@budgetSummary", "@budgetNeedsReview", "@budgetLines"]);

    cy.contains("Needs Attention")
      .closest("section, div")
      .parent()
      .within(() => {
        cy.contains("No items currently need review.").should("be.visible");
      });
    expectSummaryCardValue("variable-spending", 0);
    expectSummaryCardValue("observed-net", 2500);
    expectSummaryCardValue("one-off-spending", 25.99);

    cy.contains("One-off / Irregular Expenses").click();
    cy.contains("STREAMING SERVICE").should("be.visible");
  });
});
