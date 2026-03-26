/// <reference types="cypress" />

Cypress.on("uncaught:exception", (err) => {
  if (err.message?.includes("Hydration failed") || err.message?.includes("Minified React error #418")) {
    return false;
  }
  return true;
});

describe("Budget V2 mobile layout", () => {
  const apiBase = "https://api.echoheelixmissioncontrol.com";
  const localToken = "x".repeat(64);
  const importId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
  const budgetModule = {
    id: "budget",
    slug: "budget",
    title: "Budget",
    description: "Statement-backed budgeting",
    category: "finance",
    enabled: true,
    order: 1,
  };

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
    });

    cy.intercept("GET", `${apiBase}/api/v1/organizations/me/member*`, {
      statusCode: 200,
      body: { organization_id: "org-1", role: "owner" },
    });

    cy.intercept("GET", `${apiBase}/api/v1/organizations/me/list*`, {
      statusCode: 200,
      body: [{ id: "org-1", name: "Mission Control", is_active: true }],
    });

    cy.intercept("GET", `${apiBase}/healthz`, {
      statusCode: 200,
      body: { ok: true },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center`, {
      statusCode: 200,
      body: {
        version: 1,
        modules: [budgetModule],
        network_marketing_view_mode: "pipeline",
      },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/records`, {
      statusCode: 200,
      body: { items: [] },
    });
  }

  function stubBudgetApis() {
    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/parsers`, {
      statusCode: 200,
      body: { parsers: [{ name: "nab", banks: ["NAB"], formats: ["pdf"] }] },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/merchant-memory`, {
      statusCode: 200,
      body: { items: [] },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/latest`, {
      statusCode: 200,
      body: { import_id: importId },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}`, {
      statusCode: 200,
      body: {
        import_id: importId,
        status: "completed",
        source_bank: "NAB",
        parser_name: "nab",
        parser_confidence: 0.94,
        parser_warnings: [],
        statement_start_date: "2026-01-01",
        statement_end_date: "2026-03-31",
        transaction_count: 9,
        scope_warnings: [],
        parser_coverage_warnings: [],
        parsed_debit_count: 8,
        parsed_credit_count: 1,
        parsed_debit_total: 2222.22,
        parsed_credit_total: 4000.0,
        opening_balance: 1000.0,
        closing_balance: 2777.78,
        statement_total_debits: 2222.22,
        statement_total_credits: 4000.0,
        expected_closing_balance: 2777.78,
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
          variable_discretionary_monthly: 210.35,
          observed_one_off_total: 120.0,
          observed_transfer_total: 0.0,
          irregular_income_total: 0.0,
          core_net: 2500.0,
          observed_net: 2289.65,
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
      },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/transactions`, {
      statusCode: 200,
      body: { items: [] },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/needs-review`, {
      statusCode: 200,
      body: {
        items: [
          {
            id: "review-1",
            transaction_date: "2026-03-24",
            amount: 125.32,
            signed_amount: -125.32,
            direction: "debit",
            movement_type: "expense",
            raw_description: "OPENAI *CHATGPT SUBSCR OPENAI.COM",
            normalized_description: "OPENAI CHATGPT",
            direction_source: "balance_delta",
            interpretation_type: "consumer_spend",
            interpretation_confidence: 0.64,
            interpretation_reason: "insufficient_evidence_for_final_expense_bucket",
            category: "Discretionary",
            subcategory: "Chatgpt",
            confidence: 0.64,
            explanation: "Repeated discretionary spend remains observational.",
            bucket_assignment: "variable_discretionary",
            confidence_label: "Needs review",
            inferred_cadence: "unknown",
            cadence_confidence: 0.32,
            cadence_reason: "insufficient_observations",
            merchant_confidence: 0.83,
            bucket_confidence: 0.64,
            impact_on_baseline: "included",
            included: true,
            observed_only: true,
            review_reasons: ["cadence_ambiguous_material", "low_confidence"],
            group_key: "Discretionary|Chatgpt|OPENAI",
            group_transaction_count: 4,
            review_priority: 125.32,
            likely_merge_targets: [],
            likely_payroll_candidate: false,
          },
        ],
      },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/lines`, {
      statusCode: 200,
      body: {
        items: [
          {
            id: "line-1",
            group_key: "Discretionary|Animals|PET",
            group_label: "PET",
            line_type: "expense",
            category: "Discretionary",
            subcategory: "Animals",
            inferred_cadence: "unknown",
            cadence_confidence: 0.42,
            cadence_reason: "single_observation",
            observed_only: true,
            bucket_assignment: "variable_discretionary",
            bucket_suggestion: "variable_discretionary",
            base_amount: 120.0,
            base_period: "monthly",
            authoritative_field: "base_amount",
            source_amount: 120.0,
            source_period: "monthly",
            observed_window_total: 120.0,
            normalized_weekly: 0.0,
            normalized_fortnightly: 0.0,
            normalized_monthly: 0.0,
            normalized_yearly: 0.0,
            impact_on_baseline: "included",
            included: true,
            transaction_count: 1,
            confidence_label: "Medium confidence",
            explanation: "Observed discretionary spending only.",
            review_reasons: [],
            modeling_status: "observational_only",
            recurrence_state: "discretionary_candidate",
            is_modeled: false,
            modeled_by_default: false,
            merchant_confidence: 0.7,
            bucket_confidence: 0.6,
            observed_amount: 120.0,
            observational_monthly_estimate: 121.76,
            observed_frequency_label: "Observed only",
            duplicate_group_candidates: [],
            merge_candidate_confidence: 0,
          },
          {
            id: "line-2",
            group_key: "Discretionary|Chatgpt|OPENAI",
            group_label: "OPENAI",
            line_type: "expense",
            category: "Discretionary",
            subcategory: "Chatgpt",
            inferred_cadence: "unknown",
            cadence_confidence: 0.32,
            cadence_reason: "insufficient_observations",
            observed_only: true,
            bucket_assignment: "variable_discretionary",
            bucket_suggestion: "variable_discretionary",
            base_amount: 125.32,
            base_period: "monthly",
            authoritative_field: "base_amount",
            source_amount: 125.32,
            source_period: "monthly",
            observed_window_total: 125.32,
            normalized_weekly: 0.0,
            normalized_fortnightly: 0.0,
            normalized_monthly: 0.0,
            normalized_yearly: 0.0,
            impact_on_baseline: "included",
            included: true,
            transaction_count: 4,
            confidence_label: "Needs review",
            explanation: "Observed discretionary spending only.",
            review_reasons: ["cadence_ambiguous_material", "low_confidence"],
            modeling_status: "observational_only",
            recurrence_state: "discretionary_candidate",
            is_modeled: false,
            modeled_by_default: false,
            merchant_confidence: 0.83,
            bucket_confidence: 0.64,
            observed_amount: 125.32,
            observational_monthly_estimate: 127.16,
            observed_frequency_label: "Observed only",
            duplicate_group_candidates: [],
            merge_candidate_confidence: 0,
          },
          {
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
          },
        ],
        totals: {
          monthly_recurring_income: 4000.0,
          observed_irregular_income_total: 0.0,
          monthly_recurring_baseline: 1500.0,
          monthly_variable_discretionary: 210.35,
          observed_variable_discretionary_total: 245.32,
          observed_one_off_exceptional_total: 120.0,
          observed_transfer_total: 0.0,
          monthly_income: 4000.0,
          monthly_expenses: 1710.35,
          monthly_net: 2289.65,
          net_recurring_monthly: 2500.0,
          net_observed_monthly: 2289.65,
        },
        statement_truth: { total_credits: 4000.0, total_debits: 2222.22, net_movement: 1777.78 },
        budget_model: {
          recurring_income_monthly: 4000.0,
          recurring_baseline_monthly: 1500.0,
          variable_discretionary_monthly: 210.35,
          observed_one_off_total: 120.0,
          observed_transfer_total: 0.0,
          irregular_income_total: 0.0,
          core_net: 2500.0,
          observed_net: 2289.65,
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
      },
    });

    cy.intercept("GET", `${apiBase}/api/v1/control-center/budget/imports/${importId}/lines/**/transactions`, {
      statusCode: 200,
      body: { items: [] },
    });
  }

  beforeEach(() => {
    stubShell();
    stubBudgetApis();
  });

  it("renders the budget workspace cleanly on a mobile viewport", () => {
    cy.viewport("iphone-x");
    cy.loginWithLocalToken(localToken);
    cy.waitForBudgetWorkspaceLoaded();

    cy.contains("Monthly budget view").should("be.visible");
    cy.get("[data-cy='budget-summary-card']").should("have.length.at.least", 4);

    cy.contains("Needs Attention").click();
    cy.get("[data-cy='budget-review-card']").first().within(() => {
      cy.contains("OPENAI").should("be.visible");
      cy.contains("button", "Keep in variable spending").should("be.visible");
      cy.contains("button", "Review related transactions").should("be.visible");
    });

    cy.get("[data-cy='budget-section'][data-section-key='variable']").within(() => {
      cy.get("[data-cy='budget-section-toggle']").click();
      cy.get("[data-cy='budget-category-group']").first().click();
      cy.contains("[data-cy='budget-line-row']", "PET").within(() => {
        cy.contains("[data-cy='budget-line-edit-toggle']", "PET").click();
      });
    });
    cy.get("[data-cy='budget-line-editor']").within(() => {
      cy.contains("Save changes").should("be.visible");
      cy.get("[data-cy='budget-line-edit-base-amount']").should("be.visible");
    });

    cy.screenshot("budget-v2-mobile-layout");
  });
});
