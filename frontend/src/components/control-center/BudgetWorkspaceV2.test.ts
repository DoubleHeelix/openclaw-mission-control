import { describe, expect, it } from "vitest";

import { availableReviewActions, displayBucketTotalsFromLines, groupLinesByCategory, initialBudgetAmountDraft, needsAttentionItemCount, overviewTotalsFromSummary, preserveReviewCardOrder, safeSectionForLine, statementPeriodTotalsFromLines } from "./BudgetWorkspaceV2";

describe("availableReviewActions", () => {
  it("shows expense actions for expense review rows", () => {
    const actions = availableReviewActions({
      category: "General / Home",
      bucket_assignment: "variable_discretionary",
      movement_type: "expense",
      likely_payroll_candidate: false,
      group_key: "General / Home|Grocery Shopping|ALDI",
      review_reasons: ["weak_cadence_evidence"],
    });

    expect(actions).toContain("mark_recurring");
    expect(actions).toContain("keep_discretionary");
    expect(actions).toContain("mark_one_off");
    expect(actions).toContain("set_cadence");
  });

  it("hides expense-only actions for income rows", () => {
    const actions = availableReviewActions({
      category: "Income",
      bucket_assignment: "income_irregular",
      movement_type: "income",
      likely_payroll_candidate: true,
      group_key: "Income|Salary|ACCENTURE",
      review_reasons: [],
    });

    expect(actions).toContain("mark_recurring");
    expect(actions).toContain("mark_one_off");
    expect(actions).not.toContain("keep_discretionary");
  });

  it("hides lifestyle actions for money movement rows", () => {
    const actions = availableReviewActions({
      category: "Transfer / Money Movement",
      bucket_assignment: "transfer_money_movement",
      movement_type: "internal_transfer",
      likely_payroll_candidate: false,
      group_key: "Transfer|Internal|SAVINGS",
      review_reasons: [],
    });

    expect(actions).toEqual(["view_details"]);
  });

  it("shows cadence actions for money movement rows when cadence is the unresolved issue", () => {
    const actions = availableReviewActions({
      category: "Transfer / Money Movement",
      bucket_assignment: "transfer_money_movement",
      movement_type: "internal_transfer",
      likely_payroll_candidate: false,
      group_key: "Transfer|Internal|WISE",
      review_reasons: ["cadence_ambiguous_material"],
    });

    expect(actions).toContain("set_cadence");
    expect(actions).toContain("view_details");
    expect(actions).not.toContain("mark_one_off");
    expect(actions).not.toContain("mark_recurring");
    expect(actions).not.toContain("keep_discretionary");
  });

  it("shows cadence actions for money movement rows with insufficient observations even if the cadence reason label is the only strong signal", () => {
    const actions = availableReviewActions({
      category: "Transfer / Money Movement",
      bucket_assignment: "transfer_money_movement",
      movement_type: "internal_transfer",
      likely_payroll_candidate: false,
      group_key: "Transfer|Internal|WISE",
      review_reasons: ["Material cadence ambiguity"],
      // TypeScript test input only uses the fields in the Pick at compile time,
      // but runtime logic also tolerates the cadence_reason signal.
    } as never);

    expect(actions).toContain("set_cadence");
  });
});

describe("safeSectionForLine", () => {
  it("honors an explicit recurring baseline bucket even when cadence is irregular", () => {
    expect(
      safeSectionForLine({
        id: "rb1",
        group_key: "General / Home|Grocery Shopping|WOOLWORTHS",
        group_label: "WOOLWORTHS",
        line_type: "expense",
        category: "General / Home",
        subcategory: "Grocery Shopping",
        inferred_cadence: "irregular",
        cadence_confidence: 0.52,
        cadence_reason: "intervals_inconsistent",
        observed_only: false,
        bucket_assignment: "recurring_baseline",
        bucket_suggestion: "recurring_baseline",
        base_amount: 23.65,
        base_period: "monthly",
        authoritative_field: "base_amount",
        source_amount: 23.65,
        source_period: "monthly",
        observed_window_total: 141.9,
        normalized_weekly: 5.46,
        normalized_fortnightly: 10.92,
        normalized_monthly: 23.65,
        normalized_yearly: 283.8,
        impact_on_baseline: "included",
        included: true,
        transaction_count: 6,
        confidence_label: "Needs review",
        explanation: "",
        review_reasons: ["cadence_ambiguous_material", "low_confidence"],
        modeling_status: "user_forced_recurring",
        recurrence_state: "recurring_candidate",
        is_modeled: true,
        modeled_by_default: false,
        merchant_confidence: 0.92,
        bucket_confidence: 0.92,
        observed_amount: 23.65,
        observational_monthly_estimate: 141.9,
        observed_frequency_label: "",
        duplicate_group_candidates: [],
        merge_candidate_confidence: 0,
      }),
    ).toBe("recurring_baseline");
  });

  it("honors an explicit observed discretionary bucket even when cadence is irregular", () => {
    expect(
      safeSectionForLine({
        id: "vd1",
        group_key: "General / Home|Grocery Shopping|WOOLWORTHS",
        group_label: "WOOLWORTHS",
        line_type: "expense",
        category: "General / Home",
        subcategory: "Grocery Shopping",
        inferred_cadence: "irregular",
        cadence_confidence: 0.52,
        cadence_reason: "intervals_inconsistent",
        observed_only: true,
        bucket_assignment: "variable_discretionary",
        bucket_suggestion: "variable_discretionary",
        base_amount: 23.65,
        base_period: "monthly",
        authoritative_field: "base_amount",
        source_amount: 23.65,
        source_period: "monthly",
        observed_window_total: 141.9,
        normalized_weekly: 0,
        normalized_fortnightly: 0,
        normalized_monthly: 0,
        normalized_yearly: 0,
        impact_on_baseline: "included",
        included: true,
        transaction_count: 6,
        confidence_label: "Needs review",
        explanation: "",
        review_reasons: ["cadence_ambiguous_material", "low_confidence"],
        modeling_status: "observational_only",
        recurrence_state: "discretionary_candidate",
        is_modeled: false,
        modeled_by_default: false,
        merchant_confidence: 0.92,
        bucket_confidence: 0.92,
        observed_amount: 141.9,
        observational_monthly_estimate: 141.9,
        observed_frequency_label: "",
        duplicate_group_candidates: [],
        merge_candidate_confidence: 0,
      }),
    ).toBe("variable_observed");
  });

  it("routes one-off bucket lines to one-off section", () => {
    expect(
      safeSectionForLine({
        id: "1",
        group_key: "Entertainment|Eating out / takeaways|DINNER",
        group_label: "Dinner",
        line_type: "expense",
        category: "Entertainment",
        subcategory: "Eating out / takeaways",
        inferred_cadence: "irregular",
        cadence_confidence: 0.1,
        cadence_reason: "single_occurrence",
        observed_only: true,
        bucket_assignment: "one_off_exceptional",
        bucket_suggestion: "suggested_discretionary",
        base_amount: 50,
        base_period: "monthly",
        authoritative_field: "base_amount",
        source_amount: 50,
        source_period: "monthly",
        observed_window_total: 50,
        normalized_weekly: 0,
        normalized_fortnightly: 0,
        normalized_monthly: 0,
        normalized_yearly: 0,
        impact_on_baseline: "included",
        included: true,
        transaction_count: 1,
        confidence_label: "Needs review",
        explanation: "",
        review_reasons: ["likely_one_off"],
        modeling_status: "observational_only",
        recurrence_state: "one_off_candidate",
        is_modeled: false,
        modeled_by_default: false,
        merchant_confidence: 0.4,
        bucket_confidence: 0.4,
        observed_amount: 50,
        observed_frequency_label: "1 occurrence",
        duplicate_group_candidates: [],
        merge_candidate_confidence: 0,
      }),
    ).toBe("one_off");
  });

  it("honors canonical final buckets for fees and uncategorized even when the legacy bucket is variable", () => {
    const feeSection = safeSectionForLine({
      id: "f1",
      group_key: "variable_spending|Fees|Bank Fee|MONTHLY FEE",
      group_label: "MONTHLY FEE",
      final_bucket: "fees",
      line_type: "expense",
      category: "Discretionary",
      subcategory: "Presents",
      inferred_cadence: "irregular",
      cadence_confidence: 0.2,
      cadence_reason: "single_occurrence",
      observed_only: true,
      bucket_assignment: "variable_discretionary",
      bucket_suggestion: "variable_discretionary",
      base_amount: 0,
      base_period: "monthly",
      authoritative_field: "base_amount",
      source_amount: 0,
      source_period: "monthly",
      observed_window_total: 14.86,
      normalized_weekly: 0,
      normalized_fortnightly: 0,
      normalized_monthly: 0,
      normalized_yearly: 0,
      impact_on_baseline: "included",
      included: true,
      transaction_count: 1,
      confidence_label: "Medium confidence",
      explanation: "",
      review_reasons: [],
      modeling_status: "observational_only",
      recurrence_state: "non_recurring",
      is_modeled: false,
      modeled_by_default: false,
      merchant_confidence: 0.8,
      bucket_confidence: 0.8,
      observed_amount: 14.86,
      observational_monthly_estimate: 0,
      observed_frequency_label: "",
      duplicate_group_candidates: [],
      merge_candidate_confidence: 0,
    } as never);

    const uncategorizedSection = safeSectionForLine({
      id: "u1",
      group_key: "uncategorized|Misc|Unknown|MYSTERY",
      group_label: "MYSTERY",
      final_bucket: "uncategorized",
      line_type: "expense",
      category: "Discretionary",
      subcategory: "Presents",
      inferred_cadence: "irregular",
      cadence_confidence: 0.1,
      cadence_reason: "unknown",
      observed_only: true,
      bucket_assignment: "variable_discretionary",
      bucket_suggestion: "variable_discretionary",
      base_amount: 0,
      base_period: "monthly",
      authoritative_field: "base_amount",
      source_amount: 0,
      source_period: "monthly",
      observed_window_total: 468.41,
      normalized_weekly: 0,
      normalized_fortnightly: 0,
      normalized_monthly: 0,
      normalized_yearly: 0,
      impact_on_baseline: "included",
      included: true,
      transaction_count: 1,
      confidence_label: "Needs review",
      explanation: "",
      review_reasons: ["large_debit_unclassified"],
      modeling_status: "observational_only",
      recurrence_state: "non_recurring",
      is_modeled: false,
      modeled_by_default: false,
      merchant_confidence: 0.3,
      bucket_confidence: 0.3,
      observed_amount: 468.41,
      observational_monthly_estimate: 0,
      observed_frequency_label: "",
      duplicate_group_candidates: [],
      merge_candidate_confidence: 0,
    } as never);

    expect(feeSection).toBe("fees");
    expect(uncategorizedSection).toBe("uncategorized");
  });
});

describe("overviewTotalsFromSummary", () => {
  it("uses backend budget-model totals as the single source of truth", () => {
    const totals = overviewTotalsFromSummary({
      import_id: "123",
      status: "completed",
      transaction_count: 4,
      budget_model: {
        recurring_income_monthly: 5000,
        recurring_baseline_monthly: 2100,
        variable_discretionary_monthly: 480.55,
        observed_one_off_total: 120,
        observed_transfer_total: 300,
        irregular_income_total: 0,
        core_net: 2900,
        observed_net: 2419.45,
        modeling_allowed: true,
        modeling_restrictions: [],
      },
      trust: {
        modeling_allowed: true,
        modeling_restrictions: [],
        totals_trust_level: "verified",
      },
    });

    expect(totals.recurringIncome).toBe(5000);
    expect(totals.recurringBaseline).toBe(2100);
    expect(totals.variableMonthly).toBe(480.55);
    expect(totals.coreNet).toBe(2900);
    expect(totals.observedNet).toBe(2419.45);
  });

  it("surfaces trust-gated blocked modeling state", () => {
    const totals = overviewTotalsFromSummary({
      import_id: "123",
      status: "completed",
      transaction_count: 4,
      budget_model: {
        recurring_income_monthly: 5000,
        recurring_baseline_monthly: 2100,
        variable_discretionary_monthly: 480.55,
        observed_one_off_total: 120,
        observed_transfer_total: 300,
        irregular_income_total: 0,
        core_net: 2900,
        observed_net: 2419.45,
        modeling_allowed: false,
        modeling_restrictions: ["Statement range is too short for trusted recurrence modeling."],
      },
      trust: {
        modeling_allowed: false,
        modeling_restrictions: ["Statement range is too short for trusted recurrence modeling."],
        totals_trust_level: "provisional",
      },
    });

    expect(totals.modelingAllowed).toBe(false);
    expect(totals.modelingRestrictions).toEqual(["Statement range is too short for trusted recurrence modeling."]);
    expect(totals.variableMonthly).toBe(480.55);
    expect(totals.observedNet).toBe(2419.45);
  });
});


describe("displayBucketTotalsFromLines", () => {
  it("uses the same visible bucket totals as the section rows", () => {
    const totals = displayBucketTotalsFromLines([
      {
        id: "i1", group_key: "Income|Salary|ACCENTURE", group_label: "ACCENTURE", line_type: "income", category: "Income", subcategory: "Salary", inferred_cadence: "monthly", cadence_confidence: 0.9, cadence_reason: "intervals_match_monthly", observed_only: false, bucket_assignment: "income_recurring", bucket_suggestion: "income_recurring", base_amount: 5000, base_period: "monthly", authoritative_field: "base_amount", source_amount: 5000, source_period: "monthly", observed_window_total: 5000, normalized_weekly: 0, normalized_fortnightly: 0, normalized_monthly: 5000, normalized_yearly: 0, impact_on_baseline: "included", included: true, transaction_count: 1, confidence_label: "High confidence", explanation: "", review_reasons: [], modeling_status: "modeled", recurrence_state: "recurring_candidate", is_modeled: true, modeled_by_default: true, merchant_confidence: 0.9, bucket_confidence: 0.9, observed_amount: 5000, observational_monthly_estimate: null, observed_frequency_label: "", duplicate_group_candidates: [], merge_candidate_confidence: 0,
      },
      {
        id: "v1", group_key: "General / Home|Grocery Shopping|ALDI", group_label: "ALDI", line_type: "expense", category: "General / Home", subcategory: "Grocery Shopping", inferred_cadence: "monthly", cadence_confidence: 0.4, cadence_reason: "weak", observed_only: true, bucket_assignment: "variable_discretionary", bucket_suggestion: "variable_discretionary", base_amount: 0, base_period: "monthly", authoritative_field: "base_amount", source_amount: 0, source_period: "monthly", observed_window_total: 120, normalized_weekly: 0, normalized_fortnightly: 0, normalized_monthly: 0, normalized_yearly: 0, impact_on_baseline: "included", included: true, transaction_count: 3, confidence_label: "Needs review", explanation: "", review_reasons: [], modeling_status: "observational_only", recurrence_state: "discretionary_candidate", is_modeled: false, modeled_by_default: false, merchant_confidence: 0.8, bucket_confidence: 0.6, observed_amount: 120, observational_monthly_estimate: 130, observed_frequency_label: "", duplicate_group_candidates: [], merge_candidate_confidence: 0,
      },
      {
        id: "o1", group_key: "Entertainment|Movies / Activities|JETSTAR", group_label: "JETSTAR", line_type: "expense", category: "Entertainment", subcategory: "Movies / Activities", inferred_cadence: "irregular", cadence_confidence: 0.2, cadence_reason: "single", observed_only: true, bucket_assignment: "one_off_exceptional", bucket_suggestion: "one_off_exceptional", base_amount: 0, base_period: "monthly", authoritative_field: "base_amount", source_amount: 0, source_period: "monthly", observed_window_total: 450, normalized_weekly: 0, normalized_fortnightly: 0, normalized_monthly: 0, normalized_yearly: 0, impact_on_baseline: "included", included: false, transaction_count: 1, confidence_label: "Medium confidence", explanation: "", review_reasons: [], modeling_status: "observational_only", recurrence_state: "one_off_candidate", is_modeled: false, modeled_by_default: false, merchant_confidence: 0.8, bucket_confidence: 0.6, observed_amount: 450, observational_monthly_estimate: 0, observed_frequency_label: "", duplicate_group_candidates: [], merge_candidate_confidence: 0,
      },
    ] as never);

    expect(totals.income).toBe(5000);
    expect(totals.variable).toBe(120);
    expect(totals.oneoff).toBe(450);
  });

  it("uses the saved modeled monthly amount for variable spending when one exists", () => {
    const totals = displayBucketTotalsFromLines([
      {
        id: "v1", group_key: "General / Home|Grocery Shopping|ALDI", group_label: "ALDI", final_bucket: "variable_spending", line_type: "expense", category: "General / Home", subcategory: "Grocery Shopping", inferred_cadence: "weekly", cadence_confidence: 0.98, cadence_reason: "intervals_match_weekly", observed_only: false, bucket_assignment: "variable_discretionary", bucket_suggestion: "variable_discretionary", base_amount: 100, base_period: "weekly", authoritative_field: "base_amount", source_amount: 100, source_period: "weekly", observed_window_total: 304.52, normalized_weekly: 100, normalized_fortnightly: 200, normalized_monthly: 434.86, normalized_yearly: 5218.32, impact_on_baseline: "included", included: true, transaction_count: 5, confidence_label: "High confidence", explanation: "", review_reasons: [], modeling_status: "modeled", recurrence_state: "discretionary_candidate", is_modeled: true, modeled_by_default: false, merchant_confidence: 0.99, bucket_confidence: 0.99, observed_amount: 304.52, observational_monthly_estimate: 308.99, observed_frequency_label: "", duplicate_group_candidates: [], merge_candidate_confidence: 0,
      },
    ] as never);

    expect(totals.variable).toBeCloseTo(434.86, 2);
  });

  it("does not inflate variable spending with fees or uncategorized rows when final_bucket is resolved", () => {
    const totals = displayBucketTotalsFromLines([
      {
        id: "v1", group_key: "General / Home|Grocery Shopping|ALDI", group_label: "ALDI", final_bucket: "variable_spending", line_type: "expense", category: "General / Home", subcategory: "Grocery Shopping", inferred_cadence: "monthly", cadence_confidence: 0.4, cadence_reason: "weak", observed_only: true, bucket_assignment: "variable_discretionary", bucket_suggestion: "variable_discretionary", base_amount: 0, base_period: "monthly", authoritative_field: "base_amount", source_amount: 0, source_period: "monthly", observed_window_total: 120, normalized_weekly: 0, normalized_fortnightly: 0, normalized_monthly: 0, normalized_yearly: 0, impact_on_baseline: "included", included: true, transaction_count: 3, confidence_label: "Needs review", explanation: "", review_reasons: [], modeling_status: "observational_only", recurrence_state: "discretionary_candidate", is_modeled: false, modeled_by_default: false, merchant_confidence: 0.8, bucket_confidence: 0.6, observed_amount: 120, observational_monthly_estimate: 130, observed_frequency_label: "", duplicate_group_candidates: [], merge_candidate_confidence: 0,
      },
      {
        id: "f1", group_key: "variable_spending|Fees|Bank Fee|MONTHLY FEE", group_label: "MONTHLY FEE", final_bucket: "fees", line_type: "expense", category: "Discretionary", subcategory: "Presents", inferred_cadence: "irregular", cadence_confidence: 0.2, cadence_reason: "single", observed_only: true, bucket_assignment: "variable_discretionary", bucket_suggestion: "variable_discretionary", base_amount: 0, base_period: "monthly", authoritative_field: "base_amount", source_amount: 0, source_period: "monthly", observed_window_total: 14.86, normalized_weekly: 0, normalized_fortnightly: 0, normalized_monthly: 0, normalized_yearly: 0, impact_on_baseline: "included", included: true, transaction_count: 1, confidence_label: "Medium confidence", explanation: "", review_reasons: [], modeling_status: "observational_only", recurrence_state: "non_recurring", is_modeled: false, modeled_by_default: false, merchant_confidence: 0.8, bucket_confidence: 0.6, observed_amount: 14.86, observational_monthly_estimate: 0, observed_frequency_label: "", duplicate_group_candidates: [], merge_candidate_confidence: 0,
      },
      {
        id: "u1", group_key: "uncategorized|Misc|Unknown|MYSTERY", group_label: "MYSTERY", final_bucket: "uncategorized", line_type: "expense", category: "Discretionary", subcategory: "Presents", inferred_cadence: "irregular", cadence_confidence: 0.1, cadence_reason: "unknown", observed_only: true, bucket_assignment: "variable_discretionary", bucket_suggestion: "variable_discretionary", base_amount: 0, base_period: "monthly", authoritative_field: "base_amount", source_amount: 0, source_period: "monthly", observed_window_total: 468.41, normalized_weekly: 0, normalized_fortnightly: 0, normalized_monthly: 0, normalized_yearly: 0, impact_on_baseline: "included", included: true, transaction_count: 1, confidence_label: "Needs review", explanation: "", review_reasons: ["large_debit_unclassified"], modeling_status: "observational_only", recurrence_state: "non_recurring", is_modeled: false, modeled_by_default: false, merchant_confidence: 0.3, bucket_confidence: 0.3, observed_amount: 468.41, observational_monthly_estimate: 0, observed_frequency_label: "", duplicate_group_candidates: [], merge_candidate_confidence: 0,
      },
    ] as never);

    expect(totals.variable).toBe(120);
    expect(totals.fees).toBe(14.86);
    expect(totals.uncategorized).toBe(468.41);
  });

  it("keeps statement-period observed totals separate from monthly budget-model totals for long statements", () => {
    const monthlyTotals = overviewTotalsFromSummary({
      import_id: "90d",
      status: "completed",
      transaction_count: 12,
      budget_model: {
        recurring_income_monthly: 5751.08,
        recurring_baseline_monthly: 4101.47,
        variable_discretionary_monthly: 4099.66,
        observed_one_off_total: 1535.85,
        observed_transfer_total: 102,
        irregular_income_total: 51,
        core_net: 1649.61,
        observed_net: -2450.05,
        modeling_allowed: true,
        modeling_restrictions: [],
      },
      trust: {
        modeling_allowed: true,
        modeling_restrictions: [],
        totals_trust_level: "verified",
      },
    });

    const monthlySectionTotals = displayBucketTotalsFromLines([
      {
        id: "income-90d",
        group_key: "Income|Salary|ACCENTURE",
        group_label: "ACCENTURE",
        final_bucket: "income",
        line_type: "income",
        category: "Income",
        subcategory: "Salary",
        inferred_cadence: "monthly",
        cadence_confidence: 0.9,
        cadence_reason: "intervals_match_monthly",
        observed_only: false,
        bucket_assignment: "income_recurring",
        bucket_suggestion: "income_recurring",
        base_amount: 5751.08,
        base_period: "monthly",
        authoritative_field: "base_amount",
        source_amount: 5751.08,
        source_period: "monthly",
        observed_window_total: 17304.25,
        normalized_weekly: 1327.17,
        normalized_fortnightly: 2654.35,
        normalized_monthly: 5751.08,
        normalized_yearly: 69012.96,
        impact_on_baseline: "included",
        included: true,
        transaction_count: 3,
        confidence_label: "High confidence",
        explanation: "",
        review_reasons: [],
        modeling_status: "modeled",
        recurrence_state: "recurring_candidate",
        is_modeled: true,
        modeled_by_default: true,
        merchant_confidence: 0.98,
        bucket_confidence: 0.98,
        observed_amount: 17304.25,
        observational_monthly_estimate: null,
        observed_frequency_label: "",
        duplicate_group_candidates: [],
        merge_candidate_confidence: 0,
      },
      {
        id: "variable-90d",
        group_key: "General / Home|Grocery Shopping|ALDI",
        group_label: "ALDI",
        final_bucket: "variable_spending",
        line_type: "expense",
        category: "General / Home",
        subcategory: "Grocery Shopping",
        inferred_cadence: "weekly",
        cadence_confidence: 0.98,
        cadence_reason: "intervals_match_weekly",
        observed_only: false,
        bucket_assignment: "variable_discretionary",
        bucket_suggestion: "variable_discretionary",
        base_amount: 944.58,
        base_period: "weekly",
        authoritative_field: "base_amount",
        source_amount: 944.58,
        source_period: "weekly",
        observed_window_total: 10920.87,
        normalized_weekly: 944.58,
        normalized_fortnightly: 1889.16,
        normalized_monthly: 4099.66,
        normalized_yearly: 49195.92,
        impact_on_baseline: "included",
        included: true,
        transaction_count: 13,
        confidence_label: "High confidence",
        explanation: "",
        review_reasons: [],
        modeling_status: "modeled",
        recurrence_state: "discretionary_candidate",
        is_modeled: true,
        modeled_by_default: false,
        merchant_confidence: 0.98,
        bucket_confidence: 0.98,
        observed_amount: 10920.87,
        observational_monthly_estimate: 4099.66,
        observed_frequency_label: "",
        duplicate_group_candidates: [],
        merge_candidate_confidence: 0,
      },
    ] as never);

    const observedTotals = statementPeriodTotalsFromLines([
      {
        id: "income-90d",
        group_key: "Income|Salary|ACCENTURE",
        group_label: "ACCENTURE",
        final_bucket: "income",
        line_type: "income",
        category: "Income",
        subcategory: "Salary",
        inferred_cadence: "monthly",
        cadence_confidence: 0.9,
        cadence_reason: "intervals_match_monthly",
        observed_only: false,
        bucket_assignment: "income_recurring",
        bucket_suggestion: "income_recurring",
        base_amount: 5751.08,
        base_period: "monthly",
        authoritative_field: "base_amount",
        source_amount: 5751.08,
        source_period: "monthly",
        observed_window_total: 17304.25,
        normalized_weekly: 1327.17,
        normalized_fortnightly: 2654.35,
        normalized_monthly: 5751.08,
        normalized_yearly: 69012.96,
        impact_on_baseline: "included",
        included: true,
        transaction_count: 3,
        confidence_label: "High confidence",
        explanation: "",
        review_reasons: [],
        modeling_status: "modeled",
        recurrence_state: "recurring_candidate",
        is_modeled: true,
        modeled_by_default: true,
        merchant_confidence: 0.98,
        bucket_confidence: 0.98,
        observed_amount: 17304.25,
        observational_monthly_estimate: null,
        observed_frequency_label: "",
        duplicate_group_candidates: [],
        merge_candidate_confidence: 0,
      },
      {
        id: "variable-90d",
        group_key: "General / Home|Grocery Shopping|ALDI",
        group_label: "ALDI",
        final_bucket: "variable_spending",
        line_type: "expense",
        category: "General / Home",
        subcategory: "Grocery Shopping",
        inferred_cadence: "weekly",
        cadence_confidence: 0.98,
        cadence_reason: "intervals_match_weekly",
        observed_only: false,
        bucket_assignment: "variable_discretionary",
        bucket_suggestion: "variable_discretionary",
        base_amount: 944.58,
        base_period: "weekly",
        authoritative_field: "base_amount",
        source_amount: 944.58,
        source_period: "weekly",
        observed_window_total: 10920.87,
        normalized_weekly: 944.58,
        normalized_fortnightly: 1889.16,
        normalized_monthly: 4099.66,
        normalized_yearly: 49195.92,
        impact_on_baseline: "included",
        included: true,
        transaction_count: 13,
        confidence_label: "High confidence",
        explanation: "",
        review_reasons: [],
        modeling_status: "modeled",
        recurrence_state: "discretionary_candidate",
        is_modeled: true,
        modeled_by_default: false,
        merchant_confidence: 0.98,
        bucket_confidence: 0.98,
        observed_amount: 10920.87,
        observational_monthly_estimate: 4099.66,
        observed_frequency_label: "",
        duplicate_group_candidates: [],
        merge_candidate_confidence: 0,
      },
    ] as never);

    expect(monthlyTotals.recurringIncome).toBe(5751.08);
    expect(observedTotals.income).toBe(17304.25);
    expect(monthlyTotals.variableMonthly).toBe(4099.66);
    expect(monthlySectionTotals.variable).toBe(4099.66);
    expect(observedTotals.variable).toBe(10920.87);
    expect(observedTotals.income).toBeGreaterThan(monthlyTotals.recurringIncome);
    expect(observedTotals.variable).toBeGreaterThan(monthlyTotals.variableMonthly);
  });
});

describe("preserveReviewCardOrder", () => {
  it("keeps an acted-on card in place when its priority drops but it still needs review", () => {
    const previous = [
      { id: "inland", group_key: "g-inland", review_priority: 884 },
      { id: "chatgpt", group_key: "g-chatgpt", review_priority: 634 },
    ] as never;

    const next = [
      { id: "chatgpt", group_key: "g-chatgpt", review_priority: 634 },
      { id: "inland", group_key: "g-inland", review_priority: 634 },
    ] as never;

    const ordered = preserveReviewCardOrder(previous, next);

    expect(ordered.map((item) => item.id)).toEqual(["inland", "chatgpt"]);
  });

  it("appends genuinely new review items after the existing visible order", () => {
    const previous = [
      { id: "inland", group_key: "g-inland", review_priority: 884 },
      { id: "chatgpt", group_key: "g-chatgpt", review_priority: 634 },
    ] as never;

    const next = [
      { id: "new-item", group_key: "g-new", review_priority: 900 },
      { id: "chatgpt", group_key: "g-chatgpt", review_priority: 634 },
      { id: "inland", group_key: "g-inland", review_priority: 634 },
    ] as never;

    const ordered = preserveReviewCardOrder(previous, next);

    expect(ordered.map((item) => item.id)).toEqual(["inland", "chatgpt", "new-item"]);
  });
});

describe("groupLinesByCategory", () => {
  it("groups lines by category while preserving first-seen order", () => {
    const groups = groupLinesByCategory([
      { id: "1", category: "Discretionary" },
      { id: "2", category: "General / Home" },
      { id: "3", category: "Discretionary" },
    ] as never);

    expect(groups.map((group) => group.category)).toEqual(["Discretionary", "General / Home"]);
    expect(groups[0].items.map((item) => item.id)).toEqual(["1", "3"]);
    expect(groups[1].items.map((item) => item.id)).toEqual(["2"]);
  });
});

describe("initialBudgetAmountDraft", () => {
  it("leaves the draft blank when there is no amount", () => {
    expect(initialBudgetAmountDraft({ base_amount: 0 })).toBe("");
  });

  it("keeps a real authoritative amount when one exists", () => {
    expect(initialBudgetAmountDraft({ base_amount: 23.65 })).toBe("23.65");
  });
});


describe("needsAttentionItemCount", () => {
  it("counts review cards and uncategorized line groups together", () => {
    expect(
      needsAttentionItemCount(
        [{ id: "tx-1" } as never, { id: "tx-2" } as never],
        [{ id: "line-1" } as never],
      ),
    ).toBe(3);
  });
});
