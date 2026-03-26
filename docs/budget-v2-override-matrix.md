# Budget V2 Override Matrix

## Single actions

| Context | Action | Final bucket | Section | Net effect |
| --- | --- | --- | --- | --- |
| Expense | Mark recurring | `recurring_baseline` | Recurring Baseline Expenses | Included in Core and Observed net |
| Expense | Move to baseline | `recurring_baseline` | Recurring Baseline Expenses | Included in Core and Observed net |
| Expense | Keep discretionary | `variable_discretionary` | Variable / Observed Spending | Excluded from Core net, included in Observed net |
| Expense | Mark one-off | `one_off_exceptional` | One-off / Irregular Expenses | Excluded from both nets, shown as one-off impact |
| Income | Confirm payroll | `income_recurring` | Income | Included in Core and Observed net |
| Income | Mark recurring | `income_recurring` | Income | Included in Core and Observed net |
| Income | Mark one-off | `income_irregular` | Income | Excluded from both nets |
| Transfer-like | No lifestyle bucket actions | `transfer_money_movement` | Transfers / Money Movement | Excluded from all nets |

## Combination rules

| Sequence | Final rule |
| --- | --- |
| Mark recurring -> Mark one-off | Last write wins; final bucket `one_off_exceptional` or `income_irregular` |
| Mark one-off -> Mark recurring | Last write wins; final bucket recurring |
| Keep discretionary -> Mark one-off | Last write wins; final bucket one-off |
| Mark recurring -> Mark monthly | Final bucket recurring, cadence monthly |
| Mark recurring -> Mark irregular | Final bucket unchanged, cadence irregular, recurring monthly totals zeroed |
| Move to baseline -> Keep discretionary | Last write wins; final bucket `variable_discretionary` |
| Confirm payroll -> Mark one-off | Last write wins; final bucket `income_irregular` |

## Review resolution defaults

- `likely_payroll_candidate` clears when the final bucket is `income_recurring`
- cadence ambiguity clears when cadence is explicitly set or the final bucket is one-off / irregular
- `unknown_merchant` is not a blocking Needs Attention reason
- parser anomaly and leakage remain blocking review reasons
