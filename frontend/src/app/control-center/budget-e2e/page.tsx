export const dynamic = "force-dynamic";

import { BudgetWorkspaceV2 } from "@/components/control-center/BudgetWorkspaceV2";

export default function BudgetE2EPage() {
  return (
    <main className="min-h-screen bg-slate-50 px-4 py-6 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-7xl" data-cy="budget-e2e-page">
        <BudgetWorkspaceV2 />
      </div>
    </main>
  );
}
