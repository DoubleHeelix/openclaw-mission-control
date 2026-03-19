"use client";

export const dynamic = "force-dynamic";

import Link from "next/link";
import { Blocks, BriefcaseBusiness, FileText, Mic, UsersRound, Wrench } from "lucide-react";

import { DashboardPageLayout } from "@/components/templates/DashboardPageLayout";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { buttonVariants } from "@/components/ui/button";
import { useControlCenterState } from "@/lib/control-center";

const iconByCategory = {
  finance: BriefcaseBusiness,
  network_marketing: UsersRound,
  newsletters: FileText,
  podcasts: Mic,
  custom: Blocks,
};

export default function ControlCenterHomePage() {
  const { enabledModules, ready, state } = useControlCenterState();

  return (
    <DashboardPageLayout
      signedOut={{
        message: "Sign in to open your custom mission control.",
        forceRedirectUrl: "/control-center",
        signUpForceRedirectUrl: "/control-center",
      }}
      title="Custom Mission Control"
      description="Your modular operations workspace for finances, network marketing, newsletters, and custom sections."
      headerActions={
        <Link
          href="/control-center/builder"
          className={buttonVariants({ variant: "secondary" })}
        >
          <Wrench className="h-4 w-4" />
          Design Builder
        </Link>
      }
    >
      <div className="space-y-6">
        <Card className="border border-slate-200 bg-white shadow-sm">
          <CardHeader>
            <h2 className="text-base font-semibold text-slate-900">Phase Status</h2>
            <p className="mt-1 text-sm text-slate-500">
              Phase 1-5 foundation is active: configurable nav, domain modules, editable section builder, task-ready records, and backend persistence.
            </p>
          </CardHeader>
          <CardContent className="grid gap-3 text-sm text-slate-700 md:grid-cols-2">
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
              Phase 1: Dynamic navigation shell
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
              Phase 2: Domain workspaces (finances/network/newsletters)
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
              Phase 3: UI builder for custom sections
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
              Phase 4: Action/task-ready records layer
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 md:col-span-2">
              Phase 5: Persisted operator design model for future extension
            </div>
          </CardContent>
        </Card>

        {!ready ? (
          <p className="text-sm text-slate-500">Loading modules...</p>
        ) : (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {enabledModules.map((module) => {
              const Icon = iconByCategory[module.category];
              const count =
                module.id === "finance"
                  ? state.records.finance.length
                  : module.id === "network_marketing"
                    ? state.records.network_marketing.length
                    : module.id === "newsletters"
                      ? state.records.newsletters.length
                      : (state.records.custom[module.id] ?? []).length;
              return (
                <Card
                  key={module.id}
                  className="border border-slate-200 bg-white shadow-sm"
                >
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <Icon className="h-4 w-4 text-blue-700" />
                      <h3 className="font-semibold text-slate-900">{module.title}</h3>
                    </div>
                    <p className="mt-1 text-sm text-slate-500">{module.description}</p>
                  </CardHeader>
                  <CardContent className="flex items-center justify-between">
                    <p className="text-sm text-slate-600">
                      {count} record{count === 1 ? "" : "s"}
                    </p>
                    <Link
                      href={`/control-center/${module.slug}`}
                      className={buttonVariants({ size: "sm" })}
                    >
                      Open
                    </Link>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        )}
      </div>
    </DashboardPageLayout>
  );
}
