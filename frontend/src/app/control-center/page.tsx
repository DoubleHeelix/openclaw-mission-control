"use client";

export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  Blocks,
  BriefcaseBusiness,
  CalendarRange,
  FileText,
  Mic,
  UsersRound,
  WalletCards,
  Wrench,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { DashboardPageLayout } from "@/components/templates/DashboardPageLayout";
import DisplayCards from "@/components/ui/display-cards";
import { BackgroundPaths } from "@/components/ui/background-paths";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { buttonVariants } from "@/components/ui/button";
import { Spotlight } from "@/components/ui/spotlight";
import { SplineScene } from "@/components/ui/spline-scene";
import { useControlCenterState } from "@/lib/control-center";

const iconByCategory: Record<string, LucideIcon> = {
  finance: BriefcaseBusiness,
  network_marketing: UsersRound,
  newsletters: FileText,
  podcasts: Mic,
  custom: Blocks,
  paperclip: Blocks,
};

export default function ControlCenterHomePage() {
  const { enabledModules, ready, state } = useControlCenterState();
  const previewCards = [
    {
      icon: <CalendarRange className="size-4 text-white" />,
      title: "Events",
      description: `${(state.records.custom.events ?? []).length || 0} live event records ready for scanning, review, and follow-up.`,
      date: "Event engine",
      href: "/control-center/events",
      ctaLabel: "Open events",
      iconClassName: "border-cyan-200/60",
      titleClassName: "text-cyan-950",
    },
    {
      icon: <UsersRound className="size-4 text-white" />,
      title: "Network Marketing",
      description: `${state.records.network_marketing.length} contacts and follow-up moments flowing through the pipeline.`,
      date: "Relationship system",
      href: "/control-center/network-marketing",
      ctaLabel: "Open network marketing",
      iconClassName: "border-cyan-200/60",
      titleClassName: "text-cyan-950",
    },
    {
      icon: <WalletCards className="size-4 text-white" />,
      title: "Budget",
      description: `${state.records.finance.length} finance records tracked across imports, rules, and reconciliations.`,
      date: "Money ops",
      href: "/control-center/budget",
      ctaLabel: "Open budget",
      iconClassName: "border-cyan-200/60",
      titleClassName: "text-cyan-950",
    },
  ];

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
      <div className="space-y-8">
        <Card className="relative overflow-hidden border border-white/60 bg-[linear-gradient(135deg,rgba(2,6,23,0.95),rgba(6,78,99,0.92)_52%,rgba(20,184,166,0.82)_100%)] text-white shadow-[0_30px_90px_rgba(8,145,178,0.2)]">
          <Spotlight className="-left-28 -top-32" fill="#67e8f9" />
          <CardHeader>
            <div className="grid gap-8 lg:grid-cols-[minmax(0,1fr)_minmax(320px,420px)] lg:items-center">
              <div className="relative z-10 space-y-4">
                <p className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-200/85">
                  Custom Control Center
                </p>
                <div>
                  <h2 className="font-heading text-3xl font-semibold tracking-tight sm:text-4xl">
                    One cockpit for events, money, outreach, podcasts, and custom operations.
                  </h2>
                  <p className="mt-3 max-w-2xl text-sm leading-7 text-cyan-50/80 sm:text-base">
                    Mission Control is already wired for live modules, editable navigation, and workflow automation. This landing surface now acts like the polished front door instead of just a list of links.
                  </p>
                </div>
                <div className="grid gap-3 sm:grid-cols-2 xl:max-w-2xl">
                  <div className="rounded-[1.4rem] border border-white/12 bg-white/8 px-4 py-4 backdrop-blur-md">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-cyan-200/70">
                      Primary flow
                    </p>
                    <p className="mt-2 text-lg font-semibold text-white">Events pipeline</p>
                    <p className="mt-2 text-sm leading-6 text-cyan-50/78">
                      Scan event sources, curate weekly opportunities, and move the best ones into action.
                    </p>
                  </div>
                  <div className="rounded-[1.4rem] border border-white/12 bg-white/8 px-4 py-4 backdrop-blur-md">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-cyan-200/70">
                      Design layer
                    </p>
                    <p className="mt-2 text-lg font-semibold text-white">Builder control</p>
                    <p className="mt-2 text-sm leading-6 text-cyan-50/78">
                      Shape how your sections appear and reorder the modules that matter most day to day.
                    </p>
                  </div>
                </div>
                <div className="flex flex-wrap items-center gap-3 pt-1 text-sm">
                  <Link
                    href="/control-center/events"
                    className={buttonVariants({
                      variant: "secondary",
                    })}
                  >
                    Open Events
                  </Link>
                  <Link
                    href="/control-center/builder"
                    className={buttonVariants({
                      variant: "ghost",
                    })}
                  >
                    Open Builder
                  </Link>
                </div>
              </div>
              <div className="relative hidden min-h-[320px] overflow-hidden rounded-[1.8rem] border border-white/10 bg-black/15 lg:block">
                <SplineScene
                  scene="https://prod.spline.design/kZDDjO5HuC9GJUM2/scene.splinecode"
                  className="h-full w-full"
                />
              </div>
            </div>
          </CardHeader>
        </Card>

        <div className="grid gap-6 xl:grid-cols-[minmax(0,1.05fr)_minmax(320px,0.95fr)]">
          <div className="overflow-hidden rounded-[2rem] border border-white/60 bg-white/72 px-4 py-6 shadow-[0_26px_70px_rgba(14,116,144,0.1)] backdrop-blur-xl sm:px-8">
            <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h2 className="text-lg font-semibold text-slate-950">Mission lanes</h2>
                <p className="mt-1 text-sm leading-6 text-slate-600">
                  The primary modules are now the main way into Mission Control, not a secondary preview.
                </p>
              </div>
            </div>
            <DisplayCards cards={previewCards} className="md:min-h-[300px]" />
          </div>

          <BackgroundPaths
            eyebrow="Operational Preview"
            title="Your modules are live and ready"
            description="A quick glance at the highest-signal areas of Mission Control before you drill into the deeper workspaces."
            className="px-5 py-8 sm:px-8 sm:py-10"
          />
        </div>

        {!ready ? (
          <p className="text-sm text-slate-500">Loading modules...</p>
        ) : (
          <div className="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)]">
            <Card className="border border-white/60 bg-white/76 shadow-[0_26px_70px_rgba(14,116,144,0.1)] backdrop-blur-xl">
              <CardHeader>
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h2 className="text-lg font-semibold text-slate-950">Module deck</h2>
                    <p className="mt-1 text-sm leading-6 text-slate-600">
                      Every enabled section stays accessible here, but the layout now supports the richer component-led entry above.
                    </p>
                  </div>
                  <Link href="/control-center/builder" className={buttonVariants({ variant: "secondary" })}>
                    Reorder sections
                  </Link>
                </div>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-2">
                {enabledModules.map((module) => {
                  const Icon = iconByCategory[module.category] ?? Blocks;
                  const count =
                    module.id === "finance"
                      ? state.records.finance.length
                      : module.id === "network_marketing"
                        ? state.records.network_marketing.length
                        : module.id === "newsletters"
                          ? state.records.newsletters.length
                          : (state.records.custom[module.id] ?? []).length;
                  return (
                    <Link
                      key={module.id}
                      href={`/control-center/${module.slug}`}
                      className="group rounded-[1.6rem] border border-slate-200/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(238,249,251,0.92))] p-5 shadow-[0_18px_45px_rgba(8,33,51,0.06)] transition duration-300 hover:-translate-y-1 hover:border-cyan-300/80 hover:shadow-[0_28px_60px_rgba(8,145,178,0.14)]"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <span className="inline-flex h-11 w-11 items-center justify-center rounded-2xl bg-[linear-gradient(135deg,#0f172a_0%,#0e7490_55%,#14b8a6_100%)] text-white shadow-[0_16px_28px_rgba(8,145,178,0.2)]">
                          <Icon className="h-4 w-4" />
                        </span>
                        <span className="rounded-full border border-cyan-100 bg-cyan-50 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-cyan-900/70">
                          {count} records
                        </span>
                      </div>
                      <h3 className="mt-5 text-lg font-semibold text-slate-950 transition group-hover:text-cyan-950">
                        {module.title}
                      </h3>
                      <p className="mt-2 text-sm leading-6 text-slate-600">{module.description}</p>
                      <p className="mt-4 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-800/70">
                        Open module
                      </p>
                    </Link>
                  );
                })}
              </CardContent>
            </Card>

            <Card className="border border-white/60 bg-white/76 shadow-[0_26px_70px_rgba(14,116,144,0.1)] backdrop-blur-xl">
              <CardHeader>
                <h2 className="text-lg font-semibold text-slate-950">System state</h2>
                <p className="mt-1 text-sm leading-6 text-slate-600">
                  The foundation that keeps this workspace modular, editable, and ready for action.
                </p>
              </CardHeader>
              <CardContent className="space-y-3">
                {[
                  "Dynamic navigation shell is active",
                  "Domain workspaces are live across events, money, network, newsletters, and podcasts",
                  "Builder changes persist immediately",
                  "Records are task-ready across modules",
                  "Design model is set up for further extension",
                ].map((item) => (
                  <div
                    key={item}
                    className="rounded-[1.2rem] border border-slate-200/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(242,249,250,0.92))] px-4 py-3 text-sm leading-6 text-slate-700"
                  >
                    {item}
                  </div>
                ))}
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </DashboardPageLayout>
  );
}
