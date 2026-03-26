"use client";

export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  Blocks,
  BriefcaseBusiness,
  CalendarRange,
  FileText,
  Mic,
  Settings2,
  UsersRound,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { DashboardPageLayout } from "@/components/templates/DashboardPageLayout";
import { BackgroundPaths } from "@/components/ui/background-paths";
import { Button, buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { RadialOrbitalTimeline } from "@/components/ui/radial-orbital-timeline";
import { reorderModules, useControlCenterState } from "@/lib/control-center";

const iconByCategory: Record<string, LucideIcon> = {
  finance: BriefcaseBusiness,
  network_marketing: UsersRound,
  newsletters: FileText,
  podcasts: Mic,
  custom: Blocks,
  paperclip: Settings2,
};

export default function ControlCenterBuilderPage() {
  const { ready, state, setState } = useControlCenterState();
  const orderedModules = state.modules.slice().sort((a, b) => a.order - b.order);
  const timelineData = orderedModules.map((module, index) => {
    const Icon = module.slug === "events" ? CalendarRange : iconByCategory[module.category] ?? Blocks;
    return {
      id: index + 1,
      title: module.title,
      date: `Slot ${index + 1}`,
      content: module.enabled
        ? `${module.title} is active in the current Mission Control navigation.`
        : `${module.title} is currently hidden, but can be re-enabled at any time.`,
      icon: Icon,
      relatedIds: [orderedModules[index - 1], orderedModules[index + 1]]
        .filter(Boolean)
        .map((candidate) => orderedModules.findIndex((entry) => entry.id === candidate?.id) + 1),
      status:
        !module.enabled ? "pending" : index < 2 ? "in-progress" : "completed",
      energy: Math.max(30, 100 - index * 9),
    } as const;
  });

  const moveModule = async (moduleId: string, direction: -1 | 1) => {
    const currentIndex = orderedModules.findIndex((module) => module.id === moduleId);
    const nextIndex = currentIndex + direction;
    if (currentIndex < 0 || nextIndex < 0 || nextIndex >= orderedModules.length) return;
    const nextModules = orderedModules.slice();
    [nextModules[currentIndex], nextModules[nextIndex]] = [
      nextModules[nextIndex],
      nextModules[currentIndex],
    ];
    await setState({
      ...state,
      modules: reorderModules(nextModules),
    });
  };

  const toggleModule = async (moduleId: string) => {
    await setState({
      ...state,
      modules: reorderModules(
        state.modules.map((module) =>
          module.id === moduleId ? { ...module, enabled: !module.enabled } : module,
        ),
      ),
    });
  };

  return (
    <DashboardPageLayout
      signedOut={{
        message: "Sign in to edit your control center layout.",
        forceRedirectUrl: "/control-center/builder",
        signUpForceRedirectUrl: "/control-center/builder",
      }}
      title="Control Center Builder"
      description="Reorder your sections and hide anything you do not want in the navigation."
      headerActions={
        <Link
          href="/control-center"
          className={buttonVariants({ variant: "secondary" })}
        >
          Back to Control Center
        </Link>
      }
    >
      <div className="space-y-8">
        <BackgroundPaths
          eyebrow="Builder View"
          title="Shape the flow of your Mission Control"
          description="The orbital map and the control deck now work together so structure and editing feel like one system."
          className="px-5 py-8 sm:px-8 sm:py-10"
        />

        {ready ? (
          <div className="grid gap-6 xl:grid-cols-[minmax(0,1.12fr)_minmax(340px,0.88fr)]">
            <Card className="border border-white/60 bg-transparent shadow-none">
              <CardHeader className="px-0 pt-0">
                <div>
                  <h2 className="text-lg font-semibold text-slate-950">Orbital section map</h2>
                  <p className="mt-1 text-sm leading-6 text-slate-600">
                    Primary sections glow stronger, hidden sections recede, and neighbor links help you visualize the overall flow before editing.
                  </p>
                </div>
              </CardHeader>
              <CardContent className="px-0 pb-0">
                <RadialOrbitalTimeline timelineData={timelineData} />
              </CardContent>
            </Card>

            <Card className="border border-white/60 bg-white/78 shadow-[0_26px_70px_rgba(14,116,144,0.1)] backdrop-blur-xl">
              <CardHeader>
                <h2 className="text-lg font-semibold text-slate-950">Builder controls</h2>
                <p className="mt-1 text-sm leading-6 text-slate-600">
                  The order below updates the live navigation immediately, while visibility controls let you trim the stack without deleting sections.
                </p>
              </CardHeader>
              <CardContent className="space-y-3">
                {orderedModules.map((module, index) => (
                  <div
                    key={module.id}
                    className="rounded-[1.5rem] border border-slate-200/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(239,249,251,0.92))] p-4 shadow-[0_18px_42px_rgba(8,33,51,0.05)]"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <p className="text-base font-semibold text-slate-950">{module.title}</p>
                          <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-slate-500">
                            {module.slug}
                          </span>
                          <span
                            className={`rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] ${
                              module.enabled
                                ? "border border-emerald-200 bg-emerald-50 text-emerald-900"
                                : "border border-slate-200 bg-slate-100 text-slate-600"
                            }`}
                          >
                            {module.enabled ? "Visible" : "Hidden"}
                          </span>
                        </div>
                        <p className="mt-2 text-sm leading-6 text-slate-600">{module.description}</p>
                      </div>
                      <div className="rounded-full border border-cyan-100 bg-cyan-50 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-900/70">
                        Slot {index + 1}
                      </div>
                    </div>

                    <div className="mt-4 flex flex-wrap items-center gap-2">
                      <Button
                        type="button"
                        variant="secondary"
                        className="h-9 rounded-full px-4 text-xs"
                        onClick={() => void moveModule(module.id, -1)}
                        disabled={index === 0}
                      >
                        Move up
                      </Button>
                      <Button
                        type="button"
                        variant="secondary"
                        className="h-9 rounded-full px-4 text-xs"
                        onClick={() => void moveModule(module.id, 1)}
                        disabled={index === orderedModules.length - 1}
                      >
                        Move down
                      </Button>
                      <Button
                        type="button"
                        variant="secondary"
                        className="h-9 rounded-full px-4 text-xs"
                        onClick={() => void toggleModule(module.id)}
                      >
                        {module.enabled ? "Hide section" : "Show section"}
                      </Button>
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>
          </div>
        ) : (
          <Card className="border border-white/60 bg-white/78 shadow-[0_26px_70px_rgba(14,116,144,0.1)] backdrop-blur-xl">
            <CardContent className="py-10">
              <p className="text-sm text-slate-500">Loading sections...</p>
            </CardContent>
          </Card>
        )}
      </div>
    </DashboardPageLayout>
  );
}
