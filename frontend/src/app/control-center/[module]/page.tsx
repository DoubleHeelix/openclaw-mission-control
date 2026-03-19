"use client";

export const dynamic = "force-dynamic";

import Link from "next/link";
import { useParams } from "next/navigation";

import { BudgetWorkspaceV2 } from "@/components/control-center/BudgetWorkspaceV2";
import { DashboardPageLayout } from "@/components/templates/DashboardPageLayout";
import { ModuleWorkspace } from "@/components/control-center/ModuleWorkspace";
import { buttonVariants } from "@/components/ui/button";
import { useControlCenterState } from "@/lib/control-center";

export default function ControlCenterModulePage() {
  const params = useParams<{ module: string }>();
  const slug = params?.module ?? "";
  const {
    ready,
    state,
    addRecord,
    deleteRecord,
    promoteRecordToTask,
    updateRecordStage,
    updateRecord,
    ingestPodcastAudio,
    transcribePodcastRecord,
    summarizePodcastRecord,
    extractPodcastActions,
    classifyPodcastRecord,
    runPodcastPipeline,
    syncPodcastDriveNow,
    getPodcastRecordView,
    getPodcastRecordAudioBlob,
    importBudgetStatement,
    scanEventsWeek,
    addEventRecordToCalendar,
    runColdContactPipeline,
    getColdContactQueue,
    getFollowUpTasks,
    recomputeFollowUpTasks,
    refreshRecords,
    networkMarketingViewMode,
    setNetworkMarketingViewMode,
  } = useControlCenterState();

  const selectedModule = state.modules.find((entry) => entry.slug === slug);

  return (
    <DashboardPageLayout
      signedOut={{
        message: "Sign in to open this control center module.",
        forceRedirectUrl: `/control-center/${slug}`,
        signUpForceRedirectUrl: `/control-center/${slug}`,
      }}
      title={selectedModule?.title ?? "Control Center"}
      description={
        selectedModule?.description ??
        "Module not found. You can create it from the control center builder."
      }
      headerActions={
        <Link
          href="/control-center/builder"
          className={buttonVariants({ variant: "secondary" })}
        >
          Edit sections
        </Link>
      }
    >
      {!ready ? (
        <p className="text-sm text-slate-500">Loading module...</p>
      ) : !selectedModule ? (
        <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
          This module does not exist. Add it from the builder first.
        </div>
      ) : selectedModule.slug === "budget" ? (
        <BudgetWorkspaceV2 />
      ) : (
        <ModuleWorkspace
          module={selectedModule}
          state={state}
          onCreateRecord={addRecord}
          onDeleteRecord={deleteRecord}
          onPromoteRecord={promoteRecordToTask}
          onUpdateStage={updateRecordStage}
          onUpdateRecord={updateRecord}
          onPodcastIngest={ingestPodcastAudio}
          onPodcastTranscribe={transcribePodcastRecord}
          onPodcastSummarize={summarizePodcastRecord}
          onPodcastExtractActions={extractPodcastActions}
          onPodcastClassify={classifyPodcastRecord}
          onPodcastRunPipeline={runPodcastPipeline}
          onPodcastDriveSyncNow={syncPodcastDriveNow}
          onPodcastView={getPodcastRecordView}
          onPodcastAudio={getPodcastRecordAudioBlob}
          onBudgetImport={importBudgetStatement}
          onEventsScanWeek={scanEventsWeek}
          onEventAddToCalendar={addEventRecordToCalendar}
          onRunColdContactPipeline={runColdContactPipeline}
          onGetColdContactQueue={getColdContactQueue}
          onGetFollowUpTasks={getFollowUpTasks}
          onRecomputeFollowUpTasks={recomputeFollowUpTasks}
          onRefreshRecords={refreshRecords}
          networkMarketingViewMode={networkMarketingViewMode}
          onNetworkMarketingViewModeChange={setNetworkMarketingViewMode}
        />
      )}
    </DashboardPageLayout>
  );
}
