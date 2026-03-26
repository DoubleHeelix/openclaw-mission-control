import { describe, expect, it } from "vitest";

import {
  makeId,
  parseState,
  reorderModules,
  slugifyLabel,
  toStateFromBackend,
  type ControlModule,
} from "@/lib/control-center";

describe("control-center helpers", () => {
  it("slugifyLabel normalizes podcast labels", () => {
    expect(slugifyLabel("  Self Confidence & Mindset  ")).toBe("self-confidence-mindset");
  });

  it("reorderModules returns stable sequential ordering", () => {
    const modules: ControlModule[] = [
      {
        id: "podcasts",
        slug: "podcasts",
        title: "Podcasts",
        description: "Audio workflows",
        category: "podcasts",
        enabled: true,
        order: 4,
      },
      {
        id: "finance",
        slug: "finances",
        title: "Finances",
        description: "Finance",
        category: "finance",
        enabled: true,
        order: 1,
      },
    ];

    const reordered = reorderModules(modules);
    expect(reordered.map((m) => m.id)).toEqual(["finance", "podcasts"]);
    expect(reordered.map((m) => m.order)).toEqual([1, 2]);
  });

  it("makeId includes provided prefix", () => {
    const id = makeId("podcast");
    expect(id.startsWith("podcast-")).toBe(true);
  });

  it("preserves cold contact as a persisted network marketing view mode", () => {
    const parsed = parseState(
      JSON.stringify({
        version: 1,
        modules: [],
        networkMarketingViewMode: "cold_contact",
        records: {
          finance: [],
          network_marketing: [],
          newsletters: [],
          podcasts: [],
          custom: {},
        },
      }),
    );

    expect(parsed.networkMarketingViewMode).toBe("cold_contact");
  });

  it("rehydrates dropped_out stages and cold contact metadata from backend records", () => {
    const state = toStateFromBackend(
      [],
      [
        {
          id: "record-1",
          module_id: "network_marketing",
          module_slug: "network-marketing",
          module_category: "network_marketing",
          title: "Probe lead",
          summary: "Saved summary",
          stage: "dropped_out",
          linked_task_id: null,
          updated_at: "2026-03-19T00:00:00.000Z",
          data: {
            kind: "cold_contact",
            next_step: "Follow up next week",
            cold_contact_platform: "instagram",
            cold_contact_score: 87,
            cold_contact_why_fit: "Strong ICP fit",
            cold_contact_why_now: "Recent launch",
            cold_contact_signals: ["signal-a", "signal-b"],
            cold_contact_confidence: "high",
            cold_contact_angle: "creator tooling",
            cold_contact_research: "Recent posts mention outreach",
            follow_up_date: "2026-03-20",
            follow_up_completed: true,
            follow_up_completed_at: "2026-03-19T10:30:00.000Z",
          },
        },
      ],
      "cold_contact",
    );

    expect(state.networkMarketingViewMode).toBe("cold_contact");
    expect(state.records.network_marketing).toHaveLength(1);
    expect(state.records.network_marketing[0]).toMatchObject({
      stage: "dropped_out",
      coldContactPlatform: "instagram",
      coldContactScore: 87,
      coldContactWhyFit: "Strong ICP fit",
      coldContactWhyNow: "Recent launch",
      coldContactSignals: ["signal-a", "signal-b"],
      coldContactConfidence: "high",
      coldContactAngle: "creator tooling",
      coldContactResearch: "Recent posts mention outreach",
      followUpDate: "2026-03-20",
      followUpCompleted: true,
      followUpCompletedAt: "2026-03-19T10:30:00.000Z",
    });
  });

  it("rehydrates quit team members with parent snapshot and update history", () => {
    const state = toStateFromBackend(
      [],
      [
        {
          id: "member-1",
          module_id: "network_marketing",
          module_slug: "network-marketing",
          module_category: "network_marketing",
          title: "Departed member",
          summary: "Wrapped up well",
          stage: "quit",
          linked_task_id: null,
          updated_at: "2026-03-19T11:00:00.000Z",
          data: {
            kind: "team_member",
            parent_member_id: "parent-1",
            quit_parent_member_id: "parent-1",
            quit_parent_member_name: "Lakshan",
            quit_at: "2026-03-19T10:00:00.000Z",
            quit_reason: "Needed to focus on family",
            update_timeline: [
              { at: "2026-03-18T09:00:00.000Z", note: "Weekly check-in" },
              { at: "2026-03-19T10:00:00.000Z", note: "Marked as quit from Lakshan" },
            ],
          },
        },
      ],
      "pipeline",
    );

    expect(state.records.network_marketing[0]).toMatchObject({
      kind: "team_member",
      stage: "quit",
      quitParentMemberId: "parent-1",
      quitParentMemberName: "Lakshan",
      quitAt: "2026-03-19T10:00:00.000Z",
      quitReason: "Needed to focus on family",
      updateTimeline: [
        { at: "2026-03-18T09:00:00.000Z", note: "Weekly check-in" },
        { at: "2026-03-19T10:00:00.000Z", note: "Marked as quit from Lakshan" },
      ],
    });
  });
});
