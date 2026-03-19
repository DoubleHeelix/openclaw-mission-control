import { describe, expect, it } from "vitest";

import { makeId, reorderModules, slugifyLabel, type ControlModule } from "@/lib/control-center";

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
});
