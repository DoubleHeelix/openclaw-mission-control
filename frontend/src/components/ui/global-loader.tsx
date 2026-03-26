"use client";

import { useIsFetching, useIsMutating } from "@tanstack/react-query";

export function GlobalLoader() {
  const fetchingCount = useIsFetching({
    predicate: (query) =>
      query.state.fetchStatus === "fetching" && query.state.data === undefined,
  });
  const mutatingCount = useIsMutating();
  const visible = fetchingCount + mutatingCount > 0;

  return (
    <div
      data-cy="global-loader"
      className={`pointer-events-none fixed inset-x-0 top-0 z-[120] h-1 transition-opacity duration-200 ${
        visible ? "opacity-100" : "opacity-0"
      }`}
      aria-hidden={!visible}
      data-state={visible ? "visible" : "hidden"}
      role="status"
    >
      <div className="h-full w-full overflow-hidden bg-[linear-gradient(90deg,rgba(34,211,238,0.12)_0%,rgba(14,165,233,0.2)_50%,rgba(16,185,129,0.12)_100%)]">
        <div className="h-full w-full animate-progress-shimmer bg-[linear-gradient(90deg,transparent_0%,rgba(255,255,255,0.08)_12%,var(--accent)_45%,#10b981_70%,transparent_100%)]" />
      </div>
      <span className="sr-only">Loading</span>
    </div>
  );
}
