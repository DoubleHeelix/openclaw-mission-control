"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowRight, Link2, Zap } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export interface RadialTimelineItem {
  id: number;
  title: string;
  date: string;
  content: string;
  icon: React.ElementType;
  relatedIds: number[];
  status: "completed" | "in-progress" | "pending";
  energy: number;
}

export function RadialOrbitalTimeline({
  timelineData,
  className,
}: {
  timelineData: RadialTimelineItem[];
  className?: string;
}) {
  const [expandedItems, setExpandedItems] = useState<Record<number, boolean>>({});
  const [rotationAngle, setRotationAngle] = useState(0);
  const [autoRotate, setAutoRotate] = useState(true);
  const [activeNodeId, setActiveNodeId] = useState<number | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!autoRotate) return undefined;
    const timer = window.setInterval(() => {
      setRotationAngle((prev) => Number(((prev + 0.22) % 360).toFixed(3)));
    }, 50);
    return () => window.clearInterval(timer);
  }, [autoRotate]);

  const nodePositions = useMemo(() => {
    return timelineData.map((item, index) => {
      const angle = ((index / Math.max(timelineData.length, 1)) * 360 + rotationAngle) % 360;
      const radius = 170;
      const rad = (angle * Math.PI) / 180;
      const x = radius * Math.cos(rad);
      const y = radius * Math.sin(rad);
      const opacity = Math.max(0.42, Math.min(1, 0.44 + 0.56 * ((1 + Math.sin(rad)) / 2)));
      const zIndex = Math.round(80 + 30 * Math.cos(rad));
      return { item, x, y, opacity, zIndex };
    });
  }, [rotationAngle, timelineData]);

  const toggleItem = (id: number) => {
    setExpandedItems((prev) => {
      const next: Record<number, boolean> = {};
      const opening = !prev[id];
      if (opening) next[id] = true;
      setActiveNodeId(opening ? id : null);
      setAutoRotate(!opening);
      return next;
    });
  };

  const getStatusStyles = (status: RadialTimelineItem["status"]) => {
    switch (status) {
      case "completed":
        return "border-emerald-200 bg-emerald-100 text-emerald-900";
      case "in-progress":
        return "border-cyan-200 bg-cyan-100 text-cyan-900";
      default:
        return "border-slate-200 bg-slate-100 text-slate-700";
    }
  };

  const activeRelatedIds = activeNodeId
    ? timelineData.find((item) => item.id === activeNodeId)?.relatedIds ?? []
    : [];

  return (
    <div
      ref={containerRef}
      className={[
        "relative overflow-hidden rounded-[2rem] border border-white/50 bg-[radial-gradient(circle_at_center,rgba(34,211,238,0.12),transparent_35%),linear-gradient(180deg,rgba(2,6,23,0.96),rgba(6,23,40,0.98))] p-4 shadow-[0_35px_90px_rgba(2,6,23,0.35)] sm:p-6",
        className ?? "",
      ].join(" ")}
      onClick={(event) => {
        if (event.target === containerRef.current) {
          setExpandedItems({});
          setActiveNodeId(null);
          setAutoRotate(true);
        }
      }}
    >
      <div className="relative flex min-h-[520px] items-center justify-center overflow-hidden">
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_center,rgba(34,211,238,0.22),transparent_42%)]" />
        <div className="absolute h-24 w-24 rounded-full bg-[linear-gradient(135deg,#22d3ee_0%,#14b8a6_100%)] blur-2xl" />
        <div className="absolute h-24 w-24 rounded-full border border-white/20" />
        <div className="absolute h-[21rem] w-[21rem] rounded-full border border-white/10 sm:h-[24rem] sm:w-[24rem]" />

        {nodePositions.map(({ item, x, y, opacity, zIndex }) => {
          const Icon = item.icon;
          const isExpanded = Boolean(expandedItems[item.id]);
          const isRelated = activeRelatedIds.includes(item.id);

          return (
            <div
              key={item.id}
              className="absolute transition-all duration-700"
              style={{
                transform: `translate(${x}px, ${y}px)`,
                opacity: isExpanded ? 1 : opacity,
                zIndex: isExpanded ? 200 : zIndex,
              }}
            >
              <button
                type="button"
                onClick={(event) => {
                  event.stopPropagation();
                  toggleItem(item.id);
                }}
                className={[
                  "group relative flex h-12 w-12 items-center justify-center rounded-full border-2 transition duration-300",
                  isExpanded
                    ? "scale-125 border-cyan-200 bg-white text-slate-950 shadow-[0_0_40px_rgba(34,211,238,0.35)]"
                    : isRelated
                      ? "border-cyan-200 bg-cyan-200/20 text-white"
                      : "border-white/30 bg-slate-900/90 text-white",
                ].join(" ")}
              >
                <span className="absolute inset-[-10px] rounded-full bg-cyan-300/15 blur-xl" />
                <Icon className="relative h-4 w-4" />
              </button>
              <div
                className={[
                  "absolute left-1/2 top-14 -translate-x-1/2 whitespace-nowrap text-xs font-semibold tracking-[0.18em] transition duration-300",
                  isExpanded ? "scale-110 text-white" : "text-white/70",
                ].join(" ")}
              >
                {item.title}
              </div>

              {isExpanded ? (
                <Card className="absolute left-1/2 top-20 w-72 -translate-x-1/2 border-white/15 bg-slate-950/92 text-white shadow-[0_30px_80px_rgba(34,211,238,0.14)] backdrop-blur-xl">
                  <CardHeader className="pb-3">
                    <div className="flex items-center justify-between gap-3">
                      <Badge className={getStatusStyles(item.status)}>
                        {item.status === "completed"
                          ? "Visible"
                          : item.status === "in-progress"
                            ? "Primary"
                            : "Hidden"}
                      </Badge>
                      <span className="text-xs uppercase tracking-[0.16em] text-white/45">
                        {item.date}
                      </span>
                    </div>
                    <h3 className="mt-3 text-base font-semibold">{item.title}</h3>
                  </CardHeader>
                  <CardContent className="space-y-4 text-sm text-white/78">
                    <p>{item.content}</p>
                    <div>
                      <div className="mb-1 flex items-center justify-between text-[11px] uppercase tracking-[0.2em] text-white/55">
                        <span className="flex items-center gap-1">
                          <Zap className="h-3 w-3" />
                          Signal
                        </span>
                        <span>{item.energy}%</span>
                      </div>
                      <div className="h-1.5 overflow-hidden rounded-full bg-white/10">
                        <div
                          className="h-full rounded-full bg-[linear-gradient(90deg,#22d3ee_0%,#14b8a6_100%)]"
                          style={{ width: `${item.energy}%` }}
                        />
                      </div>
                    </div>
                    {item.relatedIds.length ? (
                      <div className="space-y-2 border-t border-white/10 pt-3">
                        <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-white/55">
                          <Link2 className="h-3 w-3" />
                          Connected sections
                        </div>
                        <div className="flex flex-wrap gap-2">
                          {item.relatedIds.map((relatedId) => {
                            const relatedItem = timelineData.find((candidate) => candidate.id === relatedId);
                            if (!relatedItem) return null;
                            return (
                              <Button
                                key={relatedId}
                                variant="outline"
                                size="sm"
                                className="h-7 rounded-full border-white/15 bg-white/5 px-2 text-[11px] text-white hover:bg-white/10"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  toggleItem(relatedId);
                                }}
                              >
                                {relatedItem.title}
                                <ArrowRight className="ml-1 h-3 w-3" />
                              </Button>
                            );
                          })}
                        </div>
                      </div>
                    ) : null}
                  </CardContent>
                </Card>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}
