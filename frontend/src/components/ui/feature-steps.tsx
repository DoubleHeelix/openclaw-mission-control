"use client";

import React, { useEffect, useMemo, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";

import { cn } from "@/lib/utils";

export interface FeatureStepItem {
  step: string;
  title?: string;
  content: string;
  image: string;
}

interface FeatureStepsProps {
  features: FeatureStepItem[];
  className?: string;
  title?: string;
  autoPlay?: boolean;
  autoPlayInterval?: number;
  activeStep?: number;
  imageHeight?: string;
}

export function FeatureSteps({
  features,
  className,
  title = "How it works",
  autoPlay = false,
  autoPlayInterval = 3200,
  activeStep,
  imageHeight = "h-[260px] md:h-[320px] lg:h-[360px]",
}: FeatureStepsProps) {
  const safeActiveStep = useMemo(() => {
    if (!features.length) return 0;
    if (typeof activeStep !== "number" || Number.isNaN(activeStep)) return 0;
    return Math.min(Math.max(activeStep, 0), features.length - 1);
  }, [activeStep, features.length]);
  const [currentFeature, setCurrentFeature] = useState(safeActiveStep);
  const [progress, setProgress] = useState(0);
  const displayedFeature = typeof activeStep === "number" ? safeActiveStep : currentFeature;

  useEffect(() => {
    if (!autoPlay || features.length <= 1 || typeof activeStep === "number") return;

    const timer = window.setInterval(() => {
      setProgress((prev) => {
        const next = prev + 100 / (autoPlayInterval / 100);
        if (next >= 100) {
          setCurrentFeature((current) => (current + 1) % features.length);
          return 0;
        }
        return next;
      });
    }, 100);

    return () => window.clearInterval(timer);
  }, [activeStep, autoPlay, autoPlayInterval, features.length]);

  if (features.length === 0) return null;

  return (
    <div
      className={cn(
        "overflow-hidden rounded-[2rem] border border-white/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.92),rgba(229,249,252,0.9))] p-5 shadow-[0_30px_80px_rgba(15,118,110,0.12)] backdrop-blur-xl md:p-8",
        className,
      )}
    >
      <div className="mx-auto w-full max-w-7xl">
        <h2 className="font-heading text-2xl font-semibold tracking-tight text-slate-950 md:text-4xl">
          {title}
        </h2>
        <div className="mt-6 flex flex-col gap-6 lg:grid lg:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)] lg:items-center lg:gap-10">
          <div className="order-2 space-y-5 lg:order-1">
            {features.map((feature, index) => {
              const isActive = index === displayedFeature;
              const isComplete = index < displayedFeature;
              return (
                <motion.div
                  key={`${feature.step}-${index}`}
                  className={cn(
                    "rounded-[1.4rem] border p-4 transition duration-300 md:p-5",
                    index === displayedFeature
                      ? "border-cyan-300/80 bg-white/90 shadow-[0_20px_55px_rgba(8,145,178,0.12)]"
                      : "border-white/50 bg-white/55",
                  )}
                  initial={{ opacity: 0.35 }}
                  animate={{ opacity: index === displayedFeature ? 1 : 0.55 }}
                  transition={{ duration: 0.35 }}
                >
                  <div className="flex items-start gap-4">
                    <motion.div
                      className={cn(
                        "flex h-10 w-10 shrink-0 items-center justify-center rounded-2xl border-2 text-sm font-semibold",
                        index === displayedFeature
                          ? "border-cyan-400 bg-[linear-gradient(135deg,#083344_0%,#0891b2_55%,#14b8a6_100%)] text-white"
                          : isComplete
                            ? "border-emerald-300 bg-emerald-100 text-emerald-800"
                            : "border-slate-200 bg-white text-slate-500",
                      )}
                      animate={isActive ? { scale: [1, 1.04, 1] } : { scale: 1 }}
                      transition={{
                        duration: 1.8,
                        repeat: index === displayedFeature ? Number.POSITIVE_INFINITY : 0,
                      }}
                    >
                      {isComplete ? "✓" : index + 1}
                    </motion.div>
                    <div className="min-w-0 flex-1">
                      <p className="text-xs font-semibold uppercase tracking-[0.24em] text-cyan-800/70">
                        {feature.step}
                      </p>
                      <h3 className="mt-2 text-lg font-semibold text-slate-950 md:text-xl">
                        {feature.title || feature.step}
                      </h3>
                      <p className="mt-2 text-sm leading-6 text-slate-600 md:text-base">
                        {feature.content}
                      </p>
                      {autoPlay && index === displayedFeature ? (
                        <div className="mt-4 h-1.5 overflow-hidden rounded-full bg-cyan-100">
                          <div
                            className="h-full rounded-full bg-[linear-gradient(90deg,#0891b2_0%,#0ea5e9_48%,#14b8a6_100%)] transition-[width] duration-100"
                            style={{ width: `${progress}%` }}
                          />
                        </div>
                      ) : null}
                    </div>
                  </div>
                </motion.div>
              );
            })}
          </div>

          <div className={cn("order-1 overflow-hidden rounded-[1.7rem] border border-white/60 bg-slate-950/90 shadow-[0_25px_70px_rgba(2,6,23,0.35)]", imageHeight)}>
            <AnimatePresence mode="wait">
              <motion.div
                key={displayedFeature}
                className="relative h-full w-full"
                initial={{ opacity: 0, y: 40, rotateX: -10 }}
                animate={{ opacity: 1, y: 0, rotateX: 0 }}
                exit={{ opacity: 0, y: -40, rotateX: 10 }}
                transition={{ duration: 0.45, ease: "easeInOut" }}
              >
                <img
                  src={features[displayedFeature]?.image}
                  alt={features[displayedFeature]?.title || features[displayedFeature]?.step}
                  className="h-full w-full object-cover"
                />
                <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(2,6,23,0.05),rgba(2,6,23,0.68))]" />
                <div className="absolute inset-x-0 bottom-0 p-5 text-white md:p-6">
                  <p className="text-xs font-semibold uppercase tracking-[0.26em] text-cyan-200/85">
                    {features[displayedFeature]?.step}
                  </p>
                  <h3 className="mt-2 text-xl font-semibold md:text-2xl">
                    {features[displayedFeature]?.title || features[displayedFeature]?.step}
                  </h3>
                  <p className="mt-2 max-w-xl text-sm leading-6 text-white/80 md:text-base">
                    {features[displayedFeature]?.content}
                  </p>
                </div>
              </motion.div>
            </AnimatePresence>
          </div>
        </div>
      </div>
    </div>
  );
}
