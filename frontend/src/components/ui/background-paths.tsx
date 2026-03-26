"use client";

import { motion } from "framer-motion";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

function FloatingPaths({ position }: { position: number }) {
  const paths = Array.from({ length: 30 }, (_, i) => ({
    id: i,
    d: `M-${380 - i * 5 * position} -${189 + i * 6}C-${
      380 - i * 5 * position
    } -${189 + i * 6} -${312 - i * 5 * position} ${216 - i * 6} ${
      152 - i * 5 * position
    } ${343 - i * 6}C${616 - i * 5 * position} ${470 - i * 6} ${
      684 - i * 5 * position
    } ${875 - i * 6} ${684 - i * 5 * position} ${875 - i * 6}`,
    width: 0.55 + i * 0.03,
  }));

  return (
    <div className="pointer-events-none absolute inset-0">
      <svg className="h-full w-full text-cyan-950/35" viewBox="0 0 696 316" fill="none">
        <title>Mission Control Background Paths</title>
        {paths.map((path) => (
          <motion.path
            key={path.id}
            d={path.d}
            stroke="currentColor"
            strokeWidth={path.width}
            strokeOpacity={0.1 + path.id * 0.018}
            initial={{ pathLength: 0.24, opacity: 0.28 }}
            animate={{
              pathLength: 1,
              opacity: [0.18, 0.42, 0.18],
              pathOffset: [0, 1, 0],
            }}
            transition={{
              duration: 18 + path.id * 0.3,
              repeat: Number.POSITIVE_INFINITY,
              ease: "linear",
            }}
          />
        ))}
      </svg>
    </div>
  );
}

export function BackgroundPaths({
  title,
  eyebrow,
  description,
  ctaLabel,
  className,
}: {
  title: string;
  eyebrow?: string;
  description?: string;
  ctaLabel?: string;
  className?: string;
}) {
  const words = title.split(" ");

  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-[2rem] border border-white/50 bg-[radial-gradient(circle_at_top,rgba(34,211,238,0.18),transparent_42%),linear-gradient(180deg,rgba(255,255,255,0.92),rgba(232,248,252,0.88))] px-6 py-12 shadow-[0_28px_70px_rgba(15,118,110,0.12)] backdrop-blur-xl sm:px-8 lg:px-12",
        className,
      )}
    >
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_bottom_right,rgba(8,145,178,0.14),transparent_35%)]" />
      <FloatingPaths position={1} />
      <FloatingPaths position={-1} />

      <div className="relative z-10 mx-auto max-w-4xl text-center">
        {eyebrow ? (
          <p className="mb-4 text-xs font-semibold uppercase tracking-[0.3em] text-cyan-800/75">
            {eyebrow}
          </p>
        ) : null}
        <motion.h2
          initial={{ opacity: 0, y: 18 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, ease: "easeOut" }}
          className="font-heading text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl lg:text-6xl"
        >
          {words.map((word, wordIndex) => (
            <span key={wordIndex} className="mr-3 inline-block last:mr-0">
              {word.split("").map((letter, letterIndex) => (
                <motion.span
                  key={`${wordIndex}-${letterIndex}`}
                  initial={{ y: 34, opacity: 0 }}
                  animate={{ y: 0, opacity: 1 }}
                  transition={{
                    delay: wordIndex * 0.08 + letterIndex * 0.02,
                    type: "spring",
                    stiffness: 140,
                    damping: 18,
                  }}
                  className="inline-block bg-[linear-gradient(135deg,#020617_0%,#0f766e_50%,#0891b2_100%)] bg-clip-text text-transparent"
                >
                  {letter}
                </motion.span>
              ))}
            </span>
          ))}
        </motion.h2>
        {description ? (
          <motion.p
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.75, delay: 0.22 }}
            className="mx-auto mt-5 max-w-2xl text-sm leading-7 text-slate-600 sm:text-base"
          >
            {description}
          </motion.p>
        ) : null}
        {ctaLabel ? (
          <motion.div
            initial={{ opacity: 0, y: 14 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.75, delay: 0.3 }}
            className="mt-8"
          >
            <Button
              variant="ghost"
              className="rounded-[1.15rem] border border-cyan-200/80 bg-white/85 px-6 py-6 text-base font-semibold text-slate-900 shadow-[0_20px_40px_rgba(8,145,178,0.12)] hover:bg-white"
            >
              {ctaLabel}
            </Button>
          </motion.div>
        ) : null}
      </div>
    </div>
  );
}
