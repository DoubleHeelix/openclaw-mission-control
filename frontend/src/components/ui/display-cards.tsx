"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { ArrowUpRight, Sparkles } from "lucide-react";

import { cn } from "@/lib/utils";

export interface DisplayCardProps {
  className?: string;
  icon?: ReactNode;
  title: string;
  description: string;
  date: string;
  href?: string;
  ctaLabel?: string;
  iconClassName?: string;
  titleClassName?: string;
}

function DisplayCard({
  className,
  icon = <Sparkles className="size-4 text-cyan-100" />,
  title,
  description,
  date,
  href,
  ctaLabel = "Open",
  iconClassName,
  titleClassName,
}: DisplayCardProps) {
  const content = (
    <div
      className={cn(
        "group relative flex h-44 w-full max-w-[24rem] select-none flex-col justify-between overflow-hidden rounded-[1.7rem] border border-white/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(233,249,252,0.92))] px-5 py-5 text-slate-900 shadow-[0_24px_55px_rgba(16,78,94,0.14)] backdrop-blur-xl transition duration-700 after:absolute after:-right-4 after:top-[-6%] after:h-[112%] after:w-[18rem] after:bg-gradient-to-l after:from-white/80 after:to-transparent after:content-[''] hover:-translate-y-1.5 hover:border-cyan-300/90 hover:shadow-[0_32px_70px_rgba(14,116,144,0.18)]",
        className,
      )}
    >
      <div className="relative z-10 flex items-start justify-between gap-3">
        <div className="flex items-center gap-3">
        <span
          className={cn(
            "inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-cyan-200/70 bg-[linear-gradient(135deg,#0f172a_0%,#0e7490_55%,#14b8a6_100%)] text-white shadow-[0_16px_28px_rgba(8,145,178,0.28)]",
            iconClassName,
          )}
        >
          {icon}
        </span>
          <div>
            <p className={cn("text-lg font-semibold tracking-tight", titleClassName)}>
              {title}
            </p>
            <p className="mt-1 text-[11px] font-semibold uppercase tracking-[0.2em] text-cyan-900/60">
              {date}
            </p>
          </div>
        </div>
        {href ? (
          <span className="inline-flex h-9 w-9 items-center justify-center rounded-2xl border border-cyan-200/70 bg-white/80 text-cyan-900 transition group-hover:border-cyan-300 group-hover:bg-cyan-50">
            <ArrowUpRight className="h-4 w-4" />
          </span>
        ) : null}
      </div>
      <p className="relative z-10 text-sm leading-6 text-slate-600">
        {description}
      </p>
      <div className="relative z-10 flex items-center justify-between gap-3">
        <p className="text-xs font-medium uppercase tracking-[0.18em] text-cyan-800/70">
          {ctaLabel}
        </p>
        {href ? (
          <p className="text-xs font-semibold text-slate-700 transition group-hover:text-cyan-900">
            Enter module
          </p>
        ) : null}
      </div>
    </div>
  );

  if (href) {
    return (
      <Link href={href} className="block w-full max-w-[24rem]">
        {content}
      </Link>
    );
  }

  return content;
}

interface DisplayCardsProps {
  cards: DisplayCardProps[];
  className?: string;
}

export default function DisplayCards({ cards, className }: DisplayCardsProps) {
  return (
    <div
      className={cn(
        "grid place-items-center gap-4 md:[grid-template-areas:'stack'] md:gap-0",
        className,
      )}
    >
      {cards.map((cardProps, index) => (
        <DisplayCard
          key={`${cardProps.title}-${index}`}
          {...cardProps}
          className={cn(
            "animate-lift-in md:[grid-area:stack]",
            index === 0 &&
              "md:hover:-translate-y-8 md:before:absolute md:before:left-0 md:before:top-0 md:before:h-full md:before:w-full md:before:rounded-[1.7rem] md:before:bg-white/35 md:before:transition-opacity md:hover:before:opacity-0",
            index === 1 &&
              "md:translate-x-12 md:translate-y-10 md:hover:-translate-y-1 md:before:absolute md:before:left-0 md:before:top-0 md:before:h-full md:before:w-full md:before:rounded-[1.7rem] md:before:bg-white/28 md:before:transition-opacity md:hover:before:opacity-0",
            index >= 2 && "md:translate-x-24 md:translate-y-20 md:hover:translate-y-10",
            cardProps.className,
          )}
        />
      ))}
    </div>
  );
}
