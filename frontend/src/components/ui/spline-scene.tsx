"use client";

import { Suspense, lazy } from "react";

const Spline = lazy(() => import("@splinetool/react-spline"));

interface SplineSceneProps {
  scene: string;
  className?: string;
}

export function SplineScene({ scene, className }: SplineSceneProps) {
  return (
    <Suspense
      fallback={
        <div className="flex h-full w-full items-center justify-center">
          <span className="inline-flex h-12 w-12 animate-spin rounded-full border-2 border-cyan-200 border-t-cyan-700" />
        </div>
      }
    >
      <Spline scene={scene} className={className} />
    </Suspense>
  );
}
