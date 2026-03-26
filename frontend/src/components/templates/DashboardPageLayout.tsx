"use client";

import type { ReactNode, Ref } from "react";
import { useEffect, useState } from "react";

import { SignedIn, SignedOut } from "@/auth/clerk";

import { AdminOnlyNotice } from "@/components/auth/AdminOnlyNotice";
import { SignedOutPanel } from "@/components/auth/SignedOutPanel";
import { DashboardSidebar } from "@/components/organisms/DashboardSidebar";
import { cn } from "@/lib/utils";

import { DashboardShell } from "./DashboardShell";

type SignedOutConfig = {
  message: string;
  forceRedirectUrl: string;
  signUpForceRedirectUrl?: string;
  mode?: "modal" | "redirect";
  buttonLabel?: string;
  buttonTestId?: string;
};

type DashboardPageLayoutProps = {
  signedOut: SignedOutConfig;
  title: ReactNode;
  description?: ReactNode;
  headerActions?: ReactNode;
  children: ReactNode;
  isAdmin?: boolean;
  adminOnlyMessage?: string;
  stickyHeader?: boolean;
  mainClassName?: string;
  headerClassName?: string;
  contentClassName?: string;
  mainRef?: Ref<HTMLElement>;
};

export function DashboardPageLayout({
  signedOut,
  title,
  description,
  headerActions,
  children,
  isAdmin,
  adminOnlyMessage,
  stickyHeader = false,
  mainClassName,
  headerClassName,
  contentClassName,
  mainRef,
}: DashboardPageLayoutProps) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => {
    setMounted(true);
  }, []);
  const showAdminOnlyNotice =
    typeof isAdmin === "boolean" && Boolean(adminOnlyMessage) && !isAdmin;

  return (
    <DashboardShell>
      {!mounted ? null : (
        <>
      <SignedOut>
        <SignedOutPanel
          message={signedOut.message}
          forceRedirectUrl={signedOut.forceRedirectUrl}
          signUpForceRedirectUrl={signedOut.signUpForceRedirectUrl}
          mode={signedOut.mode}
          buttonLabel={signedOut.buttonLabel}
          buttonTestId={signedOut.buttonTestId}
        />
      </SignedOut>
      <SignedIn>
        <DashboardSidebar />
        <main
          ref={mainRef}
          className={cn(
            "surface-shell animate-fade-in flex-1 overflow-y-auto",
            mainClassName,
          )}
        >
          <div
            className={cn(
              "surface-glass-strong motion-panel animate-lift-in border-b border-[color:var(--border)]",
              stickyHeader && "sticky top-0 z-30",
              headerClassName,
            )}
          >
            <div className="px-4 py-5 sm:px-6 lg:px-8 lg:py-6">
              {headerActions ? (
                <div className="flex flex-wrap items-center justify-between gap-4">
                  <div className="animate-stagger-1 animate-lift-in">
                    <h1 className="font-heading text-2xl font-semibold tracking-tight text-strong">
                      {title}
                    </h1>
                    {description ? (
                      <p className="mt-1 text-sm text-muted">
                        {description}
                      </p>
                    ) : null}
                  </div>
                  <div className="animate-stagger-2 animate-lift-in">
                    {headerActions}
                  </div>
                </div>
              ) : (
                <div className="animate-stagger-1 animate-lift-in">
                  <h1 className="font-heading text-2xl font-semibold tracking-tight text-strong">
                    {title}
                  </h1>
                  {description ? (
                    <p className="mt-1 text-sm text-muted">{description}</p>
                  ) : null}
                </div>
              )}
            </div>
          </div>

          <div
            className={cn(
              "animate-stagger-2 animate-lift-in p-4 sm:p-6 lg:p-8",
              contentClassName,
            )}
          >
            {showAdminOnlyNotice ? (
              <AdminOnlyNotice message={adminOnlyMessage ?? ""} />
            ) : (
              children
            )}
          </div>
        </main>
      </SignedIn>
        </>
      )}
    </DashboardShell>
  );
}
