"use client";

import { useEffect } from "react";
import type { ReactNode } from "react";
import { usePathname, useRouter } from "next/navigation";
import { Menu } from "lucide-react";

import { SignedIn, useAuth } from "@/auth/clerk";

import { ApiError } from "@/api/mutator";
import {
  type getMeApiV1UsersMeGetResponse,
  useGetMeApiV1UsersMeGet,
} from "@/api/generated/users/users";
import { BrandMark } from "@/components/atoms/BrandMark";
import { OrgSwitcher } from "@/components/organisms/OrgSwitcher";
import { UserMenu } from "@/components/organisms/UserMenu";
import { isOnboardingComplete } from "@/lib/onboarding";

export function DashboardShell({ children }: { children: ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const { isSignedIn } = useAuth();
  const isOnboardingPath = pathname === "/onboarding";

  const meQuery = useGetMeApiV1UsersMeGet<
    getMeApiV1UsersMeGetResponse,
    ApiError
  >({
    query: {
      enabled: Boolean(isSignedIn) && !isOnboardingPath,
      retry: false,
      refetchOnMount: "always",
    },
  });
  const profile = meQuery.data?.status === 200 ? meQuery.data.data : null;
  const displayName = profile?.name ?? profile?.preferred_name ?? "Operator";
  const displayEmail = profile?.email ?? "";

  useEffect(() => {
    if (!isSignedIn || isOnboardingPath) return;
    if (!profile) return;
    if (!isOnboardingComplete(profile)) {
      router.replace("/onboarding");
    }
  }, [isOnboardingPath, isSignedIn, profile, router]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const handleStorage = (event: StorageEvent) => {
      if (event.key !== "openclaw_org_switch" || !event.newValue) return;
      window.location.reload();
    };

    window.addEventListener("storage", handleStorage);

    let channel: BroadcastChannel | null = null;
    if ("BroadcastChannel" in window) {
      channel = new BroadcastChannel("org-switch");
      channel.onmessage = () => {
        window.location.reload();
      };
    }

    return () => {
      window.removeEventListener("storage", handleStorage);
      channel?.close();
    };
  }, []);

  return (
    <div className="surface-shell-ambient min-h-screen bg-app text-strong">
      <header className="surface-glass animate-fade-in sticky top-0 z-40 border-b border-[color:var(--border)]">
        <div className="grid grid-cols-[auto_1fr_auto] items-center gap-3 px-4 py-3 sm:px-6 lg:grid-cols-[260px_1fr_auto] lg:gap-0 lg:px-0">
          <div className="flex items-center gap-3 lg:px-6">
            <SignedIn>
              <button
                type="button"
                className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-[color:var(--border)] bg-white/55 text-[color:var(--text-muted)] transition hover:border-[color:var(--accent)] hover:bg-white/80 hover:text-[color:var(--accent)] lg:hidden"
                aria-label="Open navigation"
                onClick={() => {
                  window.dispatchEvent(
                    new CustomEvent("dashboard-sidebar:toggle"),
                  );
                }}
              >
                <Menu className="h-5 w-5" />
              </button>
            </SignedIn>
            <BrandMark />
          </div>
          <SignedIn>
            <div className="flex min-w-0 items-center">
              <div className="min-w-0 max-w-[220px]">
                <OrgSwitcher />
              </div>
            </div>
          </SignedIn>
          <SignedIn>
            <div className="flex items-center gap-3 lg:px-6">
              <div className="hidden text-right lg:block">
                <p className="text-sm font-semibold text-strong">
                  {displayName}
                </p>
                <p className="text-xs text-muted">Operator</p>
              </div>
              <UserMenu displayName={displayName} displayEmail={displayEmail} />
            </div>
          </SignedIn>
        </div>
      </header>
      <div className="surface-shell relative grid min-h-[calc(100vh-64px)] grid-cols-1 lg:grid-cols-[260px_1fr]">
        {children}
      </div>
    </div>
  );
}
