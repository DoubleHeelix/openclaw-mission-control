"use client";

import { ClerkProvider } from "@clerk/nextjs";
import { useEffect, useState, type ReactNode } from "react";

import { isLikelyValidClerkPublishableKey } from "@/auth/clerkKey";
import {
  clearLocalAuthToken,
  getLocalAuthToken,
  isLocalAuthMode,
} from "@/auth/localAuth";
import { LocalAuthLogin } from "@/components/organisms/LocalAuthLogin";

export function AuthProvider({ children }: { children: ReactNode }) {
  const localMode = isLocalAuthMode();
  const [mounted, setMounted] = useState(false);
  const [hasLocalToken, setHasLocalToken] = useState(false);
  const bypassLocalAuthForBudgetE2E = typeof window !== "undefined" && window.location.pathname === "/control-center/budget-e2e";

  useEffect(() => {
    if (!localMode) {
      clearLocalAuthToken();
    }
  }, [localMode]);

  useEffect(() => {
    setMounted(true);
    if (localMode) {
      setHasLocalToken(Boolean(getLocalAuthToken()));
    }
  }, [localMode]);

  if (localMode) {
    if (!mounted) {
      return null;
    }
    if (bypassLocalAuthForBudgetE2E) {
      return <>{children}</>;
    }
    if (!hasLocalToken) {
      return <LocalAuthLogin />;
    }
    return <>{children}</>;
  }

  const publishableKey = process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY;
  const afterSignOutUrl =
    process.env.NEXT_PUBLIC_CLERK_AFTER_SIGN_OUT_URL ?? "/";

  if (!isLikelyValidClerkPublishableKey(publishableKey)) {
    return <>{children}</>;
  }

  return (
    <ClerkProvider
      publishableKey={publishableKey}
      afterSignOutUrl={afterSignOutUrl}
    >
      {children}
    </ClerkProvider>
  );
}
