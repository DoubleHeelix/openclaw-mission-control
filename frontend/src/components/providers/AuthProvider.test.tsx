import React from "react";
import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";

import { AuthProvider } from "./AuthProvider";

vi.mock("@clerk/nextjs", () => ({
  ClerkProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

vi.mock("@/auth/clerkKey", () => ({
  isLikelyValidClerkPublishableKey: () => false,
}));

describe("AuthProvider local auth", () => {
  beforeEach(() => {
    process.env.NEXT_PUBLIC_AUTH_MODE = "local";
    window.sessionStorage.clear();
  });

  it("does not render children before mount when local auth mode is active", () => {
    render(
      <AuthProvider>
        <div>Secure app</div>
      </AuthProvider>,
    );

    expect(screen.queryByText("Secure app")).not.toBeInTheDocument();
  });

  it("renders children after mount when a local token is present", async () => {
    window.sessionStorage.setItem("mc_local_auth_token", "x".repeat(60));

    render(
      <AuthProvider>
        <div>Secure app</div>
      </AuthProvider>,
    );

    expect(await screen.findByText("Secure app")).toBeInTheDocument();
  });
});
