"use client";

import { useState } from "react";
import { ArrowRight, Lock, Radio, ShieldCheck, Sparkles } from "lucide-react";

import { setLocalAuthToken } from "@/auth/localAuth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { getApiBaseUrl } from "@/lib/api-base";

const LOCAL_AUTH_TOKEN_MIN_LENGTH = 50;

async function validateLocalToken(token: string): Promise<string | null> {
  let baseUrl: string;
  try {
    baseUrl = getApiBaseUrl();
  } catch (error) {
    return error instanceof Error ? error.message : "NEXT_PUBLIC_API_URL is invalid.";
  }
  const validationUrl = `${baseUrl}/api/v1/users/me`;

  let response: Response;
  try {
    response = await fetch(validationUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
  } catch (error) {
    const details =
      error instanceof Error
        ? `${error.name}${error.message ? `: ${error.message}` : ""}`
        : "unknown network failure";
    return `Unable to reach backend to validate token at ${validationUrl}. ${details}`;
  }

  if (response.ok) {
    return null;
  }
  if (response.status === 401 || response.status === 403) {
    return "Token is invalid.";
  }
  return `Unable to validate token (HTTP ${response.status}).`;
}

type LocalAuthLoginProps = {
  onAuthenticated?: () => void;
};

const defaultOnAuthenticated = () => window.location.reload();

export function LocalAuthLogin({ onAuthenticated }: LocalAuthLoginProps) {
  const [token, setToken] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isValidating, setIsValidating] = useState(false);

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const cleaned = token.trim();
    if (!cleaned) {
      setError("Bearer token is required.");
      return;
    }
    if (cleaned.length < LOCAL_AUTH_TOKEN_MIN_LENGTH) {
      setError(
        `Bearer token must be at least ${LOCAL_AUTH_TOKEN_MIN_LENGTH} characters.`,
      );
      return;
    }

    setIsValidating(true);
    const validationError = await validateLocalToken(cleaned);
    setIsValidating(false);
    if (validationError) {
      setError(validationError);
      return;
    }

    setLocalAuthToken(cleaned);
    setError(null);
    (onAuthenticated ?? defaultOnAuthenticated)();
  };

  return (
    <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-[radial-gradient(circle_at_top,_rgba(56,189,248,0.18),_transparent_28%),linear-gradient(160deg,#08111f_0%,#0f172a_38%,#132238_100%)] px-3 py-6 text-white sm:px-4 sm:py-10">
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute inset-0 bg-[linear-gradient(rgba(148,163,184,0.07)_1px,transparent_1px),linear-gradient(90deg,rgba(148,163,184,0.07)_1px,transparent_1px)] bg-[size:30px_30px] opacity-25" />
        <div className="absolute -top-28 -left-20 h-72 w-72 rounded-full bg-cyan-400/20 blur-3xl motion-safe:animate-pulse" />
        <div className="absolute right-[-4rem] top-[10%] h-64 w-64 rounded-full bg-emerald-400/10 blur-3xl motion-safe:animate-pulse" />
        <div className="absolute -bottom-20 left-[12%] h-72 w-72 rounded-full bg-blue-500/20 blur-3xl motion-safe:animate-float" />
        <div className="absolute right-[8%] bottom-[8%] h-40 w-40 rounded-full border border-cyan-300/20" />
        <div className="absolute right-[10%] bottom-[10%] h-24 w-24 rounded-full border border-cyan-300/30 motion-safe:animate-ping" />
      </div>

      <div className="relative grid w-full max-w-6xl gap-4 sm:gap-6 lg:grid-cols-[1.1fr_minmax(420px,520px)]">
        <div className="overflow-hidden rounded-[28px] border border-white/10 bg-white/8 p-5 shadow-[0_20px_60px_rgba(8,15,30,0.35)] backdrop-blur sm:p-6 xl:hidden">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-3">
              <div className="inline-flex items-center gap-2 rounded-full border border-cyan-300/20 bg-cyan-300/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-cyan-100">
                <Radio className="h-3.5 w-3.5 motion-safe:animate-pulse" />
                Local mission gateway
              </div>
              <div className="space-y-2">
                <h1 className="font-heading text-3xl font-semibold leading-tight tracking-tight text-white sm:text-4xl">
                  Unlock Mission Control
                </h1>
                <p className="max-w-md text-sm leading-6 text-slate-300">
                  Your Mac mini is running the live node. Paste the local token and
                  you are straight into the control room.
                </p>
              </div>
            </div>
            <div className="mt-1 flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-2xl bg-white/10 text-cyan-100">
              <Sparkles className="h-5 w-5 motion-safe:animate-pulse" />
            </div>
          </div>

          <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-3">
            <div className="rounded-2xl border border-white/10 bg-slate-950/20 p-3">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-cyan-100/80">
                Mode
              </p>
              <p className="mt-1 text-sm font-semibold text-white">Bearer token</p>
            </div>
            <div className="rounded-2xl border border-white/10 bg-slate-950/20 p-3">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-cyan-100/80">
                Runtime
              </p>
              <p className="mt-1 text-sm font-semibold text-white">Mac mini live</p>
            </div>
            <div className="rounded-2xl border border-white/10 bg-slate-950/20 p-3">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-cyan-100/80">
                Tunnel
              </p>
              <p className="mt-1 inline-flex items-center gap-2 text-sm font-semibold text-emerald-200">
                <span className="h-2 w-2 rounded-full bg-emerald-400 motion-safe:animate-pulse" />
                Online
              </p>
            </div>
          </div>
        </div>

        <div className="hidden min-h-[540px] overflow-hidden rounded-[32px] border border-white/10 bg-white/6 p-8 shadow-[0_30px_80px_rgba(8,15,30,0.4)] backdrop-blur xl:flex xl:flex-col xl:justify-between">
          <div className="space-y-6">
            <div className="inline-flex items-center gap-2 rounded-full border border-cyan-300/20 bg-cyan-300/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-cyan-100">
              <Radio className="h-3.5 w-3.5 motion-safe:animate-pulse" />
              Local mission gateway
            </div>
            <div className="space-y-4">
              <h1 className="max-w-xl font-heading text-5xl font-semibold leading-[1.05] tracking-tight text-white">
                Unlock your control room with a little more theatre.
              </h1>
              <p className="max-w-lg text-base leading-7 text-slate-300">
                This Mac mini is running in self-host mode, so the local token is
                your key to the full mission console. Fast to enter, hard to miss,
                and a lot more fun to land on.
              </p>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            {[
              {
                icon: ShieldCheck,
                label: "Private by default",
                copy: "Local token auth keeps access tight while your tunnel stays open.",
              },
              {
                icon: Sparkles,
                label: "Designed for launch",
                copy: "Animated entry, cleaner hierarchy, and a better first impression.",
              },
              {
                icon: Lock,
                label: "Still simple",
                copy: "Paste token, validate once, and drop straight into Mission Control.",
              },
            ].map((item) => {
              const Icon = item.icon;
              return (
                <div
                  key={item.label}
                  className="rounded-2xl border border-white/10 bg-slate-950/20 p-4 backdrop-blur"
                >
                  <div className="mb-3 inline-flex rounded-xl bg-white/10 p-2 text-cyan-100">
                    <Icon className="h-4 w-4" />
                  </div>
                  <h2 className="text-sm font-semibold text-white">
                    {item.label}
                  </h2>
                  <p className="mt-2 text-sm leading-6 text-slate-300">
                    {item.copy}
                  </p>
                </div>
              );
            })}
          </div>
        </div>

        <Card className="relative w-full overflow-hidden rounded-[28px] border border-white/10 bg-white/92 shadow-[0_30px_80px_rgba(8,15,30,0.45)] backdrop-blur animate-fade-in-up sm:rounded-[32px]">
          <div className="absolute inset-x-0 top-0 h-1 bg-[linear-gradient(90deg,#22d3ee_0%,#38bdf8_45%,#34d399_100%)]" />
          <div className="pointer-events-none absolute inset-0">
            <div className="absolute -right-8 top-10 h-32 w-32 rounded-full bg-cyan-300/25 blur-2xl" />
            <div className="absolute left-[-2rem] bottom-0 h-36 w-36 rounded-full bg-emerald-300/20 blur-2xl" />
          </div>
          <CardHeader className="relative space-y-5 border-b border-slate-200/80 px-5 pb-5 pt-6 sm:space-y-6 sm:px-6 sm:pb-6">
            <div className="flex items-start justify-between gap-3 sm:gap-4">
              <div className="space-y-3">
                <span className="inline-flex items-center gap-2 rounded-full border border-cyan-200 bg-cyan-50 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-cyan-700 sm:text-xs">
                  <Radio className="h-3.5 w-3.5 motion-safe:animate-pulse" />
                  Self-host mode
                </span>
                <div className="space-y-2">
                  <h1 className="text-2xl font-semibold tracking-tight text-slate-950 sm:text-3xl">
                    Local Authentication
                  </h1>
                  <p className="max-w-md text-sm leading-6 text-slate-600">
                    Enter your access token to unlock Mission Control. This screen now
                    behaves like a proper launch bay instead of a plain password prompt.
                  </p>
                </div>
              </div>
              <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-[linear-gradient(145deg,#083344_0%,#0f766e_100%)] text-cyan-50 shadow-lg shadow-cyan-950/20 sm:h-14 sm:w-14">
                <Lock className="h-5 w-5 sm:h-6 sm:w-6" />
              </div>
            </div>
            <div className="hidden gap-3 sm:grid-cols-3 xl:grid">
              <div className="rounded-2xl border border-slate-200 bg-white/80 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  Access mode
                </p>
                <p className="mt-1 text-sm font-semibold text-slate-900">
                  Bearer token
                </p>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-white/80 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  Runtime
                </p>
                <p className="mt-1 text-sm font-semibold text-slate-900">
                  Mac mini live node
                </p>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-white/80 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  Status
                </p>
                <p className="mt-1 inline-flex items-center gap-2 text-sm font-semibold text-emerald-700">
                  <span className="h-2 w-2 rounded-full bg-emerald-500 motion-safe:animate-pulse" />
                  Secure tunnel online
                </p>
              </div>
            </div>
          </CardHeader>
          <CardContent className="relative px-5 pb-5 pt-5 sm:px-6 sm:pb-6 sm:pt-6">
            <form onSubmit={handleSubmit} className="space-y-5">
              <div className="space-y-2">
                <label
                  htmlFor="local-auth-token"
                  className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-500"
                >
                  Access token
                </label>
                <div className="rounded-2xl border border-slate-200 bg-white p-2 shadow-sm shadow-slate-950/5 transition focus-within:border-cyan-300 focus-within:ring-4 focus-within:ring-cyan-100">
                  <Input
                    id="local-auth-token"
                    type="password"
                    value={token}
                    onChange={(event) => setToken(event.target.value)}
                    placeholder="Paste token"
                    autoFocus
                    disabled={isValidating}
                    className="border-0 bg-transparent font-mono shadow-none focus-visible:ring-0"
                  />
                </div>
              </div>
              {error ? (
                <p className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                  {error}
                </p>
              ) : (
                <div className="flex flex-col gap-2 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 sm:flex-row sm:items-center sm:justify-between sm:gap-3">
                  <p className="text-xs text-slate-600">
                    Token must be at least {LOCAL_AUTH_TOKEN_MIN_LENGTH} characters.
                  </p>
                  <p className="text-xs font-medium text-slate-500">
                    Validation happens against your live backend.
                  </p>
                </div>
              )}
              <Button
                type="submit"
                className="group h-12 w-full rounded-2xl bg-[linear-gradient(135deg,#0891b2_0%,#0ea5e9_50%,#10b981_100%)] text-white shadow-lg shadow-cyan-950/20 transition hover:scale-[1.01] hover:shadow-xl hover:shadow-cyan-950/25"
                size="lg"
                disabled={isValidating}
              >
                <span>{isValidating ? "Validating..." : "Enter Mission Control"}</span>
                <ArrowRight className="ml-2 h-4 w-4 transition-transform group-hover:translate-x-0.5" />
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
