"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Activity,
  BarChart3,
  Bot,
  Boxes,
  BriefcaseBusiness,
  CheckCircle2,
  FileText,
  Folder,
  Building2,
  LayoutGrid,
  Mic,
  Network,
  Settings,
  Store,
  Tags,
  UsersRound,
  Wrench,
  X,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { useAuth } from "@/auth/clerk";
import { ApiError } from "@/api/mutator";
import { useOrganizationMembership } from "@/lib/use-organization-membership";
import {
  type healthzHealthzGetResponse,
  useHealthzHealthzGet,
} from "@/api/generated/default/default";
import { cn } from "@/lib/utils";
import { useControlCenterState } from "@/lib/control-center";

export function DashboardSidebar() {
  const pathname = usePathname();
  const [isMobileOpen, setIsMobileOpen] = useState(false);
  const { isSignedIn } = useAuth();
  const { isAdmin } = useOrganizationMembership(isSignedIn);
  const healthQuery = useHealthzHealthzGet<healthzHealthzGetResponse, ApiError>(
    {
      query: {
        refetchInterval: 30_000,
        refetchOnMount: "always",
        retry: false,
      },
      request: { cache: "no-store" },
    },
  );

  const okValue = healthQuery.data?.data?.ok;
  const systemStatus: "unknown" | "operational" | "degraded" =
    okValue === true
      ? "operational"
      : okValue === false
        ? "degraded"
        : healthQuery.isError
          ? "degraded"
          : "unknown";
  const statusLabel =
    systemStatus === "operational"
      ? "All systems operational"
      : systemStatus === "unknown"
        ? "System status unavailable"
        : "System degraded";
  const { enabledModules, ready } = useControlCenterState();
  const iconByCategory: Record<string, LucideIcon> = {
    finance: BriefcaseBusiness,
    network_marketing: UsersRound,
    newsletters: FileText,
    podcasts: Mic,
    custom: LayoutGrid,
  };

  useEffect(() => {
    const handleToggle = () => setIsMobileOpen((current) => !current);
    const handleClose = () => setIsMobileOpen(false);

    window.addEventListener("dashboard-sidebar:toggle", handleToggle);
    window.addEventListener("dashboard-sidebar:close", handleClose);

    return () => {
      window.removeEventListener("dashboard-sidebar:toggle", handleToggle);
      window.removeEventListener("dashboard-sidebar:close", handleClose);
    };
  }, []);

  const handleMobileClose = () => setIsMobileOpen(false);
  const linkClass = (isActive: boolean) =>
    cn(
      "group/mc-link flex items-center gap-3 rounded-xl px-3 py-2.5 text-[color:var(--text-muted)] transition-all duration-200",
      isActive
        ? "bg-[linear-gradient(135deg,rgba(8,145,178,0.16)_0%,rgba(34,211,238,0.14)_50%,rgba(16,185,129,0.16)_100%)] text-[color:var(--accent-strong)] shadow-[inset_0_0_0_1px_rgba(8,145,178,0.12)]"
        : "hover:bg-white/70 hover:text-[color:var(--accent)] hover:translate-x-1",
    );

  const sidebarContent = (
    <>
      <div className="border-b border-[color:var(--border)] bg-[linear-gradient(135deg,rgba(8,145,178,0.08)_0%,rgba(34,211,238,0.04)_100%)] px-4 py-3 lg:hidden">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-semibold text-strong">Navigation</p>
            <p className="text-xs text-muted">Mission Control</p>
          </div>
          <button
            type="button"
            className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-[color:var(--border)] bg-white/65 text-muted transition hover:border-[color:var(--accent)] hover:bg-white hover:text-[color:var(--accent)]"
            aria-label="Close navigation"
            onClick={handleMobileClose}
          >
            <X className="h-5 w-5" />
          </button>
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-3 py-4">
        <p className="px-3 text-xs font-semibold uppercase tracking-wider text-quiet">
          Navigation
        </p>
        <nav className="mt-3 space-y-4 text-sm">
          <div>
            <p className="px-3 text-[11px] font-semibold uppercase tracking-wider text-quiet">
              Overview
            </p>
            <div className="mt-1 space-y-1">
              <Link
                href="/dashboard"
                className={linkClass(pathname === "/dashboard")}
              >
                <BarChart3 className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110 group-hover/mc-link:-rotate-3" />
                Dashboard
              </Link>
              <Link
                href="/activity"
                className={linkClass(pathname.startsWith("/activity"))}
              >
                <Activity className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110 group-hover/mc-link:-rotate-3" />
                Live feed
              </Link>
            </div>
          </div>

          <div>
            <p className="px-3 text-[11px] font-semibold uppercase tracking-wider text-quiet">
              Boards
            </p>
            <div className="mt-1 space-y-1">
              <Link
                href="/board-groups"
                className={linkClass(pathname.startsWith("/board-groups"))}
              >
                <Folder className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                Board groups
              </Link>
              <Link
                href="/boards"
                className={linkClass(pathname.startsWith("/boards"))}
              >
                <LayoutGrid className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                Boards
              </Link>
              <Link
                href="/tags"
                className={linkClass(pathname.startsWith("/tags"))}
              >
                <Tags className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                Tags
              </Link>
              <Link
                href="/approvals"
                className={linkClass(pathname.startsWith("/approvals"))}
              >
                <CheckCircle2 className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                Approvals
              </Link>
              {isAdmin ? (
                <Link
                  href="/custom-fields"
                  className={linkClass(pathname.startsWith("/custom-fields"))}
                >
                  <Settings className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110 group-hover/mc-link:rotate-12" />
                  Custom fields
                </Link>
              ) : null}
            </div>
          </div>

          <div>
            <p className="px-3 text-[11px] font-semibold uppercase tracking-wider text-quiet">
              Custom Control
            </p>
            <div className="mt-1 space-y-1">
              <Link
                href="/control-center"
                className={linkClass(pathname === "/control-center")}
              >
                <LayoutGrid className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                Control center
              </Link>
              <Link
                href="/control-center/builder"
                className={linkClass(pathname.startsWith("/control-center/builder"))}
              >
                <Wrench className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110 group-hover/mc-link:rotate-12" />
                Builder
              </Link>
              {ready
                ? enabledModules.map((module) => {
                    const Icon = iconByCategory[module.category] ?? LayoutGrid;
                    return (
                      <Link
                        key={module.id}
                        href={`/control-center/${module.slug}`}
                        className={linkClass(
                          pathname === `/control-center/${module.slug}`,
                        )}
                      >
                        <Icon className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                        {module.title}
                      </Link>
                    );
                  })
                : null}
            </div>
          </div>

          <div>
            {isAdmin ? (
              <>
                <p className="px-3 text-[11px] font-semibold uppercase tracking-wider text-quiet">
                  Skills
                </p>
                <div className="mt-1 space-y-1">
                  <Link
                    href="/skills/marketplace"
                    className={linkClass(
                      pathname === "/skills" ||
                        pathname.startsWith("/skills/marketplace"),
                    )}
                  >
                    <Store className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                    Marketplace
                  </Link>
                  <Link
                    href="/skills/packs"
                    className={linkClass(pathname.startsWith("/skills/packs"))}
                  >
                    <Boxes className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                    Packs
                  </Link>
                </div>
              </>
            ) : null}
          </div>

          <div>
            <p className="px-3 text-[11px] font-semibold uppercase tracking-wider text-quiet">
              Administration
            </p>
            <div className="mt-1 space-y-1">
              <Link
                href="/organization"
                className={linkClass(pathname.startsWith("/organization"))}
              >
                <Building2 className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                Organization
              </Link>
              {isAdmin ? (
                <Link
                  href="/gateways"
                  className={linkClass(pathname.startsWith("/gateways"))}
                >
                  <Network className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                  Gateways
                </Link>
              ) : null}
              {isAdmin ? (
                <Link
                  href="/agents"
                  className={linkClass(pathname.startsWith("/agents"))}
                >
                  <Bot className="h-4 w-4 transition-transform duration-200 group-hover/mc-link:scale-110" />
                  Agents
                </Link>
              ) : null}
            </div>
          </div>
        </nav>
      </div>
      <div className="border-t border-[color:var(--border)] bg-[linear-gradient(180deg,rgba(255,255,255,0.3)_0%,rgba(233,247,252,0.55)_100%)] p-4">
        <div className="flex items-center gap-2 text-xs text-muted">
          <span
            className={cn(
              "h-2 w-2 rounded-full",
              systemStatus === "operational" && "bg-emerald-500",
              systemStatus === "degraded" && "bg-rose-500",
              systemStatus === "unknown" && "bg-[color:var(--text-quiet)]",
            )}
          />
          {statusLabel}
        </div>
      </div>
    </>
  );

  return (
    <>
      <div
        className={cn(
          "fixed inset-0 z-50 bg-[#08111f]/45 backdrop-blur-[2px] transition lg:hidden",
          isMobileOpen
            ? "pointer-events-auto opacity-100"
            : "pointer-events-none opacity-0",
        )}
        onClick={handleMobileClose}
        aria-hidden={!isMobileOpen}
      />
      <aside
        className={cn(
          "surface-glass fixed inset-y-0 left-0 z-50 flex w-[min(88vw,320px)] max-w-full flex-col border-r border-[color:var(--border)] shadow-xl transition-transform duration-300 ease-out lg:hidden",
          isMobileOpen ? "translate-x-0" : "-translate-x-full",
        )}
        aria-hidden={!isMobileOpen}
        onClickCapture={(event) => {
          const target = event.target;
          if (!(target instanceof HTMLElement)) return;
          if (target.closest("a[href]")) {
            handleMobileClose();
          }
        }}
      >
        {sidebarContent}
      </aside>
      <aside className="surface-glass animate-lift-in hidden h-full w-64 flex-col border-r border-[color:var(--border)] lg:flex">
        {sidebarContent}
      </aside>
    </>
  );
}
