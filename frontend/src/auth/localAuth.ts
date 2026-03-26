"use client";

import { AuthMode } from "@/auth/mode";

let localToken: string | null = null;
const STORAGE_KEY = "mc_local_auth_token";

export function isLocalAuthMode(): boolean {
  return process.env.NEXT_PUBLIC_AUTH_MODE === AuthMode.Local;
}

function writeToken(storage: Storage | undefined, token: string | null) {
  if (!storage) return;
  try {
    if (token) {
      storage.setItem(STORAGE_KEY, token);
    } else {
      storage.removeItem(STORAGE_KEY);
    }
  } catch {
    // Ignore storage failures (private mode / policy).
  }
}

function readToken(storage: Storage | undefined): string | null {
  if (!storage) return null;
  try {
    const stored = storage.getItem(STORAGE_KEY);
    return stored || null;
  } catch {
    return null;
  }
}

export function setLocalAuthToken(token: string): void {
  localToken = token;
  if (typeof window === "undefined") return;
  writeToken(window.sessionStorage, token);
  writeToken(window.localStorage, token);
}

export function getLocalAuthToken(): string | null {
  if (localToken) return localToken;
  if (typeof window === "undefined") return null;
  const stored = readToken(window.sessionStorage) ?? readToken(window.localStorage);
  if (stored) {
    localToken = stored;
    return stored;
  }
  return null;
}

export function clearLocalAuthToken(): void {
  localToken = null;
  if (typeof window === "undefined") return;
  writeToken(window.sessionStorage, null);
  writeToken(window.localStorage, null);
}
