const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1"]);

function normalizeUrl(raw: string | undefined | null): string | null {
  if (!raw) return null;
  const normalized = raw.replace(/\/+$/, "");
  return normalized || null;
}

function isLocalHost(hostname: string): boolean {
  return (
    LOCAL_HOSTS.has(hostname) ||
    hostname.endsWith(".local") ||
    hostname.startsWith("192.168.") ||
    hostname.startsWith("10.") ||
    hostname.startsWith("172.16.") ||
    hostname.startsWith("172.17.") ||
    hostname.startsWith("172.18.") ||
    hostname.startsWith("172.19.") ||
    hostname.startsWith("172.2") ||
    hostname.startsWith("172.30.") ||
    hostname.startsWith("172.31.")
  );
}

function deriveHostedApiBaseUrl(hostname: string, protocol: string): string | null {
  if (isLocalHost(hostname)) {
    return null;
  }
  if (hostname.startsWith("mission.")) {
    return `${protocol}//api.${hostname.slice("mission.".length)}`;
  }
  return null;
}

export function getApiBaseUrl(): string {
  const envBaseUrl = normalizeUrl(process.env.NEXT_PUBLIC_API_URL);

  if (typeof window !== "undefined") {
    const hostedBaseUrl = deriveHostedApiBaseUrl(
      window.location.hostname,
      window.location.protocol,
    );
    if (hostedBaseUrl) {
      return hostedBaseUrl;
    }
  }

  if (!envBaseUrl) {
    throw new Error("NEXT_PUBLIC_API_URL is not set.");
  }

  try {
    const parsed = new URL(envBaseUrl);
    if (typeof window !== "undefined") {
      const hostedBaseUrl = deriveHostedApiBaseUrl(
        window.location.hostname,
        window.location.protocol,
      );
      if (hostedBaseUrl && isLocalHost(parsed.hostname)) {
        return hostedBaseUrl;
      }
    }
  } catch {
    throw new Error("NEXT_PUBLIC_API_URL is invalid.");
  }

  return envBaseUrl;
}
