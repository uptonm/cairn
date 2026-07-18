import "server-only";

import { clerkClient } from "@clerk/nextjs/server";

const CACHE_TTL_MS = 60_000;

let cachedGate: { expiresAt: number; isGated: boolean } | undefined;
let pendingGateLookup: Promise<boolean> | undefined;

/**
 * Reads this app's gate from the fleet operations organization. The module
 * cache limits Clerk Backend API reads to one per minute per runtime instance.
 */
export async function isAppGated(): Promise<boolean> {
  if (cachedGate && cachedGate.expiresAt > Date.now()) {
    return cachedGate.isGated;
  }

  if (!pendingGateLookup) {
    pendingGateLookup = readGate().finally(() => {
      pendingGateLookup = undefined;
    });
  }

  return pendingGateLookup;
}

async function readGate(): Promise<boolean> {
  const organizationId = process.env.GATES_ORG_ID;
  const appId = process.env.GATES_APP_ID;

  if (!organizationId || !appId) {
    throw new Error("GATES_ORG_ID and GATES_APP_ID must be configured in production.");
  }

  const client = await clerkClient();
  const organization = await client.organizations.getOrganization({ organizationId });
  const gates = organization.publicMetadata?.gates;
  const isGated =
    typeof gates === "object" &&
    gates !== null &&
    !Array.isArray(gates) &&
    (gates as Record<string, unknown>)[appId] === true;

  cachedGate = {
    isGated,
    expiresAt: Date.now() + CACHE_TTL_MS,
  };

  return isGated;
}
