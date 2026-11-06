// Who is signed in and through which hub, as the editor's header says it.
import type { HubConnection } from "../../../api/hubConnection.api";

export interface HubIdentity {
  /** Who is signed in at the hub: full name, else username; null when the connection does not say. */
  who: string | null;
  /** The hub's organisation name, else its address. */
  where: string;
  /** The hub, for the link back to it. */
  url: string;
}

/** A hub URL as an address: its host and port, without the scheme. */
export function hubAddress(url: string): string {
  try {
    return new URL(url).host || url;
  } catch {
    return url;
  }
}

/**
 * The header's who and where, from this person's hub connection: a server joined to a hub answers the person who
 * signed in through it, a desktop connected to one the account it connected with. Nothing on a desktop with no hub.
 */
export function hubIdentity(connection: HubConnection | null): HubIdentity | null {
  if (!connection?.connected || !connection.hub_url) return null;
  return {
    who: connection.account || null,
    where: connection.org_name || hubAddress(connection.hub_url),
    url: connection.hub_url,
  };
}

/** The header's tooltip: who, through which hub, and that a click opens it. */
export function hubIdentityTitle(identity: HubIdentity): string {
  const signedIn = identity.who
    ? `Signed in as ${identity.who} through ${identity.where}`
    : `Connected to ${identity.where}`;
  return `${signedIn}. Open the hub.`;
}
