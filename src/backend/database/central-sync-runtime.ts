import type { NextFunction, Request, Response } from "express";
import { databaseLogger } from "../utils/logger.js";
import { DatabaseSaveTrigger } from "../utils/database-save-trigger.js";
import {
  CentralSync,
  CentralSyncError,
  readCentralSyncConfig,
  type CentralSyncConfig,
  type CentralTransport,
  type LocalSyncStore,
  type SyncRow,
} from "./central-sync.js";
import type { SyncEntityType } from "./repositories/sync-tombstone-repository.js";
import { createCurrentUserRepository } from "./repositories/factory.js";
import {
  assignMissingSyncIds,
  deleteRowsWithoutSyncId,
  deleteSyncRow,
  listSyncRows,
  upsertSyncRow,
} from "./routes/sync.js";

const REQUEST_TIMEOUT_MS = 30 * 1000;

class HttpCentralTransport implements CentralTransport {
  constructor(private readonly config: CentralSyncConfig) {}

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    let response: globalThis.Response;
    try {
      response = await fetch(`${this.config.serverUrl}${path}`, {
        ...init,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.config.apiKey}`,
          ...(init.headers ?? {}),
        },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (error) {
      throw new CentralSyncError(
        `Central server unreachable: ${error instanceof Error ? error.message : String(error)}`,
      );
    }

    const text = await response.text();
    let body: unknown = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      // A reverse proxy's login page, most likely. Reported below.
    }

    if (!response.ok) {
      const detail =
        body && typeof body === "object" && "error" in body
          ? String((body as { error: unknown }).error)
          : `HTTP ${response.status}`;
      throw new CentralSyncError(
        `Central server ${init.method ?? "GET"} ${path} failed: ${detail}`,
        response.status,
      );
    }
    if (body === null && text) {
      throw new CentralSyncError(
        `Central server returned a non-JSON response for ${path}; is a reverse proxy intercepting API requests?`,
        response.status,
      );
    }
    return body as T;
  }

  async getUsername(): Promise<string> {
    const user = await this.request<{ username?: string }>("/users/me");
    if (!user?.username) {
      throw new CentralSyncError("Central server did not return a username");
    }
    return user.username;
  }

  async listRows(entityType: SyncEntityType): Promise<SyncRow[]> {
    const data = await this.request<{ rows?: SyncRow[] }>(
      `/sync/${entityType}`,
    );
    return data?.rows ?? [];
  }

  async pushRow(entityType: SyncEntityType, row: SyncRow): Promise<void> {
    await this.request(`/sync/${entityType}`, {
      method: "POST",
      body: JSON.stringify({ row }),
    });
  }

  async pushTombstone(
    entityType: SyncEntityType,
    syncId: string,
  ): Promise<void> {
    await this.request("/sync/tombstones", {
      method: "POST",
      body: JSON.stringify({ entityType, syncId }),
    });
  }
}

const databaseLocalStore: LocalSyncStore = {
  async findUserIdByUsername(username) {
    const user = await createCurrentUserRepository().findByUsername(username);
    return user?.id ?? null;
  },
  listRows: (userId, entityType) => listSyncRows(userId, entityType),
  async upsertRow(userId, entityType, row) {
    await upsertSyncRow(userId, entityType, row);
  },
  deleteRow: deleteSyncRow,
  deleteRowsWithoutSyncId,
  assignMissingSyncIds,
  async persist(reason) {
    await DatabaseSaveTrigger.forceSave(reason);
  },
};

let instance: CentralSync | null | undefined;

/** The configured central sync, or null when this instance runs standalone. */
export function getCentralSync(): CentralSync | null {
  if (instance !== undefined) return instance;
  const config = readCentralSyncConfig();
  instance = config
    ? new CentralSync(
        config,
        new HttpCentralTransport(config),
        databaseLocalStore,
        databaseLogger,
      )
    : null;
  return instance;
}

export function startCentralSync(): void {
  const sync = getCentralSync();
  if (!sync) return;
  databaseLogger.info("Central sync enabled", {
    operation: "central_sync_start",
    serverUrl: sync.getStatus().serverUrl,
  });
  sync.start();
}

/**
 * Write-through middleware for an Express app serving entry routes; a no-op
 * when central sync is not configured.
 */
export function centralSyncWriteThrough() {
  const sync = getCentralSync();
  if (!sync) {
    return (_req: Request, _res: Response, next: NextFunction) => next();
  }
  return sync.writeThroughMiddleware();
}
