// Central sync: runs this instance as a per-user satellite of a central
// Termix server that stays the source of truth for the user's entries.
//
// - Pull: on a timer (and whenever a write needs a clean baseline) every
//   synced entity type is fetched from the central server and applied to the
//   local user authoritatively -- rows are upserted by syncId, and local rows
//   the central server does not have are deleted.
// - Write-through: a mutating request to an entry route is serialized behind
//   a lock. When its handler answers 2xx, the response is held back while the
//   local rows that now differ from the last pulled baseline are pushed to the
//   central server, followed by a fresh pull. If the central server refuses
//   the change, a pull rolls the local instance back to the central state and
//   the client gets a 502 instead of the handler's success.
//
// Diffing against the pulled baseline rather than intercepting each route's
// payload means every create/update/delete path is covered, including ones
// that touch several tables, without translating local numeric ids: the wire
// form references other rows by syncId only.

import crypto from "crypto";
import type { NextFunction, Request, Response } from "express";
import type { SyncEntityType } from "./repositories/sync-tombstone-repository.js";

export type SyncRow = Record<string, unknown>;

// Ordered by reference dependency: hosts and snippets resolve credential,
// vault and folder syncIds, so those have to exist on the other side first.
// Mirrors electron/remote-sync-entities.cjs.
const CENTRAL_SYNC_ENTITY_ORDER: readonly SyncEntityType[] = [
  "sshCredentials",
  "vaultProfiles",
  "sshFolders",
  "snippetFolders",
  "hosts",
  "snippets",
  "dashboardServiceLinks",
  "homepageItems",
  "userPreferences",
];

const SINGLETON_ENTITY_TYPES = new Set<SyncEntityType>(["userPreferences"]);

// Route prefixes whose mutating requests can change a synced entity.
const CENTRAL_SYNC_WRITE_PREFIXES: readonly string[] = [
  "/host",
  "/credentials",
  "/snippets",
  "/vault",
  "/user-preferences",
  "/homepage/items",
  "/service-links",
];

const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

// Fields that differ between two copies of the same row without anyone having
// edited it, so they must not make a row look changed.
const UNHASHED_FIELDS = ["id", "userId", "createdAt", "updatedAt"];

const DEFAULT_INTERVAL_SECONDS = 60;
const MIN_INTERVAL_SECONDS = 10;
const WRITE_LOCK_TIMEOUT_MS = 2 * 60 * 1000;

export interface CentralSyncConfig {
  serverUrl: string;
  apiKey: string;
  localUsername: string | null;
  intervalMs: number;
}

export function readCentralSyncConfig(
  env: NodeJS.ProcessEnv = process.env,
): CentralSyncConfig | null {
  const serverUrl = env.CENTRAL_SERVER_URL?.trim();
  if (!serverUrl) return null;

  const apiKey = env.CENTRAL_SERVER_API_KEY?.trim();
  if (!apiKey) {
    throw new Error(
      "CENTRAL_SERVER_URL is set but CENTRAL_SERVER_API_KEY is missing",
    );
  }

  const parsedInterval = Number(env.CENTRAL_SYNC_INTERVAL_SECONDS);
  const intervalSeconds =
    Number.isFinite(parsedInterval) && parsedInterval > 0
      ? Math.max(parsedInterval, MIN_INTERVAL_SECONDS)
      : DEFAULT_INTERVAL_SECONDS;

  return {
    serverUrl: serverUrl.replace(/\/+$/, ""),
    apiKey,
    localUsername: env.CENTRAL_SYNC_LOCAL_USER?.trim() || null,
    intervalMs: intervalSeconds * 1000,
  };
}

export interface CentralTransport {
  getUsername(): Promise<string>;
  listRows(entityType: SyncEntityType): Promise<SyncRow[]>;
  pushRow(entityType: SyncEntityType, row: SyncRow): Promise<void>;
  pushTombstone(entityType: SyncEntityType, syncId: string): Promise<void>;
}

export interface LocalSyncStore {
  findUserIdByUsername(username: string): Promise<string | null>;
  listRows(userId: string, entityType: SyncEntityType): Promise<SyncRow[]>;
  upsertRow(
    userId: string,
    entityType: SyncEntityType,
    row: SyncRow,
  ): Promise<void>;
  deleteRow(
    userId: string,
    entityType: SyncEntityType,
    syncId: string,
  ): Promise<void>;
  deleteRowsWithoutSyncId(
    userId: string,
    entityType: SyncEntityType,
  ): Promise<void>;
  assignMissingSyncIds(
    userId: string,
    entityType: SyncEntityType,
  ): Promise<number>;
  persist(reason: string): Promise<void>;
}

export interface CentralSyncLogger {
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, error?: unknown, meta?: Record<string, unknown>): void;
}

export interface CentralSyncStatus {
  enabled: true;
  serverUrl: string;
  localUserId: string | null;
  lastPulledAt: string | null;
  lastPushedAt: string | null;
  lastError: string | null;
}

export class CentralSyncError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "CentralSyncError";
  }
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const entries = Object.keys(value as Record<string, unknown>)
      .sort()
      .map(
        (key) =>
          `${JSON.stringify(key)}:${stableStringify((value as Record<string, unknown>)[key])}`,
      );
    return `{${entries.join(",")}}`;
  }
  return JSON.stringify(value ?? null);
}

export function hashSyncRow(row: SyncRow): string {
  const content = { ...row };
  for (const field of UNHASHED_FIELDS) delete content[field];
  return crypto
    .createHash("sha256")
    .update(stableStringify(content))
    .digest("hex");
}

export function matchesWritePrefix(
  path: string,
  prefixes: readonly string[] = CENTRAL_SYNC_WRITE_PREFIXES,
): boolean {
  return prefixes.some(
    (prefix) => path === prefix || path.startsWith(`${prefix}/`),
  );
}

type Release = () => void;

class AsyncMutex {
  private tail: Promise<void> = Promise.resolve();
  private held = false;

  get locked(): boolean {
    return this.held;
  }

  acquire(): Promise<Release> {
    let unlock!: () => void;
    const next = new Promise<void>((resolve) => {
      unlock = resolve;
    });
    const previous = this.tail;
    this.tail = previous.then(() => next);
    return previous.then(() => {
      this.held = true;
      let released = false;
      return () => {
        if (released) return;
        released = true;
        this.held = false;
        unlock();
      };
    });
  }

  async runExclusive<T>(task: () => Promise<T>): Promise<T> {
    const release = await this.acquire();
    try {
      return await task();
    } finally {
      release();
    }
  }
}

type Snapshot = Map<SyncEntityType, Map<string, string>>;

interface PendingChange {
  entityType: SyncEntityType;
  row: SyncRow;
}

interface PendingDeletion {
  entityType: SyncEntityType;
  syncId: string;
}

export class CentralSync {
  private readonly mutex = new AsyncMutex();
  private snapshot: Snapshot | null = null;
  private localUserId: string | null = null;
  private timer: NodeJS.Timeout | null = null;
  private status: CentralSyncStatus;

  constructor(
    private readonly config: CentralSyncConfig,
    private readonly central: CentralTransport,
    private readonly local: LocalSyncStore,
    private readonly logger: CentralSyncLogger,
  ) {
    this.status = {
      enabled: true,
      serverUrl: config.serverUrl,
      localUserId: null,
      lastPulledAt: null,
      lastPushedAt: null,
      lastError: null,
    };
  }

  getStatus(): CentralSyncStatus {
    return { ...this.status };
  }

  start(): void {
    this.stop();
    this.timer = setInterval(() => void this.tick(), this.config.intervalMs);
    this.timer.unref?.();
    void this.tick();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /** One scheduled pull. Skipped while a write holds the lock. */
  async tick(): Promise<void> {
    if (this.mutex.locked) return;
    try {
      await this.mutex.runExclusive(() => this.pull());
    } catch (error) {
      this.logger.error("Central sync pull failed", error, {
        operation: "central_sync_pull_failed",
        serverUrl: this.config.serverUrl,
      });
    }
  }

  /** Pulls now, waiting for any in-flight write to finish first. */
  syncNow(): Promise<void> {
    return this.mutex.runExclusive(() => this.pull());
  }

  private async resolveLocalUserId(): Promise<string> {
    if (this.localUserId) return this.localUserId;
    const username =
      this.config.localUsername ?? (await this.central.getUsername());
    const userId = await this.local.findUserIdByUsername(username);
    if (!userId) {
      throw new CentralSyncError(
        `No local user named "${username}" to mirror the central account into. Create it, or set CENTRAL_SYNC_LOCAL_USER.`,
      );
    }
    this.localUserId = userId;
    this.status.localUserId = userId;
    return userId;
  }

  /**
   * Makes the local user's entries match the central server exactly. Must be
   * called with the lock held.
   */
  private async pull(): Promise<void> {
    try {
      const userId = await this.resolveLocalUserId();

      // Fetch everything before touching local state, so an unreachable
      // central server leaves the local copy intact rather than half-applied.
      const centralRows = new Map<SyncEntityType, SyncRow[]>();
      for (const entityType of CENTRAL_SYNC_ENTITY_ORDER) {
        centralRows.set(entityType, await this.central.listRows(entityType));
      }

      const centralSyncIds = new Map<SyncEntityType, Set<string>>();
      for (const entityType of CENTRAL_SYNC_ENTITY_ORDER) {
        const ids = new Set<string>();
        for (const row of centralRows.get(entityType) ?? []) {
          if (typeof row.syncId !== "string" || !row.syncId) continue;
          try {
            await this.local.upsertRow(userId, entityType, row);
            ids.add(row.syncId);
          } catch (error) {
            this.logger.warn("Skipped a central row that failed to apply", {
              operation: "central_sync_row_skipped",
              entityType,
              syncId: row.syncId,
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }
        centralSyncIds.set(entityType, ids);
      }

      // Dependents before their dependencies, so a host goes before the
      // credential it referenced.
      for (const entityType of [...CENTRAL_SYNC_ENTITY_ORDER].reverse()) {
        if (SINGLETON_ENTITY_TYPES.has(entityType)) continue;
        const keep = centralSyncIds.get(entityType)!;
        for (const row of await this.local.listRows(userId, entityType)) {
          if (typeof row.syncId === "string" && !keep.has(row.syncId)) {
            await this.local.deleteRow(userId, entityType, row.syncId);
          }
        }
        await this.local.deleteRowsWithoutSyncId(userId, entityType);
      }

      const snapshot: Snapshot = new Map();
      for (const entityType of CENTRAL_SYNC_ENTITY_ORDER) {
        const keep = centralSyncIds.get(entityType)!;
        const hashes = new Map<string, string>();
        for (const row of await this.local.listRows(userId, entityType)) {
          if (typeof row.syncId === "string" && keep.has(row.syncId)) {
            hashes.set(row.syncId, hashSyncRow(row));
          }
        }
        snapshot.set(entityType, hashes);
      }

      await this.local.persist("central_sync_pull");
      this.snapshot = snapshot;
      this.status.lastPulledAt = new Date().toISOString();
      this.status.lastError = null;
    } catch (error) {
      this.snapshot = null;
      this.status.lastError =
        error instanceof Error ? error.message : String(error);
      throw error;
    }
  }

  /**
   * Local rows that differ from the pulled baseline. Must be called with the
   * lock held and a baseline in place.
   */
  private async collectLocalChanges(userId: string): Promise<{
    changes: PendingChange[];
    deletions: PendingDeletion[];
  }> {
    const snapshot = this.snapshot!;
    const changes: PendingChange[] = [];
    const deletions: PendingDeletion[] = [];

    for (const entityType of CENTRAL_SYNC_ENTITY_ORDER) {
      await this.local.assignMissingSyncIds(userId, entityType);
      const baseline = snapshot.get(entityType) ?? new Map<string, string>();
      const seen = new Set<string>();
      for (const row of await this.local.listRows(userId, entityType)) {
        if (typeof row.syncId !== "string" || !row.syncId) continue;
        seen.add(row.syncId);
        if (baseline.get(row.syncId) !== hashSyncRow(row)) {
          changes.push({ entityType, row });
        }
      }
      if (SINGLETON_ENTITY_TYPES.has(entityType)) continue;
      for (const syncId of baseline.keys()) {
        if (!seen.has(syncId)) deletions.push({ entityType, syncId });
      }
    }

    // Dependents first, mirroring the pull.
    deletions.reverse();
    return { changes, deletions };
  }

  /**
   * Forwards the local changes a request made to the central server, then
   * re-pulls so the local copy holds exactly what the central server stored.
   * On refusal the re-pull discards the local change. Must be called with the
   * lock held.
   */
  private async forwardLocalChanges(): Promise<number> {
    const userId = await this.resolveLocalUserId();
    const { changes, deletions } = await this.collectLocalChanges(userId);
    if (changes.length === 0 && deletions.length === 0) return 0;

    try {
      for (const { entityType, row } of changes) {
        await this.central.pushRow(entityType, row);
      }
      for (const { entityType, syncId } of deletions) {
        await this.central.pushTombstone(entityType, syncId);
      }
    } catch (error) {
      this.logger.warn("Central server refused a local change; rolling back", {
        operation: "central_sync_forward_refused",
        error: error instanceof Error ? error.message : String(error),
      });
      try {
        await this.pull();
      } catch (rollbackError) {
        this.logger.error(
          "Could not restore the central state after a refused change",
          rollbackError,
          { operation: "central_sync_rollback_failed" },
        );
      }
      throw error;
    }

    this.status.lastPushedAt = new Date().toISOString();
    await this.pull();
    return changes.length + deletions.length;
  }

  /**
   * Express middleware that makes entry writes go through the central server.
   * Mount it before the routers it guards.
   */
  writeThroughMiddleware(
    prefixes: readonly string[] = CENTRAL_SYNC_WRITE_PREFIXES,
  ) {
    return async (req: Request, res: Response, next: NextFunction) => {
      if (
        !MUTATING_METHODS.has(req.method) ||
        !matchesWritePrefix(req.path, prefixes)
      ) {
        return next();
      }

      const release = await this.mutex.acquire();
      const timeout = setTimeout(() => {
        this.logger.warn("Releasing a write lock held past its timeout", {
          operation: "central_sync_lock_timeout",
          path: req.path,
        });
        release();
      }, WRITE_LOCK_TIMEOUT_MS);
      timeout.unref?.();
      const releaseOnce = () => {
        clearTimeout(timeout);
        release();
      };
      res.once("finish", releaseOnce);
      res.once("close", releaseOnce);

      // Without a baseline every local row would look new; take one before
      // the handler changes anything.
      if (!this.snapshot) {
        try {
          await this.pull();
        } catch (error) {
          res.status(503).json({
            error: "Central server unavailable; changes are disabled",
            details: error instanceof Error ? error.message : String(error),
          });
          return;
        }
      }

      this.holdResponseUntilForwarded(req, res);
      next();
    };
  }

  private holdResponseUntilForwarded(req: Request, res: Response): void {
    const originals = { json: res.json, send: res.send, end: res.end };
    const restore = () => {
      res.json = originals.json;
      res.send = originals.send;
      res.end = originals.end;
    };

    const gate =
      (method: keyof typeof originals) =>
      (...args: unknown[]): Response => {
        restore();
        const passThrough = () =>
          (originals[method] as (...a: unknown[]) => unknown).apply(res, args);

        const requestUserId = (req as Request & { userId?: string }).userId;
        const succeeded = res.statusCode >= 200 && res.statusCode < 300;
        if (
          res.headersSent ||
          !succeeded ||
          !requestUserId ||
          requestUserId !== this.localUserId
        ) {
          passThrough();
          return res;
        }

        this.forwardLocalChanges()
          .then(() => passThrough())
          .catch((error: unknown) => {
            const message =
              error instanceof Error ? error.message : String(error);
            this.status.lastError = message;
            res.status(502).json({
              error: "The central server did not accept this change",
              details: message,
            });
          });
        return res;
      };

    res.json = gate("json") as Response["json"];
    res.send = gate("send") as Response["send"];
    res.end = gate("end") as Response["end"];
  }
}
