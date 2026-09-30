import { afterEach, describe, expect, it } from "vitest";
import express from "express";
import type { AddressInfo } from "net";
import type { Server } from "http";
import {
  CentralSync,
  CentralSyncError,
  hashSyncRow,
  matchesWritePrefix,
  readCentralSyncConfig,
  type CentralTransport,
  type LocalSyncStore,
  type SyncRow,
} from "../../database/central-sync.js";
import type { SyncEntityType } from "../../database/repositories/sync-tombstone-repository.js";

type Tables = Map<SyncEntityType, Map<string, SyncRow>>;

function table(tables: Tables, entityType: SyncEntityType) {
  let rows = tables.get(entityType);
  if (!rows) {
    rows = new Map();
    tables.set(entityType, rows);
  }
  return rows;
}

class FakeCentral implements CentralTransport {
  tables: Tables = new Map();
  refuse: string | null = null;
  unreachable = false;
  pushed: Array<{ entityType: SyncEntityType; syncId: string }> = [];
  tombstoned: Array<{ entityType: SyncEntityType; syncId: string }> = [];

  async getUsername() {
    return "alice";
  }

  async listRows(entityType: SyncEntityType) {
    if (this.unreachable) throw new CentralSyncError("unreachable");
    return [...table(this.tables, entityType).values()].map((row) => ({
      ...row,
    }));
  }

  async pushRow(entityType: SyncEntityType, row: SyncRow) {
    if (this.refuse) throw new CentralSyncError(this.refuse, 400);
    this.pushed.push({ entityType, syncId: row.syncId as string });
    table(this.tables, entityType).set(row.syncId as string, {
      ...row,
      id: 1000 + this.pushed.length,
    });
  }

  async pushTombstone(entityType: SyncEntityType, syncId: string) {
    if (this.refuse) throw new CentralSyncError(this.refuse, 400);
    this.tombstoned.push({ entityType, syncId });
    table(this.tables, entityType).delete(syncId);
  }
}

class FakeLocal implements LocalSyncStore {
  tables: Tables = new Map();
  users = new Map([["alice", "local-alice"]]);
  unsynced: Array<{ entityType: SyncEntityType; row: SyncRow }> = [];
  nextSyncId = 0;

  async findUserIdByUsername(username: string) {
    return this.users.get(username) ?? null;
  }

  async listRows(_userId: string, entityType: SyncEntityType) {
    return [...table(this.tables, entityType).values()].map((row) => ({
      ...row,
    }));
  }

  async upsertRow(_userId: string, entityType: SyncEntityType, row: SyncRow) {
    const rows = table(this.tables, entityType);
    const existing = rows.get(row.syncId as string);
    rows.set(row.syncId as string, {
      ...row,
      id: existing?.id ?? rows.size + 1,
      userId: "local-alice",
    });
  }

  async deleteRow(_userId: string, entityType: SyncEntityType, syncId: string) {
    table(this.tables, entityType).delete(syncId);
  }

  async deleteRowsWithoutSyncId() {
    this.unsynced = [];
  }

  async assignMissingSyncIds(_userId: string, entityType: SyncEntityType) {
    const pending = this.unsynced.filter((u) => u.entityType === entityType);
    for (const { row } of pending) {
      const syncId = `local-${++this.nextSyncId}`;
      table(this.tables, entityType).set(syncId, { ...row, syncId });
    }
    this.unsynced = this.unsynced.filter((u) => u.entityType !== entityType);
    return pending.length;
  }

  async persist() {}
}

const silentLogger = { info() {}, warn() {}, error() {} };

function createSync(central = new FakeCentral(), local = new FakeLocal()) {
  const sync = new CentralSync(
    {
      serverUrl: "https://central.example",
      apiKey: "tmx_test",
      localUsername: null,
      intervalMs: 60_000,
    },
    central,
    local,
    silentLogger,
  );
  return { sync, central, local };
}

describe("readCentralSyncConfig", () => {
  it("is disabled without a central server URL", () => {
    expect(readCentralSyncConfig({})).toBeNull();
  });

  it("requires an API key once a URL is set", () => {
    expect(() =>
      readCentralSyncConfig({ CENTRAL_SERVER_URL: "https://c.example" }),
    ).toThrow(/CENTRAL_SERVER_API_KEY/);
  });

  it("normalizes the URL and clamps the interval", () => {
    expect(
      readCentralSyncConfig({
        CENTRAL_SERVER_URL: "https://c.example/",
        CENTRAL_SERVER_API_KEY: "tmx_abc",
        CENTRAL_SYNC_INTERVAL_SECONDS: "2",
        CENTRAL_SYNC_LOCAL_USER: "bob",
      }),
    ).toEqual({
      serverUrl: "https://c.example",
      apiKey: "tmx_abc",
      localUsername: "bob",
      intervalMs: 10_000,
    });
  });
});

describe("hashSyncRow", () => {
  it("ignores ids and timestamps but not content", () => {
    const row = { syncId: "a", name: "web", id: 1, updatedAt: "x" };
    expect(hashSyncRow(row)).toBe(
      hashSyncRow({ name: "web", syncId: "a", id: 7, updatedAt: "y" }),
    );
    expect(hashSyncRow(row)).not.toBe(hashSyncRow({ ...row, name: "db" }));
  });
});

describe("matchesWritePrefix", () => {
  it("matches whole path segments only", () => {
    expect(matchesWritePrefix("/host")).toBe(true);
    expect(matchesWritePrefix("/host/db/5")).toBe(true);
    expect(matchesWritePrefix("/host-sidebar/preferences")).toBe(false);
    expect(matchesWritePrefix("/users/login")).toBe(false);
  });
});

describe("CentralSync pull", () => {
  it("makes the local copy match the central server", async () => {
    const { sync, central, local } = createSync();
    table(central.tables, "hosts").set("h1", { syncId: "h1", name: "web" });
    table(local.tables, "hosts").set("stale", { syncId: "stale", name: "x" });
    table(local.tables, "hosts").set("h1", { syncId: "h1", name: "old" });

    await sync.syncNow();

    expect([...table(local.tables, "hosts").keys()]).toEqual(["h1"]);
    expect(table(local.tables, "hosts").get("h1")?.name).toBe("web");
    expect(sync.getStatus().lastPulledAt).not.toBeNull();
  });

  it("leaves local data alone when the central server is unreachable", async () => {
    const { sync, central, local } = createSync();
    central.unreachable = true;
    table(local.tables, "hosts").set("h1", { syncId: "h1", name: "web" });

    await expect(sync.syncNow()).rejects.toThrow("unreachable");
    expect(table(local.tables, "hosts").has("h1")).toBe(true);
    expect(sync.getStatus().lastError).toBe("unreachable");
  });

  it("refuses to run without a matching local user", async () => {
    const { sync, local } = createSync();
    local.users.clear();
    await expect(sync.syncNow()).rejects.toThrow(/No local user named/);
  });
});

describe("CentralSync write-through middleware", () => {
  let server: Server | null = null;

  afterEach(async () => {
    await new Promise<void>((resolve) =>
      server ? server.close(() => resolve()) : resolve(),
    );
    server = null;
  });

  async function serve(
    sync: CentralSync,
    handler: (req: express.Request, res: express.Response) => void,
  ) {
    const app = express();
    app.use(express.json());
    app.use(sync.writeThroughMiddleware());
    app.use((req, _res, next) => {
      (req as express.Request & { userId?: string }).userId = "local-alice";
      next();
    });
    app.all("/{*path}", handler);
    server = await new Promise<Server>((resolve) => {
      const s = app.listen(0, "127.0.0.1", () => resolve(s));
    });
    const { port } = server.address() as AddressInfo;
    return (path: string, method = "POST") =>
      fetch(`http://127.0.0.1:${port}${path}`, { method });
  }

  it("forwards a locally created entry to the central server", async () => {
    const { sync, central, local } = createSync();
    const request = await serve(sync, (_req, res) => {
      table(local.tables, "hosts").set("h-new", {
        syncId: "h-new",
        name: "new",
      });
      res.json({ id: 1 });
    });

    const response = await request("/host/db/host");

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ id: 1 });
    expect(central.pushed).toEqual([{ entityType: "hosts", syncId: "h-new" }]);
    expect(table(local.tables, "hosts").has("h-new")).toBe(true);
  });

  it("gives rows created without a syncId one before forwarding", async () => {
    const { sync, central, local } = createSync();
    const request = await serve(sync, (_req, res) => {
      local.unsynced.push({ entityType: "sshFolders", row: { name: "prod" } });
      res.status(201).json({ ok: true });
    });

    expect((await request("/host/folders")).status).toBe(201);
    expect(central.pushed).toEqual([
      { entityType: "sshFolders", syncId: "local-1" },
    ]);
  });

  it("forwards deletions as tombstones", async () => {
    const { sync, central, local } = createSync();
    table(central.tables, "snippets").set("s1", { syncId: "s1", name: "ls" });
    await sync.syncNow();
    const request = await serve(sync, (_req, res) => {
      table(local.tables, "snippets").delete("s1");
      res.json({ success: true });
    });

    expect((await request("/snippets/1", "DELETE")).status).toBe(200);
    expect(central.tombstoned).toEqual([
      { entityType: "snippets", syncId: "s1" },
    ]);
  });

  it("rolls the local change back when the central server refuses it", async () => {
    const { sync, central, local } = createSync();
    central.refuse = "Parent host not found";
    const request = await serve(sync, (_req, res) => {
      table(local.tables, "hosts").set("h-new", { syncId: "h-new" });
      res.json({ id: 1 });
    });

    const response = await request("/host/db/host");

    expect(response.status).toBe(502);
    expect((await response.json()).details).toBe("Parent host not found");
    expect(table(local.tables, "hosts").has("h-new")).toBe(false);
  });

  it("rejects writes up front while the central server is unreachable", async () => {
    const { sync, central, local } = createSync();
    central.unreachable = true;
    let handlerRan = false;
    const request = await serve(sync, (_req, res) => {
      handlerRan = true;
      table(local.tables, "hosts").set("h-new", { syncId: "h-new" });
      res.json({ id: 1 });
    });

    expect((await request("/credentials")).status).toBe(503);
    expect(handlerRan).toBe(false);
  });

  it("does not forward failed requests or unrelated routes", async () => {
    const { sync, central, local } = createSync();
    const request = await serve(sync, (req, res) => {
      table(local.tables, "hosts").set("h-new", { syncId: "h-new" });
      if (req.path.startsWith("/host")) {
        res.status(400).json({ error: "bad" });
      } else {
        res.json({ ok: true });
      }
    });

    expect((await request("/host/db/host")).status).toBe(400);
    expect((await request("/open-tabs")).status).toBe(200);
    expect((await request("/host/db/host", "GET")).status).toBe(400);
    expect(central.pushed).toEqual([]);
  });
});
