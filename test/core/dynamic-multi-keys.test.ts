import { describe, it, expect, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { KVDB, type MultiKeySchema } from "../../src/index.js";

interface BlockData {
  miner: string;
  txCount: number;
}

type BlockKeys = {
  chainId: string;
  hash: string;
  gasUsed: number;
  status?: string;
  [key: string]: unknown;
};

describe("Dynamic Multi-Keys & Schema Evolution (Example 16 Parity)", () => {
  let d1: D1Database;
  let db: KVDB;

  const schema: MultiKeySchema<BlockKeys, "integer"> = {
    primaryKey: { name: "blockNumber", type: "integer" },
    keys: {
      chainId: { type: "string", index: true },
      hash: { type: "string", index: { unique: true } },
      gasUsed: { type: "number" },
    },
  };

  beforeEach(() => {
    d1 = createMockD1Database();
    db = new KVDB({ d1 });
  });

  it("faithfully executes the exact workflow from example 16-dynamic-multi-keys.ts", async () => {
    const blocks = db.table<BlockData, BlockKeys>("blocks", { schema });

    // 1. Writing initial block records with secondary keys
    await blocks.set(1001, { miner: "0xpoolA", txCount: 42 }, {
      keys: {
        chainId: "ethereum",
        hash: "0xabc1",
        gasUsed: 21000,
      },
    });

    await blocks.set(1002, { miner: "0xpoolB", txCount: 15 }, {
      keys: {
        chainId: "ethereum",
        hash: "0xabc2",
        gasUsed: 45000,
      },
    });

    await blocks.set(1003, { miner: "0xpoolA", txCount: 99 }, {
      keys: {
        chainId: "polygon",
        hash: "0xabc3",
        gasUsed: 30000,
      },
    });

    // 2. Point lookup by Primary Key (Integer)
    const b1 = await blocks.get(1001);
    expect(b1).toEqual({ miner: "0xpoolA", txCount: 42 });

    const rec1 = await blocks.getRecord(1001);
    expect(rec1).not.toBeNull();
    expect(rec1?.key).toBe(1001);
    expect(rec1?.columns.chainId).toBe("ethereum");
    expect(rec1?.columns.hash).toBe("0xabc1");
    expect(rec1?.columns.gasUsed).toBe(21000);
    expect(rec1?.value).toEqual({ miner: "0xpoolA", txCount: 42 });

    // 3. Fast O(1) point lookup by indexed secondary key
    const byHash = await blocks.getBy("hash", "0xabc2");
    expect(byHash).not.toBeNull();
    expect(byHash?.key).toBe(1002);
    expect(byHash?.columns.hash).toBe("0xabc2");
    expect(byHash?.columns.chainId).toBe("ethereum");
    expect(byHash?.value).toEqual({ miner: "0xpoolB", txCount: 15 });

    // 4. Dynamic Schema Evolution: add a new key and a composite index on the fly
    await blocks.addKey("status", { type: "string", default: "finalized", index: true });
    await blocks.addIndex({ name: "chain_status_idx", keys: ["chainId", "status"] });

    // Write a new record using the dynamically added key
    await blocks.set(1004, { miner: "0xpoolC", txCount: 1 }, {
      keys: {
        chainId: "ethereum",
        hash: "0xabc4",
        gasUsed: 12000,
        status: "pending",
      },
    });

    // Existing records automatically receive the default value:
    const oldRec = await blocks.getRecord(1001);
    expect(oldRec?.columns.status).toBe("finalized");

    const newRec = await blocks.getRecord(1004);
    expect(newRec?.columns.status).toBe("pending");

    // 5. Query targeting declared keys directly (automatically index-accelerated)
    const queryResults = await blocks.find({
      where: {
        chainId: "ethereum",
        gasUsed: { $gte: 20000 },
      },
      sort: [{ path: "gasUsed", direction: "desc" }],
      limit: 2,
    });

    expect(queryResults).toHaveLength(2);
    expect(queryResults[0].key).toBe(1002);
    expect(queryResults[0].value).toEqual({ miner: "0xpoolB", txCount: 15 });
    expect(queryResults[1].key).toBe(1001);
    expect(queryResults[1].value).toEqual({ miner: "0xpoolA", txCount: 42 });

    // 6. Find full physical records
    const physicalResults = await blocks.findRecords({
      where: { status: "pending" },
    });

    expect(physicalResults).toHaveLength(1);
    expect(physicalResults[0]!.key).toBe(1004);
    expect(physicalResults[0]!.columns.hash).toBe("0xabc4");
    expect(physicalResults[0]!.columns.status).toBe("pending");
    expect(physicalResults[0]!.value).toEqual({ miner: "0xpoolC", txCount: 1 });

    await db.close();
  });

  it("handles write queue (WriteBatcher) SQL aggregation with dynamic multi-keys", async () => {
    const blocks = db.table<BlockData, BlockKeys>("blocks_batched", {
      schema,
      autoBatch: { maxBatchSize: 10, maxWaitMs: 100 },
    });

    // Concurrently enqueue writes into the micro-batch queue
    const p1 = blocks.set(2001, { miner: "0xpoolA", txCount: 10 }, {
      keys: { chainId: "arbitrum", hash: "0xarb1", gasUsed: 5000 },
    });
    const p2 = blocks.set(2002, { miner: "0xpoolB", txCount: 20 }, {
      keys: { chainId: "arbitrum", hash: "0xarb2", gasUsed: 6000 },
    });

    // Enqueue an update to 2001 with partial secondary keys (testing key coalesce)
    const p3 = blocks.set(2001, { miner: "0xpoolA_updated", txCount: 11 }, {
      keys: { gasUsed: 5200 },
    });

    expect(blocks.pendingBatchCount).toBe(2); // 2001 and 2002 (2001 coalesced)

    // Flush batch queue to D1 SQL
    await blocks.flush();
    await Promise.all([p1, p2, p3]);

    expect(blocks.pendingBatchCount).toBe(0);

    // Verify 2001 has coalesced secondary keys preserved (chainId and hash still present, gasUsed updated)
    const rec2001 = await blocks.getRecord(2001);
    expect(rec2001).not.toBeNull();
    expect(rec2001?.columns.chainId).toBe("arbitrum");
    expect(rec2001?.columns.hash).toBe("0xarb1");
    expect(rec2001?.columns.gasUsed).toBe(5200);
    expect(rec2001?.value).toEqual({ miner: "0xpoolA_updated", txCount: 11 });

    // Verify 2002 exists
    const rec2002 = await blocks.getRecord(2002);
    expect(rec2002).not.toBeNull();
    expect(rec2002?.columns.chainId).toBe("arbitrum");
    expect(rec2002?.columns.gasUsed).toBe(6000);
  });

  it("safely chunks large batch insertions (120 items) into 50-item D1 statement batches", async () => {
    const blocks = db.table<BlockData, BlockKeys>("blocks_large_batch", { schema });

    const entries = Array.from({ length: 120 }, (_, i) => ({
      key: 3000 + i,
      value: { miner: `0xminer_${i}`, txCount: i },
      keys: {
        chainId: i % 2 === 0 ? "optimism" : "base",
        hash: `0xhash_${i}`,
        gasUsed: 10000 + i,
      },
    }));

    // setMany should execute in 50-chunk batches without throwing
    await blocks.setMany(entries);

    // Verify first, middle, and last records exist
    const first = await blocks.getRecord(3000);
    expect(first?.columns.hash).toBe("0xhash_0");

    const mid = await blocks.getRecord(3055);
    expect(mid?.columns.hash).toBe("0xhash_55");

    const last = await blocks.getRecord(3119);
    expect(last?.columns.hash).toBe("0xhash_119");

    // Keyset query
    const optimismBlocks = await blocks.find({
      where: { chainId: "optimism" },
      limit: 5,
    });
    expect(optimismBlocks).toHaveLength(5);
  });

  it("automatically flushes write queue before executing schema evolution (addKey / addIndex)", async () => {
    const blocks = db.table<BlockData, BlockKeys>("blocks_evolve_flush", {
      schema,
      autoBatch: { maxBatchSize: 50, maxWaitMs: 5000 },
    });

    // Enqueue write
    void blocks.set(4001, { miner: "0xpoolE", txCount: 5 }, {
      keys: { chainId: "avalanche", hash: "0xavax1", gasUsed: 15000 },
    });

    expect(blocks.pendingBatchCount).toBe(1);

    // Calling addKey automatically flushes pending writes prior to ALTER TABLE DDL
    await blocks.addKey("extraData", { type: "string", default: "none" });

    expect(blocks.pendingBatchCount).toBe(0);

    const rec = await blocks.getRecord(4001);
    expect(rec).not.toBeNull();
    expect(rec?.columns.extraData).toBe("none");
  });
});
