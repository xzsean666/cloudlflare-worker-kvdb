import {
  CloudflareKVDB,
  TieredCache,
} from "cloudflare-worker-kvdb";

export interface Env {
  DB: D1Database;
  CACHE_KV: KVNamespace;
  BLOBS: R2Bucket;
}

interface Article {
  id: string;
  title: string;
  category: string;
  content: string;
  views: number;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // Initialize KVDB client with D1 and R2 transparent overflow (>64KB offloaded to R2)
    const kvdb = new CloudflareKVDB({
      d1: env.DB,
      r2: env.BLOBS,
    });

    // Initialize Multi-Tier Cache: L1 Worker isolate memory (<0.05ms) -> L2 Workers KV (10ms) -> L3 D1
    const cache = new TieredCache({
      l1: { max: 1000 },
      l2: { namespace: env.CACHE_KV },
      ctx,
    });

    // Access physical schema table with indexed category column
    const articles = kvdb.table<Article>("articles", {
      schema: {
        tableName: "t_articles",
        primaryKey: { name: "id" },
        columns: {
          category: { type: "string", index: true },
        },
      },
      overflowThresholdBytes: 64 * 1024, // 64 KB R2 overflow threshold
    });

    await articles.init();

    // Route: POST /articles - Create or update an article
    if (request.method === "POST" && url.pathname === "/articles") {
      const body = (await request.json()) as Article;
      if (!body.id || !body.title) {
        return new Response("Missing id or title", { status: 400 });
      }

      await articles.set(body.id, body);
      // Invalidate cache
      await cache.delete(`article:${body.id}`);

      return Response.json({ success: true, article: body }, { status: 201 });
    }

    // Route: GET /articles/:id - Read article through multi-tier cache
    const articleMatch = url.pathname.match(/^\/articles\/([a-zA-Z0-9_-]+)$/);
    if (request.method === "GET" && articleMatch) {
      const id = articleMatch[1]!;

      // Read-Through Cache with L1 Memory -> L2 KV -> D1 fallback
      const article = await cache.wrap(
        `article:${id}`,
        async () => {
          return await articles.get(id);
        },
        { ttlMs: 300_000, swrMs: 60_000 }
      );

      if (!article) {
        return new Response("Article not found", { status: 404 });
      }

      return Response.json(article);
    }

    // Route: GET /articles?category=tech - Filter using native SQL compiler
    if (request.method === "GET" && url.pathname === "/articles") {
      const category = url.searchParams.get("category");
      const results = await articles.find(
        category ? { category } : undefined,
        { limit: 20 }
      );
      return Response.json({ articles: results });
    }

    // Route: DELETE /articles/:id - Delete article and clean up R2 blob
    if (request.method === "DELETE" && articleMatch) {
      const id = articleMatch[1]!;
      const deleted = await articles.delete(id);
      await cache.delete(`article:${id}`);
      return Response.json({ deleted });
    }

    return new Response("Not Found", { status: 404 });
  },
};
