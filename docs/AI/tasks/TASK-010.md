# TASK-010: Method Caching Decorators (`@Cacheable`, `@CacheClear`) for Cloudflare Workers

## Objective
Implement native TC39 decorators (`@Cacheable`, `@CacheClear`) tailored for Cloudflare Workers and Durable Objects, enabling zero-boilerplate caching on class methods (e.g. expensive external API fetches or heavy D1 queries).

## Scope
- Implement `src/decorators/cache-key.ts`: Stable cache key generator hashing class name, method name, and serialized arguments.
- Implement `src/decorators/cacheable.ts`: `@Cacheable({ ttlMs, refreshThreshold, keyBuilder })` decorator delegating to `Cache.wrap()`.
- Implement `src/decorators/cache-clear.ts`: `@CacheClear({ patterns })` decorator for invalidating cache entries upon method execution.
- Implement tests verifying standard TC39 decorator semantics in TypeScript without requiring `experimentalDecorators`.

## Allowed Files
- `src/decorators/cache-key.ts`
- `src/decorators/cacheable.ts`
- `src/decorators/cache-clear.ts`
- `test/decorators/cacheable.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-010.md`

## Dependencies
- TASK-009 (Multi-Tier Caching System)

## Inputs and Outputs
- **Inputs**: Class method and `@Cacheable` decorator annotations.
- **Outputs**: Transparently cached method invocations.

## Acceptance Criteria
- [x] Method annotated with `@Cacheable()` executes underlying logic once on first call, then returns cached value on subsequent calls.
- [x] Argument variations generate distinct cache keys.
- [x] Calling `@CacheClear()` purges the associated cache key.
- [x] Tests pass in Vitest.

## Verification Commands
```bash
pnpm test test/decorators/
pnpm typecheck
```

## Risks and Assumptions
- Use standard TC39 decorators (Stage 3 / TS 5.x native) instead of legacy TypeScript experimental decorators.

## Status
DONE

