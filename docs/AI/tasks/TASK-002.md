# TASK-002: Project Scaffolding, TypeScript Configuration, and Cloudflare Test Environment Setup

## Objective
Initialize the repository's build system, TypeScript configuration (`tsconfig.json`), package manifest (`package.json`), and Vitest testing environment tailored for Cloudflare Workers (using Miniflare / `@cloudflare/vitest-pool-workers`).

## Scope
- Create `package.json` with scripts (`build`, `test`, `typecheck`, `lint`).
- Create `tsconfig.json` with strict mode, ES2022/ESNext target, and `@cloudflare/workers-types`.
- Create `tsup.config.ts` or `vite.config.ts` for bundling ESM package artifacts.
- Configure `vitest.config.ts` with Cloudflare Workers test pool / Miniflare environment for local SQLite/KV/R2 mocks.
- Create a smoke test verifying the test runner executes inside the simulated Cloudflare Workers environment.

## Allowed Files
- `package.json`
- `tsconfig.json`
- `tsup.config.ts` or `vite.config.ts`
- `vitest.config.ts`
- `wrangler.jsonc` (test runner config)
- `test/smoke.test.ts`
- `src/index.ts` (stub export)
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-002.md`

## Dependencies
- TASK-001 (Architecture and documentation established)

## Inputs and Outputs
- **Inputs**: Cloudflare Workers runtime requirements and modern build tooling.
- **Outputs**: Compiling TypeScript project with working Vitest execution environment.

## Acceptance Criteria
- [x] `pnpm install` succeeds without errors.
- [x] `pnpm typecheck` runs without errors.
- [x] `pnpm test` runs the smoke test and passes in the Cloudflare simulated environment.
- [x] `pnpm build` produces clean ESM output in `dist/`.

## Verification Commands
```bash
pnpm install
pnpm typecheck
pnpm test
pnpm build
```

## Risks and Assumptions
- Package dependencies must be compatible with Node 20+ and pnpm.
- `@cloudflare/workers-types` must be configured in `tsconfig.json` without conflicting with DOM or Node types.

## Status
DONE

