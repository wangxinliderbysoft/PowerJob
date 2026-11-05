# AGENTS.md — base-archtype

Core agent guidance for the `base-archtype` Spring Boot 3.5.16 scaffold.

- Technical details and modules: [Readme.md](./Readme.md)
- Code review checklist: [CR-CHECKLIST.md](./CR-CHECKLIST.md)

For agent work, this document takes precedence over conflicting checklist guidance.
Apply rules proportionally to the task; do not create work merely to satisfy a checklist.

## Quick Navigation

1. [Project Overview](#project-overview)
2. [Compatibility & Impact Analysis](#compatibility--impact-analysis)
3. [Code Quality Rules](#code-quality-rules)
4. [Infrastructure](#infrastructure)
5. [Code Discovery Guide](#code-discovery-guide)
6. [Change Management](#change-management)
7. [Testing](#testing)
8. [Build & Process](#build--process)
9. [Constraints & Definition of Done](#constraints--definition-of-done)
10. [Agent Priority](#agent-priority)

## Project Overview

DerbySoft backend microservice scaffold built on `gps-boot-starter` 2.3.5.

### Baseline

| Component | Value |
|-----------|-------|
| Java | 25 |
| Spring Boot | 3.5.16 |
| Build tool | Maven via `mvnd` (never `mvn`) |
| Packaging | Backend WAR → Tomcat; other modules produce JARs |
| Database | MySQL 8 + Spring Data JPA |
| Testing | Spock 2.5-groovy-6.0 + Groovy 6 + JUnit 5 |
| DerbySoft starter | `gps-boot-starter` 2.3.5 |
| HTTP client | Apache HttpClient 5 |
| JSON | Jackson configured by `JacksonConfig` |
| Logging / code generation | Lombok + Logback |
| Coverage target | JaCoCo: 90% line / 80% branch |
| API documentation | SpringDoc OpenAPI 2.8.13 |

### Responsibility Mapping

| Module | Ownership | Key examples |
|--------|-----------|--------------|
| `common` | Genuinely shared utilities, configuration and infrastructure | `JsonUtils`, `JacksonConfig`, HTTP clients, privacy filters |
| `domain` | Database entities and repositories | `User`, `UserRepository` |
| `test-support` | Reusable test infrastructure | `BaseJsonResourceSpecification`, `@TestSource`, JSON loaders, AST transformers |
| `backend` | Web layer, entry point and profiles | Controllers, `ServletConfig`, `application.properties` |

Keep tests in their owning module's `src/test/java` and application fixtures in
`src/test/resources`; do not publish application-specific fixtures in `test-support`.
Backend/domain depend on test-support with **test scope**. Common tests use Spock
directly to avoid a reactor cycle.

### Engineering Priorities

Correctness → compatibility → minimal scope → maintainability → testability →
established conventions. Preserve backward compatibility with existing consumers.
Do not modernize architecture merely because another approach appears newer.

### Scope Control

- Change only files required by the task; no unrelated refactoring or formatting.
- Preserve names, packages, public APIs and functionality unless their change is
  requested or necessary to satisfy the requirement. Assess compatibility first.
- Do not delete existing functionality unless explicitly requested.
- Database schema changes require explicit authorization. If needed, explain the
  impact and obtain confirmation before modifying mappings that change schema,
  indexes or migrations.
- Add dependencies only for a demonstrated need. Do not upgrade versions,
  change Java/Spring Boot, replace libraries or architecture without an explicit requirement.
- Do not modify unrelated configuration, deployment or CI/CD as incidental cleanup.
- Reuse existing architecture and infrastructure before adding alternatives.
  Changes to shared components require inspection of callers and affected tests.

## Compatibility & Impact Analysis

Before changing behavior, trace affected callers, implementations, tests and
configuration, including jobs, handlers and integrations where relevant.
One edited class does not imply a local impact.

- Identify API/JSON, database/data, configuration, event/message and deployment
  contracts touched by the change. Preserve existing behavior unless requested otherwise.
- Prefer additive changes; explain breaking changes, affected consumers and
  migration requirements before implementation.
- For authorized database changes, assess migrations, backfills, rollout ordering
  and rollback. Follow the repository's existing mechanism; do not invent one.

## Code Quality Rules

### Java 25 Practices

Use features when they improve clarity, not merely because they are newer:

1. Records for immutable DTOs/VOs, not entities or mutable objects.
2. Text blocks for multiline JSON, SQL and regex strings.
3. Switch expressions where clearer than statements.
4. Pattern matching to avoid unnecessary casts.
5. `var` when the inferred type is obvious from the right-hand side.
6. Streams for clear collection transformations.

### Spring & Dependency Injection

- Prefer constructor injection with Lombok `@RequiredArgsConstructor` where appropriate.
- Avoid field injection. Use existing Lombok conventions (`@Slf4j`, `@Getter`, `@Setter`);
  do not manually write boilerplate already handled by Lombok.

### DTO / VO

- Use records for immutable request/response DTOs and VOs; existing location is `common/vo/`.
- Do not expose persistence entities or database-specific structure as REST DTOs.

### API Design & Compatibility

Apply the [impact analysis](#compatibility--impact-analysis) to endpoint consumers,
including generated clients where present.

- Validate untrusted input at the boundary using existing Jakarta Bean Validation
  conventions (`@Valid`, constraints and error responses). Avoid redundant validation
  across layers unless separate entry points or business invariants require it.
- Preserve HTTP statuses, envelopes, error codes, pagination and defaults.
  Review JSON field names, null handling, enums and date/time formats for compatibility.
- REST JSON controller responses, including normal API errors, must use
  `BaseResponse<T>` as the response body. Use the existing `BaseResponse.success(...)`,
  `successPageData(...)` and `fail(...)` factories; centralize exception translation
  in the existing `RestExceptionAdvice` rather than returning a different error shape.
  This rule excludes raw file responses, direct servlet output, downloads/streams and
  client-disconnect cases where a response body cannot be written.
- For new endpoints with potentially large collections, define bounded pagination.
  For existing endpoints, preserve their contract; propose limit changes for approval,
  rather than silently truncating results or adding new rejection behavior.
- Consider duplicate requests for state-changing APIs. Add idempotency controls only
  when required by the business contract; do not assume retries are harmless.
- Keep maintained OpenAPI documentation and examples consistent with the contract.
  See [Testing](#testing) for applicable validation and compatibility cases.

### Entity & Repository

- Normal auditable entities extend `BaseTimeAuditable`; it supplies `createdTime`
  and `lastModifyTime` as UTC `Instant` values. Do not duplicate audit fields.
- Preserve entity mappings, lifecycle and naming conventions.
- Use Spring Data JPA; prefer readable derived queries, then `@Query`.
  Use native SQL only when necessary.

### JPA Query Performance & Data Access

- Inspect relationships and result cardinality. Account for duplicate roots from
  collection joins in selection, counts, sorting and pagination.
- Address demonstrated N+1 problems with focused queries, projections, entity graphs
  or established fetch strategies; do not globally enable eager loading.
- Do not paginate collection `JOIN FETCH` without verifying SQL and result semantics.
  Use an established alternative, such as paging root IDs then fetching required data.
- Load only necessary data; bound large reads with pagination, streaming or batches.
  Preserve existing contracts under the API compatibility rule.
- Before bulk updates/deletes, assess persistence-context synchronization, lifecycle
  callbacks, auditing and optimistic-lock semantics.
- Base material performance changes on SQL, query counts, execution plans or measurements.
  Assess indexes against actual filters/joins/sorts, write overhead and migration impact;
  follow schema authorization rules.

### Logging

- Use `@Slf4j` and parameterized messages (`log.info("msg: {}", value)`), not concatenation.
- Never log passwords, tokens, secrets, payment data or PII; retain sanitization.
- Use `error` for investigation-worthy failures, `warn` for abnormal/recoverable
  conditions, `info` for important events and `debug` for diagnostics.

### Exception Handling

- Use existing exceptions and `BaseResponse.success(data)`, `successPageData(page)`
  or `fail(errorCode, message)`. `RestExceptionAdvice` centrally translates uncaught exceptions.
- Do not introduce a parallel exception hierarchy or generic `RuntimeException` when
  a meaningful existing exception applies.
- Do not catch merely to rethrow unchanged, swallow failures or return success-shaped defaults.
- Error messages must not expose sensitive SQL values, tokens, credentials or stack traces.

### Transactions, Concurrency & Async Work

- Place transactions at the appropriate application/service boundary; understand
  propagation, rollback and read/write semantics before changing them.
- Keep transactions short. Avoid slow external work inside them unless consistency
  requires it; see [HTTP reliability](#external-http-reliability--business-consistency).
- Identify concurrent writers across requests, jobs, callbacks and async handlers.
  A check followed by an update is not automatically atomic.
- Choose constraints, conditional updates or locking based on the conflict scope.
  Consider optimistic locking (`@Version`) when conflicts must be detected; do not
  assume this scaffold already has a version-field convention. New version columns
  require schema authorization. Use pessimistic locking only when justified.
- Define lock duration, timeouts and conflict outcomes; never add locking mechanically.
  Protect overlapping jobs or duplicate delivery when required, reusing available
  unique-key, idempotency, distributed-lock or state-transition mechanisms.
- Do not share mutable entities or persistence contexts across threads. Pass immutable
  identifiers/DTOs and load/update within the owning transaction.
- Shared mutable state requires an explicit synchronization and lifecycle strategy.
  Reuse existing executors/scheduling; do not create unmanaged threads or executors.
- Async work needs error handling, observability, shutdown and context propagation
  where required. `@Transactional` does not cross newly created thread boundaries automatically.
- Follow existing conflict retry/error policies. Never blindly retry a workflow
  containing non-idempotent external side effects.

### Design Necessity, SRP & Duplication

Check project code, Spring and the JDK before creating abstractions.

- Separate independent business reasons to change, not words such as "and" or step counts.
- Services may orchestrate validation, persistence and calls. Simple validation,
  formatting or logging does not each require a class. Controllers remain HTTP adapters.
- Share constants/logic only when meanings and change reasons align; similar syntax
  or two occurrences alone do not justify coupling.
- Prefer local code or private helpers before new classes. Interfaces, strategies
  and layers need a current contract, variation or architectural boundary.
- Every new class needs a clear responsibility and a reason an existing class/helper
  cannot handle it. Avoid pass-through wrappers and speculative frameworks.
- Judge anonymous classes by complexity/readability, not line count; use suitable
  lambdas or named classes when warranted.
- Refactor duplication introduced or directly affected by this task, not the entire repository.

**Design gate:** If a simpler implementation satisfies the same requirement, prefer it.
Otherwise identify the current need for the extra abstraction; future extensibility alone is insufficient.

## Infrastructure

### HTTP Clients

- Reuse `ProviderHttpClientManager`, `WalletClient`, `ProviderProperties` and
  `PoolProperties`; preserve shared pooling, configuration and shutdown behavior.
- Do not introduce OkHttp, WebClient, RestTemplate or JDK HttpClient without an explicit requirement.
- Convert failures through the existing exception model and release closeable resources appropriately.

See [ProviderHttpClientManager](./common/src/main/java/com/derbysoft/gps/backend/app/common/http/ProviderHttpClientManager.java).

### External HTTP Reliability & Business Consistency

- Configure finite connection-request, connection and response timeouts using the
  shared infrastructure; choose values against the business deadline and downstream behavior.
- Distinguish pool/connection/response timeouts, TLS failures, HTTP errors and parsing failures.
  A timeout does not prove the remote operation was not processed.
- Retry only plausibly transient failures when repetition is safe. Establish the
  contract, request ID or deduplication/reconciliation mechanism before retrying
  side-effecting calls such as payment, booking or cancellation.
- Bound attempts and backoff by the remaining deadline; avoid nested retries and retry storms.
- Distinguish HTTP success from business success; parse and validate the provider response.
- Preserve non-sensitive correlation IDs for reconciliation, without bypassing logging rules.
- Local transactions cannot roll back remote side effects. Coordinate through existing
  state transitions, callbacks, reconciliation or compensation where required.
- Define uncertain outcomes explicitly: reconcile/query, await callback, mark pending,
  retry safely or escalate. Do not invent a parallel workflow.

### Dependencies

- Prefer the JDK, Spring Boot and existing dependencies over new libraries.
- Do not override managed versions or upgrade Spring Boot, `gps-boot-starter`,
  Jackson, Hibernate, Groovy, Spock or HttpClient without a specific requirement.
- Necessary additions must be compatible with Java 25 and Spring Boot 3.5.16.

### Configuration & Profiles

Maven profiles: `dev` (default), `qa`, `uat`, `prod`. Checked-in backend
configuration directories: `dev`, `uat`, `prod`.
The selected profile sets `${env}` and packages `backend/src/main/profiles/${env}/`;
this is build-time resource selection, not a runtime Spring profile switch.

Prod requires runtime variables: `APP_NAME`, `REGION`, `SGC_SERVICE_ID`,
`LOGGING_TOPIC_PERF`, `LOGGING_TOPIC_STREAM`, `LOG_PATH`, `DATABASE_URL`,
`DATABASE_USERNAME`, `DATABASE_PASSWORD`. No UAT addresses or credential defaults.
`LOG_PATH` must be writable; MySQL TLS must use a trusted certificate matching
the JDBC hostname (`sslMode=VERIFY_IDENTITY`). Provision the schema before startup:
prod validates mappings, does not generate/update schema and disables SQL output.

- Keep environment-specific values in profile/environment configuration, not code.
- Never commit credentials or secrets.
- Prefer strongly typed `@ConfigurationProperties` for grouped settings.

See [Readme.md](./Readme.md) for auto-wired URLs and `env`/region behavior.

## Code Discovery Guide

Inspect relevant implementations, callers, tests, configuration and reusable infrastructure
before adding code. An unsuccessful search in one directory does not establish absence.

| Pattern | Location | Key examples |
|---------|----------|--------------|
| HTTP | `common/src/main/java/.../app/common/http/` | `ProviderHttpClientManager`, `WalletClient` |
| Persistence | `domain/src/main/java/.../app/db/` | Entities, repositories |
| JSON | `common/src/main/java/.../app/common/utils/`, `.../app/config/` | `JsonUtils`, `JacksonConfig` |
| Privacy | `common/src/main/java/.../app/common/privacy/` | `SensitiveMessageProcesser` |
| Errors / responses | `backend/src/main/java/.../app/web/`, `common/src/main/java/.../app/common/vo/` | `RestExceptionAdvice`, `BaseResponse` |
| DTOs / VOs | `common/src/main/java/.../app/common/vo/` | Records by subpackage |
| Test infrastructure | `test-support/src/main/java/.../app/config/` | `BaseJsonResourceSpecification`, `@TestSource` |

## Change Management

Use one proportional workflow; routine tasks need no separate planning documents
or ceremonial reports.

1. **Understand:** Define requested behavior, module ownership, compatibility boundaries
   and observable acceptance criteria, including relevant normal, boundary and failure cases.
2. **Inspect:** Follow the discovery guide and impact analysis.
3. **Plan:** Choose the smallest change and apply the design gate. Resolve material
   ambiguity, without blocking routine work with unnecessary questions.
4. **Implement:** Follow the applicable code/infrastructure rules within scope.
5. **Test & validate:** Use [Testing](#testing) and [Validation Strategy](#validation-strategy);
   confirm intended tests actually execute.
6. **Review & report:** Review the complete diff, then report actual changes,
   commands/results and material limitations under the completion criteria.

## Testing

### Test Frameworks & Naming

- Prefer Spock for new logic-heavy tests; use JUnit 5 where existing tests/integration use it.
- Use `*Spec.groovy` for Spock and `*Test.java` for JUnit.
- Follow project conventions across all components; choose unit/integration scope
  based on the behavior, not a separate standard for each component type.
- Mirror production packages under the owning module's `src/test/java`.
  Keep application fixtures under its `src/test/resources`.

### Spock Conventions

- No unnecessary `public`/`private` modifiers or semicolons.
- Use `given / when / then`, `expect` and `where` as appropriate.
- No `assert` in `then:`/`expect:`; assertions are automatic.
- Use `@Unroll` when data rows should be reported separately.
- Stub with `>>`; verify relevant interactions with `1 *` or forbidden calls with `0 *`.

### JSON Utilities in Tests

Never call a static `JsonUtils.setObjectMapper(...)`. Instantiate directly:

```groovy
new JsonUtils(new JacksonConfig().jackson2ObjectMapperBuilder())
```

### Test Isolation & Determinism

- Tests must pass alone and in the relevant suite, independent of execution order.
- Do not depend on shared mutable leftovers, developer paths, local services, uncontrolled
  time/randomness or network availability, except in explicitly isolated integration tests.
- Control collaborators, time and randomness through existing test patterns.
  Isolate persistence data and clean up test-owned data as required.
- Never modify shared development, staging or production data.
- No sleeps to mask async races; use deterministic synchronization and bounded timeouts.
- Use real framework/database/serialization/HTTP integration where that behavior is
  part of the requirement. Do not mock away what the test is intended to prove.

### Test Quality & Coverage

- Add focused tests for changed behavior and relevant risks; bug fixes need a regression
  case demonstrating the original defect. Do not restate implementation or test trivial accessors.
- Select relevant cases rather than mechanically testing every checklist item:
  API validation/shape/status/compatibility, query cardinality/counts/sorting/pagination,
  concurrency conflicts, or HTTP transient/permanent failures, uncertain timeouts,
  malformed responses and duplicate delivery.
- Do not delete failing tests, weaken assertions, lower thresholds or bypass required checks.
- Surefire discovers `*Test.java`, `*Test.groovy`, `*Spec.java`, `*Spec.groovy`;
  zero selected tests is not verification.
- Coverage targets are 90% line / 80% branch. JaCoCo lives in root `pluginManagement`;
  thresholds apply only when activated. `mvnd verify` success alone does not prove coverage.

## Build & Process

### Build Commands

Use `mvnd`, not `mvn`. Set `JAVA_HOME` and `PATH` to JDK 25 first.

```bash
mvnd test                          # unit tests
mvnd verify                        # tests + packaging; coverage only if activated
mvnd clean install                 # full reactor build and install
mvnd clean install -Puat            # UAT configuration
mvnd clean install -Pprod           # production configuration
mvnd -pl backend -am test           # backend + upstream tests
mvnd -pl common -am test            # common + upstream tests
```

For a single test, install required upstream artifacts, then select only its owning module:

```bash
mvnd -pl test-support -am install
mvnd -pl backend test -Dtest=UserControllerSpec
```

### Validation Strategy

**Verify the requirement, not a proxy.** Command availability, keyword counts,
link syntax or build success do not prove behavior or checks that never ran.

- Run the smallest existing test/build command covering the change; confirm selection/results.
- Prefer `verify` or `clean install` for configuration, dependencies, multiple modules,
  JPA/HTTP infrastructure or build behavior.
- Verify configuration binding/runtime behavior, not just property presence.
- Documentation-only changes need actual link targets, anchors and formatting checks;
  no application build unless documentation tests require it.
- Use existing linters/formatters. Broaden checks only for scope, failures or unresolved risks.
- Investigate failures and fix task-related causes; never hide failures or disable checks.

**Evidence gate:** Match each acceptance criterion to the executed check and observed
result. Report blocked/unrun checks; disclosure does not substitute for verification.

### Operational & Destructive-Action Safety

- Destructive operations and business/operational side effects require explicit
  authorization for the action and environment.
- Do not run destructive SQL, schema resets, data deletion or bulk updates against
  shared/non-test environments without that authorization.
- Do not deploy, publish remotely, send real emails, invoke side-effecting business
  APIs or trigger production workflows without authorization.
- Do not incidentally change CI/CD, infrastructure, environment variables, secrets,
  deployment or production configuration during application work.
- Routine dependency downloads, local artifact installation, task-related read-only
  queries and isolated integration tests may run within the authorized task/environment.
  This does not authorize production calls or sending private code/secrets to third parties.
- Prefer test/mock endpoints and isolated data. If authorization is missing, explain
  the impact and obtain confirmation before acting.

### Git Workflow

- Inspect with `git status`, `git diff`, `git log`, `git branch`.
- Inspect and preserve existing changes before editing.
- Do not reset/discard user changes, force-push, rewrite history, delete branches
  or change remotes. Do not commit or push unless explicitly requested.

## Constraints & Definition of Done

The [scope](#scope-control), [design](#design-necessity-srp--duplication) and
[operational safety](#operational--destructive-action-safety) rules apply throughout.
Do not replace JPA, Apache HttpClient 5 or Spock/JUnit without an explicit requirement;
do not introduce parallel utilities, response formats or infrastructure.

### Definition of Done

A task is complete only when applicable criteria hold:

- Requested behavior meets its acceptance criteria with validation evidence.
- Design, module ownership, conventions and compatibility are preserved;
  relevant migration, transaction, concurrency and lifecycle concerns are addressed.
- Relevant tests ran and passed, required validation succeeded, and coverage checks
  were not weakened/bypassed. Do not claim coverage if enforcement was not activated.
- The complete final diff contains no unrelated edits, unjustified abstractions,
  incorrectly shared logic, unnecessary dependencies, secrets, debug code,
  unintended TODOs or accidental API/configuration changes.
- Operational actions stayed within authorization. Report what changed and why,
  actual commands/results and material limitations concisely, without repeated summaries.

**Implementation finished is not task complete.** If required validation is blocked,
unrun or failing, report "implementation finished; verification pending" with the
reason and outstanding checks. Do not claim unconditional completion.

### Documentation Rules

Update docs for changes to API behavior, configuration, architecture, operations or
developer workflow, not merely to describe implementation details.
Do not add classes, abstractions, tests, docs or checks solely to satisfy checklist wording.

## Agent Priority

1. Explicit user request, subject to safety and higher-level instructions.
2. This `AGENTS.md` for project-specific rules.
3. Existing architecture and established conventions.
4. Existing implementations when consistent with the above.
5. General Java/Spring best practices.

Existing code is evidence, not permission to reproduce defects.
When uncertain, inspect first, prefer the smallest compatible change, avoid unrelated
work and ask only when a material requirement remains ambiguous.

- **Version:** v0.6.0
- **Spring Boot:** 3.5.16
- **Java:** 25
- **gps-boot-starter:** 2.3.5
- **Last Updated:** October 2026
