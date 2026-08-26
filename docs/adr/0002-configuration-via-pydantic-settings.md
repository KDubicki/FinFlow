# 2. Configuration via a single Pydantic Settings object

Date: 2026-08-26

## Status

Accepted

## Context

Data pipelines accumulate configuration: credentials for several sources, paths,
timeouts, retry counts, feature flags. The default habit is to call `os.getenv`
where the value is needed. That scatters the configuration surface across the
codebase, so nobody can answer "what does this system read from the
environment?" without grepping, and misconfiguration surfaces deep inside a
pipeline run rather than at startup.

Credentials additionally risk ending up in logs or exception messages, since a
plain string is printed by any `repr()` of the object holding it.

## Decision

All configuration is declared in one `Settings` class in `finflow.config`, built
on `pydantic-settings`:

- Every variable uses the `FINFLOW_` prefix and is documented in `.env.example`.
- Credentials are typed `SecretStr`, so they are redacted in `repr` and logs.
- The object is `frozen=True` — configuration cannot mutate mid-run.
- Validation constraints live on the fields, so bad values fail at construction.
- Credentials are `None` by default, and a component needing one calls
  `settings.require(...)`, which raises an error naming the missing variable.

Modules accept a `Settings` instance as an argument rather than importing a
global, which keeps them testable without patching the environment.

## Consequences

- The complete configuration surface is readable in one file.
- Misconfiguration fails at startup with an actionable message, not mid-pipeline.
- Secrets do not leak through `repr`, logs or tracebacks. A test asserts this.
- Tests construct `Settings` directly with `_env_file=None`, so a developer's
  real `.env` can never influence a test result.
- Slight indirection: adding a variable means editing two files, the settings
  class and `.env.example`. That is deliberate — it keeps the documentation
  honest.
