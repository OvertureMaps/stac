# Publishing architecture

`stac.overturemaps.org` used to come out of an Airflow DAG in `tf-data-platform`, `release_publish_stac_dag`, which meant the catalog's build logic lived in a different repo than the code that defined the catalog format. This repo is now self-contained: `publish-catalog.yaml` builds the catalog with this repo's own `overture-stac` CLI, validates it, and publishes it, all without leaving GitHub Actions.

## Data flow

A run has three stages, always in this order:

1. `gen-stac --output public_releases` walks every release currently in the public registry bucket and writes the catalog to a working directory.
2. `stac-check-action` validates the result against the STAC spec before anything gets published, using `fast-linting` for speed since this runs on the schedule.
3. The validated catalog is synced to S3 and the CDN cache in front of it is invalidated.

The build stage runs unauthenticated: it only reads public data and needs no AWS credentials. Publishing is where the workflow needs to touch two separate AWS accounts, which is the part worth understanding before changing anything here.

## Why two AWS accounts

The bucket that serves `stac.overturemaps.org` and the CloudFront distribution in front of it live in different AWS accounts, split along Overture's existing account boundaries rather than anything specific to this workflow:

- The distribution account (`913550007193`) owns `overturemaps-extras-us-west-2`, the S3 bucket the catalog is synced into under the `stac/` prefix.
- The core-data account (`763944545891`) owns the CloudFront distribution ("Overture STAC Index", ID `E209PEWSOCNO5D`) that fronts `stac.overturemaps.org` and caches what's in that bucket.

A single IAM role in one account can't reach into the other, so the workflow authenticates twice per publish: once to push the catalog to S3, once to bust the CDN cache.

## OIDC and role chaining

The workflow never holds long-lived AWS credentials. Each publish run starts by assuming `stac-publish-oidc-overturemaps` in the distribution account via GitHub's OIDC provider; that role's trust policy is scoped to this repo, keyed off `ref:refs/heads/main` for the scheduled trigger and `environment:manual-publish` for manual dispatch, so a fork or an unrelated branch can't assume it.

Busting the CloudFront cache means getting into the core-data account from a role that only exists in the distribution account, which is where role chaining comes in: `configure-aws-credentials` is called a second time with `role-chaining: true`, using the credentials from the first assume-role to assume `cloudfront-invalidator` in the core-data account. That role's trust policy lists `stac-publish-oidc-overturemaps` as a principal, alongside the legacy `mwaa-executor` role left over from the Airflow DAG (tracked for removal once the DAG is decommissioned).

## Triggers and gating

`publish-catalog.yaml` runs on a `schedule` (every 6 hours) and on `workflow_dispatch`. Both triggers share one `publish` job; what differs is the job's `environment`, set via `${{ github.event_name == 'workflow_dispatch' && 'manual-publish' || '' }}`. An empty string evaluates to no environment, so the scheduled run publishes straight through, while a manual dispatch is gated behind the `manual-publish` environment's required reviewer approval. That's also the mechanism the OIDC trust policy keys off: the scheduled and manual paths present different claims to AWS, which is why the environment gate and the trust policy have to agree on the same environment name.
