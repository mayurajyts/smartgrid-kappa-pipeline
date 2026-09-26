# Serving schema

Numbered, forward-only migrations for the serving store (plan section 6.5): 001_schema.sql, 002_indexes.sql, 003_views.sql. Applied in Phase 4. The version/is_current columns on household_billing_daily are what make bill restatement (R7) atomic and auditable.
