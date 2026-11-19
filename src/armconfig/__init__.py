"""ARM configuration from Supabase, Phase 1 (design `docs/design/automated-risk-manager.md` §B9).

Polls, validates and installs accepted configuration documents only; a document can never name a sink or widen the
lockout. Under the read-only lockout (`src/riskmgr/lockout.py`, design §B3); local enable flags never come from here.
"""
