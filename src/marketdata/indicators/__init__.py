"""Indicators of the market-data layer (design §A6; plan S18): the `Indicator` Protocol (`base`), the registry keyed
`(KIND, VERSION)` (`registry`), the canonical ATR (`atr`), the `md-indicators` job runner (`engine`) and the state
checkpoints (`checkpoint`). Values live in `redis-md` only, never in PostgreSQL."""
