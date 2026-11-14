"""Non-blocking pub-sub ingestion (dto-contracts.md): HLC, EventBus, DlqSpooler, SM4, orchestrator, consumers.

Only the IngestionOrchestrator publishes to the four topics; persistence goes through src.ports.
"""
