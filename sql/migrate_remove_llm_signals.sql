-- =============================================================================
-- Migration: Remove LLM signals table
-- =============================================================================
-- The automated signal pipeline has been replaced by an interactive LLM
-- chatbox. This table is no longer written to or read from.
-- =============================================================================

DROP TABLE IF EXISTS crypto_db.llm_signals;
