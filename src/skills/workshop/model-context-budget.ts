const DEFAULT_MODEL_CONTEXT_TOKENS = 8_192;
const MODEL_CONTEXT_PROJECTION_SHARE = 0.35;
const MIN_PROJECTION_CHARS = 256;

const MAX_COLLECTION_HISTORY_CHARS = 8_000;

export function resolveSkillWorkshopProjectionBudgets(contextTokens?: number) {
  const effectiveContextTokens =
    typeof contextTokens === "number" && Number.isFinite(contextTokens) && contextTokens > 0
      ? Math.floor(contextTokens)
      : DEFAULT_MODEL_CONTEXT_TOKENS;
  const contextChars = Math.max(
    MIN_PROJECTION_CHARS,
    Math.floor(effectiveContextTokens * MODEL_CONTEXT_PROJECTION_SHARE),
  );
  return {
    artifactChars: contextChars,
    collectionHistoryChars: Math.min(contextChars, MAX_COLLECTION_HISTORY_CHARS),
  };
}
