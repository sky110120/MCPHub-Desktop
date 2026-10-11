export interface SmartRoutingConfigAliases<T> {
  smartRouting?: T | null;
  smartRoutingConfig?: T | null;
}

export function resolveSmartRoutingConfig<T>(
  systemConfig: SmartRoutingConfigAliases<T>,
): T | undefined {
  const config = systemConfig.smartRouting ?? systemConfig.smartRoutingConfig;
  if (config == null) return undefined;

  const fields = config as T & Record<string, unknown>;
  const valueFor = (current: string, legacy: string): string => {
    const value = Object.prototype.hasOwnProperty.call(fields, current)
      ? fields[current]
      : fields[legacy];
    return typeof value === 'string' ? value : '';
  };
  return {
    ...fields,
    llmProviderBaseUrl: valueFor('llmProviderBaseUrl', 'openaiApiBaseUrl'),
    llmProviderApiKey: valueFor('llmProviderApiKey', 'openaiApiKey'),
    embeddingModel: valueFor('embeddingModel', 'openaiApiEmbeddingModel'),
  } as T;
}
