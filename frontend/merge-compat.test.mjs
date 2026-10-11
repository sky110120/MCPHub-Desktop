import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { normalizeImportedServers } from './src/utils/jsonImport.ts';
import { resolveSmartRoutingConfig } from './src/utils/settingsCompatibility.ts';
import { transformTauriResponse } from './src/utils/tauriClient.ts';

test('legacy disabled and modern enabled fields round-trip semantically', () => {
  const result = normalizeImportedServers({
    mcpServers: {
      legacyDisabled: { command: 'node', disabled: true },
      enabledWins: { command: 'node', enabled: false, disabled: false },
      legacyEnabled: { command: 'node', disabled: false },
    },
  });

  assert.deepEqual(
    result.servers.map(({ config }) => config.enabled),
    [false, false, true],
  );
});

test('SmartRouting falls back to the legacy key when the current key is absent', () => {
  const legacy = {
    enabled: true,
    dbUrl: 'legacy-db',
    openaiApiBaseUrl: 'https://legacy.example/v1',
    openaiApiKey: 'legacy-key',
    openaiApiEmbeddingModel: 'legacy-embedding',
  };

  assert.deepEqual(
    resolveSmartRoutingConfig({ smartRoutingConfig: legacy }),
    {
      ...legacy,
      llmProviderBaseUrl: 'https://legacy.example/v1',
      llmProviderApiKey: 'legacy-key',
      embeddingModel: 'legacy-embedding',
    },
  );
});

test('current empty SmartRouting fields win over legacy field aliases', () => {
  const current = {
    llmProviderBaseUrl: '',
    llmProviderApiKey: '',
    embeddingModel: '',
  };

  assert.deepEqual(
    resolveSmartRoutingConfig({
      smartRouting: current,
      smartRoutingConfig: {
        openaiApiBaseUrl: 'https://legacy.example/v1',
        openaiApiKey: 'legacy-key',
        openaiApiEmbeddingModel: 'legacy-embedding',
      },
    }),
    current,
  );
});

test('explicitly cleared canonical fields cannot resurrect legacy credentials', () => {
  const result = resolveSmartRoutingConfig({
    smartRouting: {
      llmProviderBaseUrl: null,
      llmProviderApiKey: '',
      embeddingModel: null,
      openaiApiBaseUrl: 'https://legacy.example/v1',
      openaiApiKey: 'old-key',
      openaiApiEmbeddingModel: 'old-model',
    },
  });
  assert.equal(result.llmProviderBaseUrl, '');
  assert.equal(result.llmProviderApiKey, '');
  assert.equal(result.embeddingModel, '');
});

test('RAG auto-update remains opt-in when settings are initialized or absent', async () => {
  const hook = await readFile(new URL('./src/hooks/useRagData.tsx', import.meta.url), 'utf8');
  const service = await readFile(new URL('./src/services/ragService.ts', import.meta.url), 'utf8');
  const page = await readFile(new URL('./src/pages/RagPage.tsx', import.meta.url), 'utf8');

  assert.match(hook, /autoUpdateEnabled: false/);
  assert.match(service, /autoUpdateEnabled: false/);
  assert.match(page, /initial\.autoUpdateEnabled \?\? false/);
});

test('DynamicForm rejects prototype-mutating path segments', async () => {
  const form = await readFile(new URL('./src/components/ui/DynamicForm.tsx', import.meta.url), 'utf8');

  for (const key of ['__proto__', 'constructor', 'prototype']) {
    assert.ok(form.includes(`key === '${key}'`) || form.includes(`lastKey === '${key}'`));
  }
});

test('SmartRouting reindex preserves real per-server stats from Tauri', () => {
  const invokeResult = {
    syncedServers: 1,
    failedServers: 1,
    skippedServers: 1,
    upToDateServers: 1,
    totalTools: 10,
    results: [
      { serverName: 'indexed', toolCount: 2, ok: true, skipped: false },
      { serverName: 'failed', toolCount: 3, ok: false, skipped: false, error: 'embedding failed' },
      {
        serverName: 'unchanged',
        toolCount: 5,
        ok: true,
        skipped: true,
        error: 'index is already up to date',
      },
    ],
  };

  assert.deepEqual(
    transformTauriResponse('smart_routing_reindex', invokeResult),
    { success: true, data: invokeResult },
  );
});

test('SmartRouting reindex never treats a server count as a tool count', () => {
  assert.deepEqual(
    transformTauriResponse('smart_routing_reindex', 2),
    { success: true, data: 2 },
  );
});
