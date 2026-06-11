/**
 * Share-link composable (app-shell only, like the saved-flows library).
 *
 * Serverless flow sharing: the flow + small text inputs travel inside the URL
 * hash fragment, so links work without any backend. Generation snapshots the
 * live flow; import opens the shared flow in a new designer tab via the tabs
 * store. URL/history manipulation stays in AppLayout — this composable only
 * deals in hash strings, which keeps it testable without router mocks.
 */

import { useFlowStore } from '../stores/flow-store'
import { useFlowTabsStore } from '../stores/flow-tabs-store'
import { encodeShareHash, decodeShareHash, hasShareHash } from '../utils/share-link'

export type ShareImportResult =
  | { status: 'imported'; missingFiles: Array<{ nodeId: number; fileName: string }> }
  | { status: 'none' | 'cancelled' | 'invalid' | 'failed' }

export function useShareLink() {
  const flowStore = useFlowStore()
  const flowTabsStore = useFlowTabsStore()

  /** Build a share URL for the live flow. Excluded files (binary/oversized)
   * are returned so the share dialog can tell the sender what won't travel. */
  async function generateShareUrl(): Promise<{
    url: string
    excludedFiles: Array<{ nodeId: number; fileName: string }>
  }> {
    const { flow, files, excludedFiles } = await flowStore.exportShareSnapshot()
    const hash = await encodeShareHash(flow, files)
    return { url: `${window.location.origin}/designer${hash}`, excludedFiles }
  }

  /**
   * Import a shared flow from a URL hash. Opens in a new tab (current flow is
   * kept); asks before opening when a non-empty flow is on canvas.
   */
  async function importShareHash(
    hash: string,
    opts?: { confirmOpen?: () => boolean },
  ): Promise<ShareImportResult> {
    if (!hasShareHash(hash)) return { status: 'none' }
    const payload = await decodeShareHash(hash)
    if (!payload) return { status: 'invalid' }

    if (flowStore.nodes.size > 0) {
      const confirmOpen =
        opts?.confirmOpen ??
        (() =>
          window.confirm(
            'Open the shared flow? It will open in a new tab; your current flow is kept.',
          ))
      if (!confirmOpen()) return { status: 'cancelled' }
    }

    const ok = await flowTabsStore.openWith(async () => {
      // Same restore sequence as the saved-flows library, minus the library
      // identity — a shared flow arrives unsaved.
      if (!flowStore.importFromFlowfile(payload.flow)) return false
      for (const [nid, content] of Object.entries(payload.files ?? {})) {
        flowStore.setFileContent(Number(nid), content)
      }
      // URL-sourced inputs whose content didn't travel hydrate in the
      // background; running before they arrive triggers an awaited fetch.
      void flowStore.refetchRemoteFiles()
      await flowStore.propagateSchemas()
      return true
    })
    if (!ok) return { status: 'failed' }

    return { status: 'imported', missingFiles: flowStore.getMissingFileNodes() }
  }

  return { generateShareUrl, importShareHash }
}
