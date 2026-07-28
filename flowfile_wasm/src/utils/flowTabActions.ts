// Pure tab-menu logic for the flow tab strip — no Vue imports so it stays unit-testable.
import type { ContextMenuOption } from '../components/common/ContextMenu.vue'

export type TabCloseAction = 'close' | 'close-others' | 'close-all' | 'close-left' | 'close-right'

/** Tab ids to close for a tab action, in tab order. Unknown target → no-op. */
export function computeCloseTargets(
  tabIds: string[],
  targetId: string,
  action: TabCloseAction
): string[] {
  const index = tabIds.indexOf(targetId)
  if (index === -1) return []
  switch (action) {
    case 'close':
      return [targetId]
    case 'close-others':
      return tabIds.filter((id) => id !== targetId)
    case 'close-all':
      return [...tabIds]
    case 'close-left':
      return tabIds.slice(0, index)
    case 'close-right':
      return tabIds.slice(index + 1)
  }
}

export function tabMenuOptions(tabIds: string[], targetId: string): ContextMenuOption[] {
  const index = tabIds.indexOf(targetId)
  const count = tabIds.length
  return [
    { label: 'Rename', action: 'rename' },
    { label: 'Close', action: 'close' },
    { label: 'Close Others', action: 'close-others', disabled: count <= 1 },
    { label: 'Close All', action: 'close-all' },
    { label: 'Close Tabs to the Left', action: 'close-left', disabled: index <= 0 },
    { label: 'Close Tabs to the Right', action: 'close-right', disabled: index >= count - 1 }
  ]
}
