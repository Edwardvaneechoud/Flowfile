// Opening a version of a hub flow as the person's own copy, shared by Open Flow → Hub and the Catalog tab's hub
// flow panel: core writes the copy, and a copy with unpublished edits is asked about before it is replaced.
import { ElMessageBox } from "element-plus";
import { openFromHub, refusalOf } from "../../api/hubFlows.api";
import { detailOf } from "../../utils/apiDetail";
import {
  LOCAL_COPY_TITLE,
  OPENED_FROM_HUB,
  OPENED_LOCAL_COPY,
  OPEN_MY_COPY,
  localCopyQuestion,
  refusalMessage,
  replaceWithLabel,
} from "./hubFlows";

export interface OpenedFromHub {
  /** The path of the copy to open as any local flow. */
  flowPath: string;
  /** What to tell the person: the version opened, or their own copy kept. */
  message: string;
}

/** The person closed the question about their copy without choosing: nothing was opened. */
export class OpenFromHubCancelled extends Error {
  constructor() {
    super("Open from hub cancelled");
  }
}

/** Core or the hub refused the open; the message is the sentence to show. */
export class OpenFromHubRefused extends Error {}

const refusedWith = (err: unknown): OpenFromHubRefused => {
  const refusal = refusalOf(err);
  return new OpenFromHubRefused(
    refusal ? refusalMessage(refusal) : detailOf(err, "Couldn't open the flow."),
  );
};

/**
 * Checks `version` of the hub flow out as the person's copy and says where it is. A copy with unpublished edits
 * asks: open that copy, or replace it with the version (`OpenFromHubCancelled` when neither is chosen).
 */
export async function openHubFlowVersion(
  flowUuid: string,
  version: number,
): Promise<OpenedFromHub> {
  try {
    const opened = await openFromHub({ flow_uuid: flowUuid, version, replace: false });
    return { flowPath: opened.flow_path, message: opened.note || OPENED_FROM_HUB };
  } catch (err) {
    const refusal = refusalOf(err);
    if (refusal?.kind !== "local_copy_exists") throw refusedWith(err);
    return askAboutLocalCopy(flowUuid, refusal.flow_path, version);
  }
}

async function askAboutLocalCopy(
  flowUuid: string,
  flowPath: string,
  version: number,
): Promise<OpenedFromHub> {
  try {
    await ElMessageBox.confirm(localCopyQuestion(version), LOCAL_COPY_TITLE, {
      type: "warning",
      confirmButtonText: OPEN_MY_COPY,
      cancelButtonText: replaceWithLabel(version),
      distinguishCancelAndClose: true,
    });
  } catch (action) {
    if (action !== "cancel") throw new OpenFromHubCancelled();
    try {
      const opened = await openFromHub({ flow_uuid: flowUuid, version, replace: true });
      return { flowPath: opened.flow_path, message: opened.note || OPENED_FROM_HUB };
    } catch (err) {
      throw refusedWith(err);
    }
  }
  return { flowPath, message: OPENED_LOCAL_COPY };
}
