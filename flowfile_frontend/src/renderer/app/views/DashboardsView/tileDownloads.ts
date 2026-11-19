import { ElMessage } from "element-plus";
import { saveCsv, saveFile } from "../../utils/tableExport";
import { toCsv } from "./tableTile";
import { downloadName, resultColumns } from "./tileActions";

type TileQuery = (
  payload: any,
) => Promise<{ rows: Record<string, unknown>[]; error: string | null }>;

/**
 * The data a chart draws as a CSV file.
 *
 * Graphic Walker sends a chart several queries (filter domains, the view) that the tile cannot
 * tell apart, so the chart's own query is built from its spec with Graphic Walker's
 * ``chartToWorkflow`` and sent through the tile's ``query``, which adds the tile's filters and
 * takes the chart's route, exactly as the drawn chart's query goes.
 */
export async function downloadChartCsv(
  chart: Record<string, any>,
  query: TileQuery,
  title: string,
): Promise<void> {
  try {
    const { chartToWorkflow } = await import("@kanaries/graphic-walker");
    const { rows, error } = await query(chartToWorkflow(chart as any));
    if (error) {
      ElMessage.error(`Could not read the chart's data: ${error}`);
      return;
    }
    await saveCsv(toCsv(resultColumns(rows), rows), downloadName(title, "csv", "chart"));
  } catch (err) {
    console.error("Chart CSV download failed:", err);
    ElMessage.error("Could not download the chart's data.");
  }
}

/** The chart as drawn, painted over the tile's background, as a PNG file. */
export async function downloadChartImage(
  image: string | null | undefined,
  background: string,
  title: string,
): Promise<void> {
  if (!image) {
    ElMessage.warning("The chart is not drawn yet.");
    return;
  }
  try {
    await saveFile(
      await imageOnBackground(image, background),
      downloadName(title, "png", "chart"),
      {
        name: "PNG image",
        extensions: ["png"],
      },
    );
  } catch (err) {
    console.error("Chart image download failed:", err);
    ElMessage.error("Could not save the chart's image.");
  }
}

/**
 * A picture painted over a background colour, as a PNG.
 *
 * Graphic Walker's export can leave transparent pixels, which read as black or white depending on
 * the viewer opening the file; painting the tile's colour first keeps the picture as it was shown.
 */
export async function imageOnBackground(dataUrl: string, background: string): Promise<Blob> {
  const img = new Image();
  await new Promise<void>((resolve, reject) => {
    img.onload = () => resolve();
    img.onerror = () => reject(new Error("The chart's image could not be read"));
    img.src = dataUrl;
  });
  const canvas = document.createElement("canvas");
  canvas.width = img.naturalWidth;
  canvas.height = img.naturalHeight;
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("No canvas to draw on");
  ctx.fillStyle = isOpaque(background) ? background : "#ffffff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.drawImage(img, 0, 0);
  return new Promise<Blob>((resolve, reject) =>
    canvas.toBlob(
      (blob) => (blob ? resolve(blob) : reject(new Error("The image could not be encoded"))),
      "image/png",
    ),
  );
}

const isOpaque = (colour: string): boolean => {
  if (!colour || colour === "transparent") return false;
  const rgba = colour.match(/^rgba\([^)]*,\s*([\d.]+)\)$/);
  return !rgba || Number(rgba[1]) > 0.9;
};
