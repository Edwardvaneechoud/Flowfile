const UNITS: [number, string][] = [
  [60, "second"],
  [60, "minute"],
  [24, "hour"],
  [7, "day"],
  [4.34524, "week"],
  [12, "month"],
  [Number.POSITIVE_INFINITY, "year"],
];

// Compact "x minutes ago" relative time from an ISO timestamp.
export const timeAgo = (iso: string): string => {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const secs = Math.round((Date.now() - then) / 1000);
  let value = Math.max(secs, 0);
  let unit = "second";
  for (const [size, name] of UNITS) {
    if (value < size) {
      unit = name;
      break;
    }
    value = Math.floor(value / size);
  }
  if (value <= 0 && unit === "second") return "just now";
  return `${value} ${unit}${value === 1 ? "" : "s"} ago`;
};

// Terse relative time for tight spots like stat tiles: "now", "43m", "2h", "3d", "2w", "5mo", "1y".
export const timeAgoShort = (iso: string): string => {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "—";
  const secs = Math.max(Math.round((Date.now() - then) / 1000), 0);
  const mins = Math.floor(secs / 60);
  if (mins < 1) return "now";
  if (mins < 60) return `${mins}m`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d`;
  const weeks = Math.floor(days / 7);
  if (weeks < 5) return `${weeks}w`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo`;
  return `${Math.floor(days / 365)}y`;
};
