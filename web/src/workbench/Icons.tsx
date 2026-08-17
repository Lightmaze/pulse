import type { SVGProps } from "react";

export type IconName =
  | "pulse"
  | "plus"
  | "search"
  | "folder"
  | "message"
  | "activity"
  | "network"
  | "database"
  | "coins"
  | "settings"
  | "help"
  | "globe"
  | "chevronDown"
  | "chevronRight"
  | "panelLeft"
  | "panelRight"
  | "monitor"
  | "gitBranch"
  | "history"
  | "filter"
  | "edit"
  | "send"
  | "paperclip"
  | "terminal"
  | "file"
  | "check"
  | "clock"
  | "route"
  | "spark"
  | "external"
  | "refresh"
  | "more"
  | "x"
  | "info"
  | "radio";

const paths: Record<IconName, React.ReactNode> = {
  pulse: <path d="M3 12h4l2.3-6 4.1 12 2.3-6H21" />,
  plus: <><path d="M12 5v14" /><path d="M5 12h14" /></>,
  search: <><circle cx="11" cy="11" r="6.5" /><path d="m16 16 4 4" /></>,
  folder: <><path d="M3.5 7.5h6l2-2h3l2 2h4v11h-17z" /><path d="M3.5 10h17" /></>,
  message: <path d="M5 5.5h14v10H9l-4 3z" />,
  activity: <path d="M3 12h4l2-5 4 10 2-5h6" />,
  network: <><circle cx="6" cy="6" r="2" /><circle cx="18" cy="7" r="2" /><circle cx="12" cy="18" r="2" /><path d="m8 7 8 0M7 8l4 8m6-7-4 7" /></>,
  database: <><ellipse cx="12" cy="5.5" rx="7.5" ry="3" /><path d="M4.5 5.5v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-6" /><path d="M4.5 11.5v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-6" /></>,
  coins: <><ellipse cx="9" cy="7" rx="5.5" ry="2.5" /><path d="M3.5 7v4c0 1.4 2.5 2.5 5.5 2.5s5.5-1.1 5.5-2.5V7" /><path d="M9 13.5v3c0 1.4 2.5 2.5 5.5 2.5s5.5-1.1 5.5-2.5v-4" /><path d="M14.5 10.5c3 0 5.5 1.1 5.5 2.5s-2.5 2.5-5.5 2.5" /></>,
  settings: <><circle cx="12" cy="12" r="3" /><path d="M12 3v2m0 14v2M3 12h2m14 0h2M5.6 5.6 7 7m10 10 1.4 1.4M18.4 5.6 17 7M7 17l-1.4 1.4" /></>,
  help: <><circle cx="12" cy="12" r="9" /><path d="M9.7 9a2.5 2.5 0 1 1 3.2 2.4c-.9.3-.9 1-.9 1.8" /><path d="M12 17h.01" /></>,
  globe: <><circle cx="12" cy="12" r="9" /><path d="M3 12h18M12 3c2.3 2.5 3.4 5.5 3.4 9S14.3 18.5 12 21c-2.3-2.5-3.4-5.5-3.4-9S9.7 5.5 12 3" /></>,
  chevronDown: <path d="m7 10 5 5 5-5" />,
  chevronRight: <path d="m10 7 5 5-5 5" />,
  panelLeft: <><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M9 4v16" /></>,
  panelRight: <><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M15 4v16" /></>,
  monitor: <><rect x="3" y="4" width="18" height="13" rx="2" /><path d="M8 21h8m-4-4v4" /></>,
  gitBranch: <><circle cx="6" cy="5" r="2" /><circle cx="18" cy="7" r="2" /><circle cx="6" cy="19" r="2" /><path d="M6 7v10M8 10h4a6 6 0 0 0 6-3" /></>,
  history: <><path d="M4 10a8 8 0 1 1 2 7" /><path d="M4 5v5h5" /><path d="M12 7v5l3 2" /></>,
  filter: <path d="M4 5h16l-6 7v5l-4 2v-7z" />,
  edit: <><path d="m5 19 4-.8L19 8.2 15.8 5 5.8 15z" /><path d="m14.5 6.3 3.2 3.2" /></>,
  send: <><path d="m4 4 16 8-16 8 3-8z" /><path d="M7 12h13" /></>,
  paperclip: <path d="m9 12 5.6-5.6a3 3 0 0 1 4.2 4.2l-7.7 7.7a4.5 4.5 0 0 1-6.4-6.4l7-7" />,
  terminal: <><path d="m5 7 4 4-4 4" /><path d="M11 17h8" /></>,
  file: <><path d="M6 3h8l4 4v14H6z" /><path d="M14 3v5h5M9 13h6m-6 4h6" /></>,
  check: <path d="m5 12 4 4 10-10" />,
  clock: <><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></>,
  route: <><circle cx="6" cy="6" r="2" /><circle cx="18" cy="18" r="2" /><path d="M8 6h3a4 4 0 0 1 4 4v4a4 4 0 0 0 3 4" /><path d="m15 16 3 2-2 3" /></>,
  spark: <path d="m12 3 1.7 5.3L19 10l-5.3 1.7L12 17l-1.7-5.3L5 10l5.3-1.7z" />,
  external: <><path d="M14 4h6v6M20 4l-9 9" /><path d="M18 13v7H4V6h7" /></>,
  refresh: <><path d="M20 7v5h-5" /><path d="M18.5 16a8 8 0 1 1 .5-7l1 3" /></>,
  more: <><circle cx="5" cy="12" r="1" fill="currentColor" stroke="none" /><circle cx="12" cy="12" r="1" fill="currentColor" stroke="none" /><circle cx="19" cy="12" r="1" fill="currentColor" stroke="none" /></>,
  x: <><path d="m6 6 12 12M18 6 6 18" /></>,
  info: <><circle cx="12" cy="12" r="9" /><path d="M12 11v6M12 7h.01" /></>,
  radio: <><circle cx="12" cy="12" r="2" fill="currentColor" stroke="none" /><path d="M8.5 8.5a5 5 0 0 0 0 7M15.5 8.5a5 5 0 0 1 0 7M5.5 5.5a9 9 0 0 0 0 13M18.5 5.5a9 9 0 0 1 0 13" /></>,
};

export function Icon({
  name,
  size = 18,
  ...props
}: SVGProps<SVGSVGElement> & { name: IconName; size?: number }) {
  return (
    <svg
      aria-hidden="true"
      fill="none"
      height={size}
      viewBox="0 0 24 24"
      width={size}
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="1.55"
      {...props}
    >
      {paths[name]}
    </svg>
  );
}

export function HexMark({
  label,
  tone = "pulse",
  size = 42,
}: {
  label?: string;
  tone?: "pulse" | "amber" | "blue" | "violet" | "neutral";
  size?: number;
}) {
  return (
    <span
      className={`pw-hex pw-hex-${tone}`}
      style={{ "--hex-size": `${size}px` } as React.CSSProperties}
      aria-hidden="true"
    >
      {label ?? ""}
    </span>
  );
}
