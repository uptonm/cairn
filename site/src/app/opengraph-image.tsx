import { ImageResponse } from "next/og";
import { siteConfig } from "@/lib/site-config";

export const size = { width: 1200, height: 630 };
export const contentType = "image/png";
export const alt = siteConfig.title;

export default async function OpengraphImage() {
  return new ImageResponse(
    <div
      style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        padding: "80px",
        background: "#141110",
        backgroundImage:
          "radial-gradient(circle at 78% 22%, rgba(242,164,76,0.28), transparent 55%), radial-gradient(circle at 8% 92%, rgba(242,164,76,0.12), transparent 45%)",
        fontFamily: "monospace",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 18,
          color: "#f2a44c",
          fontSize: 30,
          letterSpacing: -1,
        }}
      >
        <svg width="40" height="40" viewBox="0 0 32 32" fill="none" role="img">
          <title>cairn</title>
          <rect width="32" height="32" rx="7" fill="#221c16" />
          <g fill="#f2a44c">
            <ellipse cx="16" cy="24.5" rx="9" ry="3.1" />
            <ellipse cx="16" cy="18" rx="6.6" ry="2.6" />
            <ellipse cx="16.4" cy="12.4" rx="4.4" ry="2.1" />
            <ellipse cx="15.8" cy="7.6" rx="2.6" ry="1.5" />
          </g>
        </svg>
        <span>cairn.uptonm.io</span>
      </div>
      <div
        style={{
          display: "flex",
          color: "#faf6f0",
          fontSize: 92,
          fontWeight: 700,
          marginTop: 36,
          letterSpacing: -3,
        }}
      >
        cairn
      </div>
      <div
        style={{
          display: "flex",
          color: "#cbc0b3",
          fontSize: 34,
          marginTop: 20,
          maxWidth: 980,
          lineHeight: 1.35,
        }}
      >
        A from-scratch, sharded, Raft-replicated, LSM-backed distributed
        key-value store — written in Rust.
      </div>
      <div
        style={{
          display: "flex",
          gap: 14,
          marginTop: 46,
        }}
      >
        {["Custom LSM engine", "Raft consensus", "MVCC", "Rust"].map(
          (label) => (
            <div
              key={label}
              style={{
                display: "flex",
                color: "#f2a44c",
                fontSize: 22,
                border: "1px solid rgba(242,164,76,0.4)",
                borderRadius: 999,
                padding: "8px 20px",
              }}
            >
              {label}
            </div>
          ),
        )}
      </div>
    </div>,
    { ...size },
  );
}
