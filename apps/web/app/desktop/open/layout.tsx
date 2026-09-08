import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Open Multica",
  robots: { index: false, follow: false },
};

export default function DesktopOpenLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return children;
}
