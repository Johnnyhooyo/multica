import { render, screen, waitFor } from "@testing-library/react";
import { I18nProvider } from "@multica/core/i18n/react";
import enAuth from "@multica/views/locales/en/auth.json";
import enCommon from "@multica/views/locales/en/common.json";
import enSettings from "@multica/views/locales/en/settings.json";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

const TEST_RESOURCES = {
  en: { auth: enAuth, common: enCommon, settings: enSettings },
};
const searchParamsState = vi.hoisted(() => ({
  params: new URLSearchParams(),
}));

vi.mock("next/navigation", () => ({
  useSearchParams: () => searchParamsState.params,
}));

import Page from "./page";
import { buildDesktopIssueTarget, safeWebFallback } from "./issue-handoff";

const ISSUE_ID = "11111111-2222-3333-4444-555555555555";
const WORKSPACE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const COMMENT_ID = "99999999-8888-7777-6666-555555555555";
const originalLocation = window.location;

function Wrapper({ children }: { children: ReactNode }) {
  return (
    <I18nProvider locale="en" resources={TEST_RESOURCES}>
      {children}
    </I18nProvider>
  );
}

afterEach(() => {
  Object.defineProperty(window, "location", {
    configurable: true,
    value: originalLocation,
  });
});

describe("desktop Issue handoff", () => {
  it("builds only the supported UUID-scoped Multica target", () => {
    const params = new URLSearchParams({
      issue: ISSUE_ID.toUpperCase(),
      workspace: WORKSPACE_ID.toUpperCase(),
      comment: COMMENT_ID.toUpperCase(),
    });
    expect(buildDesktopIssueTarget(params)).toBe(
      `multica://issue/${ISSUE_ID}?workspace=${WORKSPACE_ID}&comment=${COMMENT_ID}`,
    );

    params.set("workspace", "not-a-uuid");
    expect(buildDesktopIssueTarget(params)).toBeNull();

    params.set("workspace", WORKSPACE_ID);
    params.append("comment", COMMENT_ID);
    expect(buildDesktopIssueTarget(params)).toBeNull();
  });

  it("accepts only a same-origin HTTP fallback", () => {
    expect(
      safeWebFallback(
        new URLSearchParams({ fallback: "https://app.example.com/acme/issues/1#comment-2" }),
        "https://app.example.com",
      ),
    ).toBe("https://app.example.com/acme/issues/1#comment-2");
    expect(
      safeWebFallback(
        new URLSearchParams({ fallback: "https://evil.example/acme/issues/1" }),
        "https://app.example.com",
      ),
    ).toBeNull();
  });

  it("tries Desktop immediately and keeps explicit Desktop and web actions", async () => {
    const target = `multica://issue/${ISSUE_ID}?workspace=${WORKSPACE_ID}&comment=${COMMENT_ID}`;
    const fallback = `https://app.example.com/acme/issues/${ISSUE_ID}#comment-${COMMENT_ID}`;
    searchParamsState.params = new URLSearchParams({
      issue: ISSUE_ID,
      workspace: WORKSPACE_ID,
      comment: COMMENT_ID,
      fallback,
    });
    const hrefSetter = vi.fn();
    Object.defineProperty(window, "location", {
      configurable: true,
      value: {
        origin: "https://app.example.com",
        set href(value: string) {
          hrefSetter(value);
        },
      },
    });

    render(<Page />, { wrapper: Wrapper });

    await waitFor(() => expect(hrefSetter).toHaveBeenCalledWith(target));
    expect(screen.getByRole("link", { name: "Open Multica Desktop" })).toHaveAttribute("href", target);
    expect(screen.getByRole("link", { name: "Continue in browser" })).toHaveAttribute("href", fallback);
  });
});
