"use client";

import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@multica/ui/components/ui/card";
import { buttonVariants } from "@multica/ui/components/ui/button";
import { useT } from "@multica/views/i18n";
import { buildDesktopIssueTarget, safeWebFallback } from "./issue-handoff";

function DesktopIssueHandoff() {
  const { t } = useT("auth");
  const searchParams = useSearchParams();
  const target = buildDesktopIssueTarget(searchParams);
  const [currentOrigin, setCurrentOrigin] = useState<string | null>(null);
  const fallback = currentOrigin
    ? safeWebFallback(searchParams, currentOrigin)
    : null;

  useEffect(() => {
    setCurrentOrigin(window.location.origin);
    if (target) window.location.href = target;
  }, [target]);

  const copy = target
    ? {
        title: t(($) => $.web.issue_handoff.opening_title),
        description: t(($) => $.web.issue_handoff.opening_description),
      }
    : {
        title: t(($) => $.web.issue_handoff.invalid_title),
        description: t(($) => $.web.issue_handoff.invalid_description),
      };

  return (
    <div className="flex min-h-screen items-center justify-center px-4">
      <Card className="w-full max-w-sm">
        <CardHeader className="text-center">
          <CardTitle className="text-display-sm">{copy.title}</CardTitle>
          <CardDescription>{copy.description}</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col items-stretch gap-2">
          {target ? (
            <a className={buttonVariants()} href={target}>
              {t(($) => $.web.issue_handoff.open_button)}
            </a>
          ) : null}
          {fallback ? (
            <a
              className={buttonVariants({ variant: "outline" })}
              href={fallback}
            >
              {t(($) => $.web.issue_handoff.continue_web)}
            </a>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}

export default function Page() {
  return (
    <Suspense fallback={null}>
      <DesktopIssueHandoff />
    </Suspense>
  );
}
