const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

type SearchParams = Pick<URLSearchParams, "getAll">;

function singleParam(params: SearchParams, key: string): string | null {
  const values = params.getAll(key);
  return values.length === 1 ? (values[0] ?? null) : null;
}

export function buildDesktopIssueTarget(params: SearchParams): string | null {
  const issueId = singleParam(params, "issue");
  const workspaceId = singleParam(params, "workspace");
  const commentIds = params.getAll("comment");
  const commentId = commentIds[0] ?? null;
  if (
    !issueId ||
    !workspaceId ||
    commentIds.length > 1 ||
    !UUID_PATTERN.test(issueId) ||
    !UUID_PATTERN.test(workspaceId) ||
    (commentId !== null && !UUID_PATTERN.test(commentId))
  ) {
    return null;
  }

  const query = new URLSearchParams({ workspace: workspaceId.toLowerCase() });
  if (commentId) query.set("comment", commentId.toLowerCase());
  return `multica://issue/${issueId.toLowerCase()}?${query.toString()}`;
}

export function safeWebFallback(
  params: SearchParams,
  currentOrigin: string,
): string | null {
  const raw = singleParam(params, "fallback");
  if (!raw) return null;
  try {
    const fallback = new URL(raw);
    if (
      fallback.origin !== currentOrigin ||
      (fallback.protocol !== "https:" && fallback.protocol !== "http:")
    ) {
      return null;
    }
    return fallback.toString();
  } catch {
    return null;
  }
}
