// Review preparation cannot inherit an employee's permission to send or change data.
const REVIEW_TOOLS = new Set([
  "computer_observe",
  "list_files",
  "read_file",
  "recall_memory",
  "schedule_list",
  "scratchpad_list",
  "skill_read",
]);

export function reviewPreparationToolAllowed(
  name: string,
  readOnlyConnector: boolean,
  socialReview = false,
): boolean {
  return (
    REVIEW_TOOLS.has(name) ||
    readOnlyConnector ||
    (socialReview && name === "brandwell_socialstreams_update_opportunity")
  );
}
