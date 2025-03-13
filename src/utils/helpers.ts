/**
 * Helper function to escape single apostrophes in a JSON string
 * @param {string} json
 * @returns {string}
 */
export function escapeJson(json: string): string {
  return json.replace(/'/g, "''")
}
