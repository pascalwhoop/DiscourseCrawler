/**
 * Main entry point for the application
 */
export function main(): void {
  console.log('Hello, TypeScript!');
}

// Start the application if this file is run directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
