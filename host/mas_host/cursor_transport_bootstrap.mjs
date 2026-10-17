// Configure the exact ESM SDK instance imported by the pinned bridge, before
// starting its original entry point. No vendor files or TLS settings change.
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";

try {
    const [node, , entry, ...arguments_] = process.argv;
    const sdk = resolve(dirname(entry), "..", "..", "node_modules", "@cursor", "sdk", "dist", "esm", "index.js");
    const { configureCursorSdk } = await import(pathToFileURL(sdk).href);
    configureCursorSdk({ local: { useHttp1ForAgent: true } });
    // The bridge CLI continues to receive exactly its original argument list.
    process.argv = [node, entry, ...arguments_];
    await import(pathToFileURL(entry).href);
} catch {
    process.stderr.write("Restricted Cursor transport failed to start.\n");
    process.exitCode = 1;
}
