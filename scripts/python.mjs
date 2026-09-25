import { spawn } from "node:child_process";

const candidates = process.platform === "win32"
  ? [["python", []], ["py", ["-3"]]]
  : [["python3", []], ["python", []]];

function launch(index) {
  if (index >= candidates.length) {
    console.error("Python 3.11+ was not found in PATH.");
    process.exit(127);
  }
  const [command, prefix] = candidates[index];
  const child = spawn(command, [...prefix, ...process.argv.slice(2)], { stdio: "inherit", env: process.env });
  child.on("error", error => {
    if (error.code === "ENOENT") launch(index + 1);
    else { console.error(error); process.exit(1); }
  });
  child.on("exit", (code, signal) => {
    if (signal) process.kill(process.pid, signal);
    else process.exit(code ?? 1);
  });
}

launch(0);
