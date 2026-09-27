import { spawnSync, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const python = resolve(root, ".venv", process.platform === "win32" ? "Scripts/python.exe" : "bin/python");
const run = (exe,args) => {
  const result = spawnSync(exe,args,{cwd:root,stdio:"inherit"});
  if(result.error || result.status !== 0) process.exit(result.status || 1);
};
let created = false;
if (!existsSync(python)) {
  run(process.platform === "win32" ? "python" : "python3", ["-m","venv",resolve(root,".venv")]);
  created = true;
}
const args = process.argv.slice(2);
if (created || args[0] === "--setup") {
  run(python,["-m","pip","install","--upgrade","pip","setuptools","wheel"]);
  run(python,["-m","pip","install","-e",".[dev]"]);
}
if (args[0] === "--setup") process.exit(0);
const child = spawn(python,args,{cwd:root,stdio:"inherit",env:process.env});
for (const signal of ["SIGINT","SIGTERM"]) process.on(signal,()=>child.kill(signal));
child.on("error",()=>process.exit(1));
child.on("exit",code=>process.exit(code ?? 1));
