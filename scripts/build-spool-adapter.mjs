import { readFile, mkdir, writeFile } from 'node:fs/promises';
import ts from 'typescript';
// Cópia compilada de um adaptador opcional, não executa o adaptador, migrations ou qualquer SQL.
const source = await readFile(new URL('../adapters/local-spool.ts',import.meta.url),'utf8');
const result = ts.transpileModule(source,{ fileName: 'local-spool.ts', compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022, isolatedModules: true,
} });
await mkdir(new URL('../dist/adapters/',import.meta.url),{ recursive: true });
await writeFile(new URL('../dist/adapters/local-spool.js',import.meta.url),result.outputText);
