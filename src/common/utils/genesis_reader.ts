import fs from "fs";

// CommonJS interop for stream-json ecosystem
import streamChainPkg from "stream-chain";
import streamJsonPkg from "stream-json";
import pickPkg from "stream-json/filters/Pick";
import streamValuesPkg from "stream-json/streamers/StreamValues";

const { chain } = streamChainPkg;
const { parser } = streamJsonPkg;
const { pick } = pickPkg;
const { streamValues } = streamValuesPkg;

export async function readGenesis(path: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const pipeline = chain([
      fs.createReadStream("genesis.json"),
      parser(),
      pick({ filter: path }),
      streamValues(),
    ]);

    const result: any[] = [];

    pipeline.on("data", (data: any) => {
      result.push(data.value);
    });

    pipeline.on("end", () => resolve(result));
    pipeline.on("error", reject);
  });
}
