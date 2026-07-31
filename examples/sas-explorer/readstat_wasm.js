/** Small browser binding for the readstat Emscripten module. */
let instance;
let memory;
let progressCallback = () => {};

// Native stage ABI: 1 metadata, 2 preview parse, 3 preview encode,
// 4 export parse, 5 export encode.
export const STAGES = Object.freeze({1:"metadata",2:"preview-parse",3:"preview-encode",4:"export-parse",5:"export-encode"});

function imports() {
  return {
    wasi_snapshot_preview1: {
      environ_sizes_get(a,b){ const v=new DataView(memory.buffer); v.setUint32(a,0,true); v.setUint32(b,0,true); return 0 },
      environ_get(){return 0}, fd_close(){return 0}, fd_seek(){return 0}, fd_read(){return 0},
      fd_write(fd,p,n,out){ const v=new DataView(memory.buffer); let total=0; for(let i=0;i<n;i++) total+=v.getUint32(p+i*8+4,true); v.setUint32(out,total,true); return 0 },
      random_get(p,n){ crypto.getRandomValues(new Uint8Array(memory.buffer,p,n)); return 0 }
    },
    env: {
      emscripten_notify_memory_growth(){},
      __syscall_getcwd(p){ new Uint8Array(memory.buffer).set([47,0],p); return p },
      readstat_progress(stage,current,total){ progressCallback({stage:STAGES[stage]||`stage-${stage}`,stageCode:stage,current:Number(current),total:Number(total)}) }
    }
  };
}

function cString(ptr){ const bytes=new Uint8Array(memory.buffer); let end=ptr; while(end<bytes.length&&bytes[end]) end++; return new TextDecoder().decode(bytes.subarray(ptr,end)) }
function lastError(){ const fn=instance.exports.readstat_last_error; const p=typeof fn==="function"?fn():0; return p?cString(p):"Native operation failed" }
function call(fn,bytes,...args){
  if(!instance) throw new Error("WASM is not initialized");
  const {malloc,free,free_string}=instance.exports;
  const input=malloc(bytes.byteLength); if(!input) throw new Error("Unable to allocate WASM input memory");
  let output=0;
  try { new Uint8Array(memory.buffer).set(bytes,input); output=fn(input,bytes.byteLength,...args); if(!output) throw new Error(lastError()); return cString(output) }
  finally { free(input); if(output) free_string(output) }
}
function callBinary(fn,bytes,...args){
  if(!instance) throw new Error("WASM is not initialized");
  const {malloc,free,free_binary}=instance.exports;
  const input=malloc(bytes.byteLength); if(!input) throw new Error("Unable to allocate WASM input memory");
  const outputLength=malloc(4); if(!outputLength){free(input);throw new Error("Unable to allocate WASM output length")}
  let output=0,length=0;
  try {
    new Uint8Array(memory.buffer).set(bytes,input);
    output=fn(input,bytes.byteLength,...args,outputLength); if(!output) throw new Error(lastError());
    length=new DataView(memory.buffer).getUint32(outputLength,true);
    return new Uint8Array(memory.buffer,output,length).slice();
  } finally { free(input);free(outputLength);if(output)free_binary(output,length) }
}
function reduced(fn,bytes,selection,binary=false){
  if(!instance) throw new Error("WASM is not initialized");
  const {columns,rowOffset,rowLimit}=selection||{};
  if(!Array.isArray(columns)||!columns.length||columns.some(name=>typeof name!=="string"||!name)) throw new TypeError("columns must be a non-empty array of column names");
  if(!Number.isInteger(rowOffset)||rowOffset<0||rowOffset>0xffff_ffff) throw new RangeError("rowOffset must be an integer between 0 and 4294967295");
  if(!Number.isInteger(rowLimit)||rowLimit<1||rowLimit>0xffff_ffff) throw new RangeError("rowLimit must be an integer between 1 and 4294967295");
  const encoded=new TextEncoder().encode(JSON.stringify(columns)),pointer=instance.exports.malloc(encoded.length);
  if(!pointer) throw new Error("Unable to allocate selected columns");
  try { new Uint8Array(memory.buffer).set(encoded,pointer); const args=[pointer,encoded.length,rowOffset,rowLimit]; return binary?callBinary(fn,bytes,...args):call(fn,bytes,...args) }
  finally { instance.exports.free(pointer) }
}

export async function init(options={}) {
  if(instance) return;
  progressCallback=options.onProgress||progressCallback;
  const response=await fetch(new URL("readstat_wasm.wasm",import.meta.url));
  if(!response.ok) throw new Error(`Could not download analysis engine (${response.status})`);
  const total=Number(response.headers.get("Content-Length"))||0;
  let loaded=0, wasmBytes;
  if(response.body){ const reader=response.body.getReader(), chunks=[]; while(true){ const {done,value}=await reader.read(); if(done) break; chunks.push(value); loaded+=value.byteLength; options.onDownloadProgress?.({loaded,total}); } wasmBytes=new Uint8Array(loaded); let at=0; for(const chunk of chunks){wasmBytes.set(chunk,at);at+=chunk.byteLength} }
  else { wasmBytes=new Uint8Array(await response.arrayBuffer()); loaded=wasmBytes.byteLength; options.onDownloadProgress?.({loaded,total}); }
  const result=await WebAssembly.instantiate(wasmBytes,imports()); instance=result.instance; memory=instance.exports.memory;
  if(typeof instance.exports._initialize==="function") instance.exports._initialize();
}
export function read_metadata(bytes){ return call(instance.exports.read_metadata,bytes) }
export function read_preview(bytes,rowLimit){ if(!Number.isInteger(rowLimit)||rowLimit<1||rowLimit>0xffff_ffff) throw new RangeError("rowLimit must be an integer between 1 and 4294967295"); return call(instance.exports.read_preview,bytes,rowLimit) }
export function read_data(bytes){ return call(instance.exports.read_data,bytes) }
export function read_data_ndjson(bytes){ return call(instance.exports.read_data_ndjson,bytes) }
export function read_data_parquet(bytes){ return callBinary(instance.exports.read_data_parquet,bytes) }
export function read_data_feather(bytes){ return callBinary(instance.exports.read_data_feather,bytes) }
export function read_data_reduced(bytes,selection){ return reduced(instance.exports.read_data_reduced,bytes,selection) }
export function read_data_ndjson_reduced(bytes,selection){ return reduced(instance.exports.read_data_ndjson_reduced,bytes,selection) }
export function read_data_parquet_reduced(bytes,selection){ return reduced(instance.exports.read_data_parquet_reduced,bytes,selection,true) }
export function read_data_feather_reduced(bytes,selection){ return reduced(instance.exports.read_data_feather_reduced,bytes,selection,true) }
