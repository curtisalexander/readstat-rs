/** Small browser binding for the readstat Emscripten module. */
let instance;
let memory;
let progressCallback = () => {};

// Native stage ABI: 1 metadata, 2 preview parse, 3 preview encode,
// 4 export parse, 5 export encode. Export stages are reserved for future UI.
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
