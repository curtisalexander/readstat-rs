import {init,read_data,read_data_feather,read_data_ndjson,read_data_parquet,read_metadata,read_preview} from "./readstat_wasm.js";

// Central browser policy. The UI receives this exact configuration in `ready`.
const MiB=1024*1024;
export const POLICY=Object.freeze({recommendedBytes:250*MiB,hardMaxBytes:500*MiB,exportMaxBytes:100*MiB,previewOptions:[25,50,100,250,500,1000],defaultPreview:100});
const EXPORTS=Object.freeze({
  csv:{run:read_data,extension:"csv",mime:"text/csv;charset=utf-8"},
  ndjson:{run:read_data_ndjson,extension:"ndjson",mime:"application/x-ndjson;charset=utf-8"},
  parquet:{run:read_data_parquet,extension:"parquet",mime:"application/vnd.apache.parquet"},
  feather:{run:read_data_feather,extension:"feather",mime:"application/vnd.apache.arrow.file"}
});
let activeOperation=0;
let bytes=null;
const send=(type,detail={},transfer=[])=>postMessage({type,...detail},transfer);
const state=(operationId,name)=>send("state",{operationId,state:name});

function nativeProgress(operationId,p){ send("progress",{operationId,phase:p.stage,current:p.current,total:p.total,determinate:p.total>0}) }
function readFile(file,operationId){ return new Promise((resolve,reject)=>{ const reader=new FileReader(); reader.onprogress=e=>send("progress",{operationId,phase:"file-read",current:e.loaded,total:e.total,determinate:e.lengthComputable}); reader.onerror=()=>reject(reader.error||new Error("The file could not be read")); reader.onabort=()=>reject(new Error("File reading was cancelled")); reader.onload=()=>resolve(new Uint8Array(reader.result)); reader.readAsArrayBuffer(file) }) }

async function selectFile(message){
  const {operationId,file,rowLimit}=message; activeOperation=operationId;
  bytes=null;
  try {
    if(file.size>POLICY.hardMaxBytes) throw new Error(`The selected file exceeds the ${POLICY.hardMaxBytes} byte maximum`);
    state(operationId,"reading"); const fileBytes=await readFile(file,operationId);
    // Multiple FileReaders can overlap while this worker awaits I/O. Do not let
    // an older read replace the dataset retained for later preview requests.
    if(operationId!==activeOperation)return;
    bytes=fileBytes;
    state(operationId,"metadata"); const metadata=JSON.parse(read_metadata(fileBytes));
    send("result",{operationId,kind:"metadata",metadata});
    state(operationId,"preview"); const ndjson=read_preview(fileBytes,rowLimit);
    send("result",{operationId,kind:"preview",ndjson,rowLimit}); state(operationId,"complete");
  } catch(error){ if(operationId===activeOperation) send("error",{operationId,message:error?.message||String(error)}) }
}
function rerunPreview({operationId,rowLimit}){
  activeOperation=operationId;
  try { if(!bytes) throw new Error("Choose a file before requesting a preview"); state(operationId,"preview"); const ndjson=read_preview(bytes,rowLimit); send("result",{operationId,kind:"preview",ndjson,rowLimit}); state(operationId,"complete") }
  catch(error){ send("error",{operationId,message:error?.message||String(error)}) }
}
function exportData({operationId,format,sourceName}){
  activeOperation=operationId;
  try {
    if(!bytes) throw new Error("Choose a file before exporting");
    if(bytes.byteLength>POLICY.exportMaxBytes) throw new Error(`Full export is limited to source files no larger than ${POLICY.exportMaxBytes} bytes`);
    const selected=EXPORTS[format]; if(!selected) throw new Error(`Unsupported export format: ${format}`);
    state(operationId,"exporting");
    const result=selected.run(bytes);
    const output=typeof result==="string"?new TextEncoder().encode(result):result;
    const baseName=sourceName.replace(/\.sas7bdat$/i,"")||"dataset";
    send("result",{operationId,kind:"export",output:output.buffer,filename:`${baseName}.${selected.extension}`,mime:selected.mime},[output.buffer]);
    state(operationId,"complete");
  } catch(error){ if(operationId===activeOperation)send("error",{operationId,message:error?.message||String(error)}) }
}
self.onmessage=e=>{ const m=e.data; if(m.type==="select") selectFile(m); else if(m.type==="preview") rerunPreview(m); else if(m.type==="export") exportData(m); else if(m.type==="reset"){activeOperation=m.operationId;bytes=null} };

state(0,"initializing");
init({
  onDownloadProgress:p=>send("progress",{operationId:0,phase:"wasm-download",current:p.loaded,total:p.total,determinate:p.total>0}),
  onProgress:p=>nativeProgress(activeOperation,p)
}).then(()=>send("ready",{operationId:0,config:POLICY})).catch(error=>send("error",{operationId:0,message:error?.message||String(error)}));
