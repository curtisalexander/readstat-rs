const $=id=>document.getElementById(id);
const worker=new Worker(new URL("./worker.js",import.meta.url),{type:"module"});
let config=null, operationId=0, currentFile=null, pendingFile=null, variables=[], selectedColumns=new Set(), metadataRows=0, previewRows=[], previewColumns=[], variableTypes=new Map(), sort={column:null,direction:1}, parserBusy=true, sqlBusy=false, isBusy=true;
let duckdbModule=null, duckdb=null, sqlConnection=null, sqlFileName="", sqlSelectionKey="", sqlGeneration=0, sqlQueryRunning=false, sqlLoadMetrics=null;
const formatBytes=n=>n>=1024**2?`${(n/1024**2).toFixed(n<10*1024**2?1:0)} MiB`:`${Math.ceil(n/1024)} KiB`;
function status(text,kind=""){ $("status").textContent=text; $("status").className=kind }
function busy(value){ parserBusy=value;isBusy=parserBusy||sqlBusy;const disabled=isBusy||!config; $("previewLimit").disabled=isBusy||!currentFile; ["exportFormat","exportRowStart","exportRowCount","selectAllVariables","clearVariables"].forEach(id=>$(id).disabled=isBusy||!currentFile); document.querySelectorAll(".variable-select-input").forEach(input=>input.disabled=isBusy); $("exportButton").disabled=isBusy||!currentFile||currentFile.size>config.exportMaxBytes||selectedColumns.size===0||metadataRows===0; $("sqlLoadButton").disabled=isBusy||!currentFile||currentFile.size>config.sqlMaxSourceBytes||selectedColumns.size===0||metadataRows===0; $("sqlRunButton").disabled=isBusy||!sqlConnection; $("sqlCancelButton").disabled=!sqlQueryRunning; $("filePicker").disabled=disabled; $("replaceFile").disabled=disabled; $("startOver").disabled=disabled; $("dropZone").classList.toggle("disabled",disabled); $("dropZone").setAttribute("aria-disabled",String(disabled)) }
function setSqlBusy(value,running=false){sqlBusy=value;sqlQueryRunning=running;busy(parserBusy)}
function progress(m){ $("statusPanel").classList.add("has-progress");$("progressWrap").classList.remove("hidden"); const p=$("progress"); if(m.determinate&&m.total>0){p.max=m.total;p.value=m.current}else p.removeAttribute("value"); const labels={"wasm-download":"Downloading analysis engine","file-read":"Reading local file",metadata:"Reading metadata","preview-parse":"Parsing preview","preview-encode":"Encoding preview","export-parse":"Parsing export","export-encode":"Encoding export"}; $("progressText").textContent=`${labels[m.phase]||m.phase}${m.determinate&&m.total?` — ${Math.min(100,Math.round(m.current/m.total*100))}%`:""}` }
function clearProgress(){ $("progress").value=1;$("progress").max=1;$("progressText").textContent="";$("progressWrap").classList.add("hidden");$("statusPanel").classList.remove("has-progress") }

worker.onmessage=e=>{ const m=e.data; if(m.type==="ready"){config=m.config; setupConfig(); status("Ready — choose a SAS dataset");clearProgress();return} if(m.operationId!==operationId)return;
  if(m.type==="progress")progress(m); else if(m.type==="state"){busy(m.state!=="complete"); const labels={reading:"Reading the selected file…",metadata:"Inspecting metadata…",preview:"Building a bounded preview…",exporting:"Exporting selected data…","sql-preparing":"Preparing the bounded SQL dataset…",complete:`Ready — ${currentFile?.name||"dataset"}`}; if(labels[m.state]&&!(m.state==="complete"&&sqlBusy))status(labels[m.state]); if(m.state==="complete")clearProgress()}
  else if(m.type==="error"){status(m.message,"error");if(sqlBusy)showSqlError(m.message);setSqlBusy(false);busy(false);clearProgress()} else if(m.type==="result"){if(m.kind==="metadata")renderMetadata(m.metadata);else if(m.kind==="preview")renderPreview(m.ndjson,m.rowLimit);else if(m.kind==="sql-data")void loadSqlData(m);else downloadExport(m)}
};
worker.onerror=()=>{status("The analysis worker stopped unexpectedly. Reload the page to try again.","error");clearProgress();busy(true)};
function setupConfig(){ $("pickLabel").classList.remove("disabled");$("policyText").textContent=`Recommended up to ${formatBytes(config.recommendedBytes)} · maximum ${formatBytes(config.hardMaxBytes)}`;$("exportPolicy").textContent=`Exports are limited to source files up to ${formatBytes(config.exportMaxBytes)} because output is still materialized in memory. Selecting fewer rows or variables reduces parsed and downloaded data but is not yet a streaming export.`; for(const n of config.previewOptions){const o=new Option(n,n,n===config.defaultPreview,n===config.defaultPreview);$("previewLimit").add(o)} busy(false) }

function validateFile(file){
  if(!config)return; if(!file.name.toLowerCase().endsWith(".sas7bdat")){status("Choose a file ending in .sas7bdat.","error");return} if(file.size>config.hardMaxBytes){status(`${file.name} is ${formatBytes(file.size)} and exceeds the ${formatBytes(config.hardMaxBytes)} maximum.`,"error");return}
  return true;
}
function choose(file){
  if(!validateFile(file))return; if(currentFile){ pendingFile=file; $("replacementName").textContent=file.name; $("currentFileName").textContent=currentFile.name; $("replaceDialog").showModal(); return }
  loadFile(file);
}
function loadFile(file){
  void disposeSql();setPreviewExpanded(false);currentFile=file; pendingFile=null; operationId++; variables=[];selectedColumns.clear();metadataRows=0;previewRows=[]; variableTypes.clear(); $("variableSearch").value="";$("exportRowStart").value="1";$("exportRowCount").value=""; ["summaryPanel","exportPanel","sqlPanel","variablesPanel","previewPanel"].forEach(id=>$(id).classList.add("hidden"));
  $("dropZone").classList.add("hidden"); $("loadedFile").classList.remove("hidden"); $("statusPanel").classList.add("has-file"); $("loadedFileName").textContent=file.name; $("loadedFileDetails").textContent=`${formatBytes(file.size)} · Local browser session`;
  const warning=$("warning"); warning.classList.toggle("hidden",file.size<=config.recommendedBytes); warning.textContent=file.size>config.recommendedBytes?`Large file: ${formatBytes(file.size)} exceeds the recommended ${formatBytes(config.recommendedBytes)}. It may require substantial memory and take longer.`:"";
  status(`Reading ${file.name} locally…`);$("progress").removeAttribute("value");$("statusPanel").classList.add("has-progress");$("progressWrap").classList.remove("hidden");busy(true);worker.postMessage({type:"select",operationId,file,rowLimit:Number($("previewLimit").value)});
}
const zone=$("dropZone"), picker=$("filePicker"); picker.onchange=()=>picker.files[0]&&choose(picker.files[0]); zone.onclick=e=>{if(config&&e.target!==picker&&e.target.tagName!=="LABEL")picker.click()}; zone.onkeydown=e=>{if(config&&(e.key==="Enter"||e.key===" ")){e.preventDefault();picker.click()}};
for(const event of ["dragenter","dragover"]){zone.addEventListener(event,e=>{e.preventDefault();if(config&&!isBusy)zone.classList.add("drag")})} zone.ondragleave=()=>zone.classList.remove("drag");zone.ondrop=e=>{e.preventDefault();zone.classList.remove("drag");if(config&&!isBusy&&e.dataTransfer.files[0])choose(e.dataTransfer.files[0])};
$("replaceFile").onclick=()=>{picker.value="";picker.click()}; $("keepCurrent").onclick=()=>{pendingFile=null;picker.value="";$("replaceDialog").close()}; $("confirmReplace").onclick=()=>{const file=pendingFile;$("replaceDialog").close();if(file)loadFile(file)}; $("replaceDialog").oncancel=()=>{pendingFile=null;picker.value=""};
$("startOver").onclick=()=>{void disposeSql();setPreviewExpanded(false);operationId++;worker.postMessage({type:"reset",operationId});currentFile=null;pendingFile=null;variables=[];selectedColumns.clear();metadataRows=0;previewRows=[];previewColumns=[];variableTypes.clear();picker.value="";$("variableSearch").value="";["summaryPanel","exportPanel","sqlPanel","variablesPanel","previewPanel","loadedFile","warning"].forEach(id=>$(id).classList.add("hidden"));$("statusPanel").classList.remove("has-file");$("dropZone").classList.remove("hidden");status("Ready — choose a SAS dataset");clearProgress();busy(false)};

function value(v){return v===null||v===undefined||v===""?"—":String(v)}
function renderMetadata(meta){
  const entries=[["Rows",meta.row_count?.toLocaleString(),true],["Variables",meta.var_count?.toLocaleString(),true],["File size",formatBytes(currentFile.size),true],["Table",meta.table_name],["Encoding",meta.file_encoding],["Compression",meta.compression],["Endianness",meta.endianness],["Created",meta.creation_time],["Modified",meta.modified_time]];
  const dl=$("summary");dl.replaceChildren(...entries.map(([k,v,primary])=>{const d=document.createElement("div"),dt=document.createElement("dt"),dd=document.createElement("dd");if(primary)d.classList.add("metric-primary");dt.textContent=k;dd.textContent=value(v);d.append(dt,dd);return d}));
  variables=Object.entries(meta.vars||{}).sort((a,b)=>Number(a[0])-Number(b[0])).map(([,v])=>v);selectedColumns=new Set(variables.map(v=>v.var_name));metadataRows=Math.max(0,Number(meta.row_count)||0);$("exportRowStart").max=Math.max(1,metadataRows);$("exportRowCount").max=Math.max(1,metadataRows);variableTypes=new Map(variables.map(v=>[v.var_name,typeInfo(v)]));renderVariables();updateExportSelection();$("summaryPanel").classList.remove("hidden");$("exportPanel").classList.remove("hidden");$("sqlPanel").classList.remove("hidden");$("variablesPanel").classList.remove("hidden")
}
function typeInfo(v){const format=String(v.var_format_class||"").toLowerCase(),type=String(v.var_type||"").toLowerCase();if(format.startsWith("datetime"))return{kind:"datetime",label:"Datetime"};if(format==="date")return{kind:"date",label:"Date"};if(format.startsWith("time"))return{kind:"time",label:"Time"};if(type.includes("string")||String(v.var_type_class||"").toLowerCase()==="string")return{kind:"string",label:"String"};return{kind:"numeric",label:v.var_type||"Numeric"}}
function renderVariables(){const q=$("variableSearch").value.trim().toLowerCase(), fields=v=>{const info=typeInfo(v),storage=`${v.var_type||""}${v.var_type_class?` / ${v.var_type_class}`:""}`,semantic=info.kind==="numeric"||info.kind==="string"?storage:`${info.label} · ${storage}`;return[v.var_name,semantic,v.storage_width,v.var_label,v.var_format,v.display_width]}, filtered=variables.filter(v=>!q||fields(v).join(" ").toLowerCase().includes(q)); $("variableCount").textContent=q?`— ${filtered.length.toLocaleString()} of ${variables.length.toLocaleString()} variables`:`— ${variables.length.toLocaleString()} variables`; const rows=filtered.map(v=>{const tr=document.createElement("tr"),info=typeInfo(v),selectCell=document.createElement("td"),checkbox=document.createElement("input");selectCell.className="column-select";checkbox.type="checkbox";checkbox.className="variable-select-input";checkbox.checked=selectedColumns.has(v.var_name);checkbox.disabled=isBusy;checkbox.setAttribute("aria-label",`Export ${v.var_name}`);checkbox.onchange=()=>{if(checkbox.checked)selectedColumns.add(v.var_name);else selectedColumns.delete(v.var_name);updateExportSelection()};selectCell.append(checkbox);tr.append(selectCell);for(const [i,x] of fields(v).entries()){const td=document.createElement("td");td.textContent=value(x);td.title=x==null?"":String(x);if(i===0)td.classList.add("variable-name",`type-${info.kind}`);if(i===1){const badge=document.createElement("span");badge.classList.add("type-badge",`type-${info.kind}`);badge.textContent=info.label;td.replaceChildren(badge)}tr.append(td)}return tr});$("variables").replaceChildren(...rows)} $("variableSearch").oninput=renderVariables;

function updateExportSelection(){const selected=selectedColumns.size,total=variables.length;$("exportColumnCount").textContent=selected===total?`All ${total.toLocaleString()} variables selected`:`${selected.toLocaleString()} of ${total.toLocaleString()} variables selected`;updateSqlScope();busy(parserBusy)}
$("selectAllVariables").onclick=()=>{selectedColumns=new Set(variables.map(v=>v.var_name));renderVariables();updateExportSelection()};
$("clearVariables").onclick=()=>{selectedColumns.clear();renderVariables();updateExportSelection()};

function renderPreview(ndjson,limit){previewRows=ndjson.split(/\r?\n/).filter(Boolean).map((line,i)=>{try{return JSON.parse(line)}catch{throw new Error(`Invalid preview data on line ${i+1}`)}});previewColumns=variables.length?variables.map(v=>v.var_name):[...new Set(previewRows.flatMap(Object.keys))];sort={column:null,direction:1};drawPreview();$("previewNote").textContent=`Showing ${previewRows.length.toLocaleString()} of up to ${Number(limit).toLocaleString()} rows · ${previewColumns.length.toLocaleString()} columns · Scroll horizontally to view all columns`;$("previewPanel").classList.remove("hidden")}
function drawPreview(){let rows=[...previewRows];if(sort.column){rows.sort((a,b)=>{const x=a[sort.column],y=b[sort.column];if(x==null)return y==null?0:1;if(y==null)return-1;return(typeof x==="number"&&typeof y==="number"?x-y:String(x).localeCompare(String(y),undefined,{numeric:true}))*sort.direction})} const hr=document.createElement("tr");for(const c of previewColumns){const info=variableTypes.get(c)||{kind:"numeric",label:"Unknown"},th=document.createElement("th"),b=document.createElement("button"),name=document.createElement("span"),type=document.createElement("small");th.classList.add("typed-header",`type-${info.kind}`);name.textContent=c+(sort.column===c?(sort.direction>0?" ▲":" ▼"):"");type.textContent=info.label;b.title=`Sort by ${c} (${info.label})`;b.onclick=()=>{sort=sort.column===c?{column:c,direction:-sort.direction}:{column:c,direction:1};drawPreview()};b.append(name,type);th.append(b);hr.append(th)}$("previewHead").replaceChildren(hr);$("previewBody").replaceChildren(...rows.map(row=>{const tr=document.createElement("tr");for(const c of previewColumns){const td=document.createElement("td");const x=row[c];td.textContent=x==null?"":typeof x==="object"?JSON.stringify(x):String(x);td.title=td.textContent;tr.append(td)}return tr}))}
function setPreviewExpanded(expanded){$("previewPanel").classList.toggle("expanded",expanded);document.body.classList.toggle("preview-expanded",expanded);$("expandPreview").textContent=expanded?"Exit expanded view":"Expand table";$("expandPreview").setAttribute("aria-expanded",String(expanded))}
$("expandPreview").onclick=()=>setPreviewExpanded(!$("previewPanel").classList.contains("expanded"));document.addEventListener("keydown",e=>{if(e.key==="Escape"&&$("previewPanel").classList.contains("expanded"))setPreviewExpanded(false)});
$("previewLimit").onchange=()=>{if(!currentFile)return;operationId++;busy(true);worker.postMessage({type:"preview",operationId,rowLimit:Number($("previewLimit").value)})};

function selectedRange(rowCap=Infinity,sql=false){
  const start=Number($("exportRowStart").value),requested=$("exportRowCount").value.trim();
  if(!Number.isInteger(start)||start<1||start>metadataRows){const message=`Start row must be between 1 and ${metadataRows.toLocaleString()}.`;sql?showSqlError(message):status(message,"error");return}
  const available=metadataRows-start+1,limit=requested===""?Math.min(available,rowCap):Number(requested);
  if(!Number.isInteger(limit)||limit<1||limit>available){const message=`Row count must be between 1 and ${available.toLocaleString()} for this start row.`;sql?showSqlError(message):status(message,"error");return}
  if(limit>rowCap){const message=`SQL input is limited to ${rowCap.toLocaleString()} rows; reduce Row count before loading.`;showSqlError(message);return}
  const columns=variables.map(v=>v.var_name).filter(name=>selectedColumns.has(name));
  if(!columns.length){const message="Select at least one variable.";sql?showSqlError(message):status(message,"error");return}
  return{columns,rowOffset:start-1,rowLimit:limit};
}
function sqlKey(selection){return JSON.stringify(selection)}
function updateSqlScope(){
  if(!config||!currentFile||!metadataRows)return;
  const start=Math.max(1,Number($("exportRowStart").value)||1),available=Math.max(0,metadataRows-start+1),requested=Number($("exportRowCount").value)||Math.min(available,config.sqlMaxRows),rows=Math.min(requested,available),columns=selectedColumns.size;
  const candidate={columns:variables.map(v=>v.var_name).filter(name=>selectedColumns.has(name)),rowOffset:start-1,rowLimit:rows},stale=sqlSelectionKey&&sqlSelectionKey!==sqlKey(candidate);
  $("sqlScope").textContent=`${rows.toLocaleString()} rows from row ${start.toLocaleString()} · ${columns.toLocaleString()} variables${stale?" · selection changed":""}`;
  $("sqlPolicy").textContent=currentFile.size>config.sqlMaxSourceBytes?`This experiment accepts source files up to ${formatBytes(config.sqlMaxSourceBytes)}.`:`An empty Row count loads at most ${config.sqlMaxRows.toLocaleString()} rows. SQL data and results remain in browser memory.`;
  $("sqlLoadButton").textContent=sqlSelectionKey?"Reload selection":"Load selection into SQL";
}
$("exportRowStart").addEventListener("input",updateSqlScope);
$("exportRowCount").addEventListener("input",updateSqlScope);

function showSqlError(message){$("sqlError").textContent=message||"";$("sqlError").classList.toggle("hidden",!message)}
function metric(label,contents){const box=document.createElement("div"),dt=document.createElement("dt"),dd=document.createElement("dd");dt.textContent=label;dd.textContent=contents;box.append(dt,dd);return box}
function renderSqlMetrics(queryMetrics=null){
  if(!sqlLoadMetrics)return;
  const entries=[["DuckDB startup",`${sqlLoadMetrics.engineMs.toFixed(0)} ms`],["SAS → Parquet",`${sqlLoadMetrics.parseMs.toFixed(0)} ms`],["DuckDB registration",`${sqlLoadMetrics.registerMs.toFixed(0)} ms`],["SQL input",`${sqlLoadMetrics.rows.toLocaleString()} rows · ${sqlLoadMetrics.columns.toLocaleString()} columns · ${formatBytes(sqlLoadMetrics.bytes)}`]];
  if(queryMetrics)entries.push(["First result batch",`${queryMetrics.firstBatchMs.toFixed(0)} ms`],["Query total",`${queryMetrics.totalMs.toFixed(0)} ms`],["Displayed",`${queryMetrics.rows.toLocaleString()} rows${queryMetrics.truncated?" (capped)":""}`],["Memory estimate",queryMetrics.memoryBytes?formatBytes(queryMetrics.memoryBytes):"Browser API unavailable"]);
  $("sqlMetrics").replaceChildren(...entries.map(([label,contents])=>metric(label,contents)));$("sqlMetrics").classList.remove("hidden");
}
async function initializeDuckDb(){
  if(duckdb)return 0;
  const started=performance.now();
  duckdbModule??=await import("./vendor/duckdb-browser.mjs");
  const base=new URL("./vendor/",import.meta.url),bundle=await duckdbModule.selectBundle({
    mvp:{mainModule:new URL("duckdb-mvp.wasm",base).href,mainWorker:new URL("duckdb-browser-mvp.worker.js",base).href},
    eh:{mainModule:new URL("duckdb-eh.wasm",base).href,mainWorker:new URL("duckdb-browser-eh.worker.js",base).href},
  });
  const nextWorker=new Worker(bundle.mainWorker),nextDatabase=new duckdbModule.AsyncDuckDB(new duckdbModule.VoidLogger(),nextWorker);
  try{await nextDatabase.instantiate(bundle.mainModule,bundle.pthreadWorker)}catch(error){try{await nextDatabase.terminate()}catch{nextWorker.terminate()}throw error}
  duckdb=nextDatabase;
  return performance.now()-started;
}
async function loadSqlData({output,selection}){
  const generation=++sqlGeneration,parseStarted=sqlLoadMetrics?.parseStarted||performance.now(),parseMs=performance.now()-parseStarted,bytes=output.byteLength,nextFile=`sas-explorer-selection-${generation}.parquet`;
  setSqlBusy(true);showSqlError("");status("Starting the local DuckDB engine…");
  let nextConnection=null,fileRegistered=false;
  try{
    const engineMs=await initializeDuckDb();if(generation!==sqlGeneration)return;
    const database=duckdb,previousConnection=sqlConnection,previousFile=sqlFileName;
    nextConnection=await database.connect();
    const registered=performance.now();
    await database.registerFileBuffer(nextFile,new Uint8Array(output));fileRegistered=true;
    await nextConnection.query(`create or replace view data as select * from read_parquet('${nextFile}')`);
    if(generation!==sqlGeneration)return;
    sqlConnection=nextConnection;nextConnection=null;sqlFileName=nextFile;sqlSelectionKey=sqlKey(selection);sqlLoadMetrics={engineMs,parseMs,registerMs:performance.now()-registered,bytes,rows:selection.rowLimit,columns:selection.columns.length};
    try{await previousConnection?.close()}catch{}try{if(previousFile)await database.dropFile(previousFile)}catch{}
    $("sqlWorkspace").classList.remove("hidden");$("sqlResults").classList.add("hidden");renderSqlMetrics();updateSqlScope();status(`SQL ready — ${selection.rowLimit.toLocaleString()} rows loaded locally`);setSqlBusy(false);
  }catch(error){if(generation!==sqlGeneration)return;showSqlError(error?.message||String(error));status("SQL data could not be loaded.","error");setSqlBusy(false)}
  finally{try{await nextConnection?.close()}catch{}try{if(fileRegistered&&nextFile!==sqlFileName)await duckdb?.dropFile(nextFile)}catch{}}
}
async function disposeSql(){
  sqlGeneration++;sqlSelectionKey="";sqlLoadMetrics=null;showSqlError("");$("sqlWorkspace").classList.add("hidden");$("sqlResults").classList.add("hidden");$("sqlMetrics").classList.add("hidden");
  const connection=sqlConnection,database=duckdb;sqlConnection=null;sqlFileName="";duckdb=null;setSqlBusy(false);
  try{await connection?.close()}catch{}
  try{await database?.terminate()}catch{}
}
function sqlValue(value){if(value==null)return"";if(typeof value==="bigint")return value.toString();if(value instanceof Date)return value.toISOString();if(typeof value==="object")return JSON.stringify(value);return String(value)}
function renderSqlResults(columns,rows,truncated){
  const header=document.createElement("tr");for(const column of columns){const th=document.createElement("th");th.textContent=column;header.append(th)}$("sqlResultHead").replaceChildren(header);
  $("sqlResultBody").replaceChildren(...rows.map(row=>{const tr=document.createElement("tr");for(const cell of row){const td=document.createElement("td");td.textContent=cell;td.title=cell;tr.append(td)}return tr}));
  $("sqlResultNote").textContent=`Showing ${rows.length.toLocaleString()} row${rows.length===1?"":"s"}${truncated?` · display capped at ${config.sqlResultRows.toLocaleString()} rows`:""}`;$("sqlResults").classList.remove("hidden");
}
async function memoryEstimate(){try{return typeof performance.measureUserAgentSpecificMemory==="function"?(await performance.measureUserAgentSpecificMemory()).bytes:null}catch{return null}}
async function runSql(){
  let query=$("sqlQuery").value.trim().replace(/;\s*$/,"");
  if(!query){showSqlError("Enter a SQL query.");return}if(query.includes(";")||!/^\s*(select|with)\b/i.test(query)){showSqlError("Only one read-only SELECT or WITH query is allowed.");return}
  const generation=sqlGeneration,started=performance.now(),limit=config.sqlResultRows,wrapped=`select * from (${query}) as explorer_result limit ${limit+1}`;setSqlBusy(true,true);showSqlError("");$("sqlResults").classList.add("hidden");status("Running SQL locally…");
  try{
    const reader=await sqlConnection.send(wrapped,true),columns=[],rows=[];let firstBatchMs=null;
    for await(const batch of reader){firstBatchMs??=performance.now()-started;if(!columns.length)columns.push(...batch.schema.fields.map(field=>field.name));for(let row=0;row<batch.numRows&&rows.length<=limit;row++)rows.push(columns.map((_,column)=>sqlValue(batch.getChildAt(column).get(row))))}
    if(generation!==sqlGeneration)return;const truncated=rows.length>limit;if(truncated)rows.length=limit;const queryMetrics={firstBatchMs:firstBatchMs??performance.now()-started,totalMs:performance.now()-started,rows:rows.length,truncated,memoryBytes:await memoryEstimate()};renderSqlResults(columns,rows,truncated);renderSqlMetrics(queryMetrics);status(`SQL complete — ${rows.length.toLocaleString()} rows displayed`);setSqlBusy(false);
  }catch(error){if(generation!==sqlGeneration)return;showSqlError(error?.message||String(error));status("SQL query failed.","error");setSqlBusy(false)}
}
$("sqlLoadButton").onclick=()=>{const selection=selectedRange(config.sqlMaxRows,true);if(!selection||isBusy)return;showSqlError("");sqlLoadMetrics={parseStarted:performance.now()};setSqlBusy(true);operationId++;busy(true);worker.postMessage({type:"sql-load",operationId,selection})};
$("sqlRunButton").onclick=()=>void runSql();
$("sqlCancelButton").onclick=async()=>{try{await sqlConnection?.cancelSent();status("Cancelling SQL query…")}catch(error){showSqlError(error?.message||String(error))}};

function downloadExport({output,filename,mime}){const url=URL.createObjectURL(new Blob([output],{type:mime})),link=document.createElement("a");link.href=url;link.download=filename;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);status(`Downloaded ${filename}`)}
$("exportButton").onclick=()=>{if(!currentFile||isBusy)return;const selected=selectedRange();if(!selected)return;const reduced=selected.columns.length!==variables.length||selected.rowOffset!==0||selected.rowLimit!==metadataRows,selection=reduced?selected:null;operationId++;busy(true);worker.postMessage({type:"export",operationId,format:$("exportFormat").value,sourceName:currentFile.name,selection})};
