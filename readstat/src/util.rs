use std::error::Error;
use std::ffi::CString;
use std::path::Path;

#[cfg(unix)]
pub fn path_to_cstring(path: &Path) -> Result<CString, Box<dyn Error>> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    CString::new(bytes).map_err(|_| From::from("Invalid path"))
}

#[cfg(not(unix))]
pub fn path_to_cstring(path: &Path) -> Result<CString, Box<dyn Error>> {
    let rust_str = path
        .as_os_str()
        .as_str()
        .ok_or(Err(From::from("Invalid path")))?;
    let bytes = path.as_os_str().as_bytes();
    CString::new(rust_str).map_err(|_| From::from("Invalid path"))
}
