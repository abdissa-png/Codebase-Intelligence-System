//! MCP stdio entry — prefers **`cisd --mcp`** (single process) unless `CIS_STANDALONE_MCP=1`.

use std::io;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;

fn sibling_cisd() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("cisd")))
        .filter(|p| p.is_file())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--tools" || a == "tools") {
        cis_mcp::dump_tool_names();
        return Ok(());
    }

    let standalone = std::env::var_os("CIS_STANDALONE_MCP").is_some_and(|v| v == "1");
    if !standalone {
        if let Some(cisd) = sibling_cisd() {
            let err = Command::new(&cisd)
                .arg("--mcp")
                .envs(std::env::vars())
                .exec();
            return Err(io::Error::other(format!(
                "cis-mcp: failed to exec {} --mcp: {}",
                cisd.display(),
                err
            )));
        }
        eprintln!("cis-mcp: cisd not found beside binary; running standalone (set CIS_STANDALONE_MCP=1 to silence)");
    }

    let rt = cis_mcp::build_runtime(None)?;
    cis_mcp::run_stdio(rt)
}
