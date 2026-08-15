// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

// Use mimalloc as the global allocator. `override` is intentionally not
// enabled; the fixes here target macOS ARM, where symbol interposition can
// break fork/atfork handlers.
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    mcphub_lib::run();
}
