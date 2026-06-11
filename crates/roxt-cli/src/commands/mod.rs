//! One module per subcommand. Each `run` validates all arguments before
//! performing any IO and returns the process exit code.

pub mod diff;
pub mod query;
pub mod rec;
pub mod replay;
