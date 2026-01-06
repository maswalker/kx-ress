use clap::{Args, Parser};
use kasplex_reth_chainspec::{parser::KasplexChainSpecParser, spec::KasplexChainSpec};
use reth_chainspec::Chain;
use reth_cli::chainspec::ChainSpecParser;
use reth_discv4::{DEFAULT_DISCOVERY_ADDR, DEFAULT_DISCOVERY_PORT};
use reth_network_peers::TrustedPeer;
use reth_node_core::{
    dirs::{ChainPath, PlatformPath, XdgPath},
};
use std::{
    env::VarError,
    fmt,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

/// Ress CLI interface.
#[derive(Clone, Debug, Parser)]
#[command(author, version, about = "Ress", long_about = None)]
pub struct RessArgs {
    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        long_help = KasplexChainSpecParser::help_message(),
        default_value = KasplexChainSpecParser::SUPPORTED_CHAINS[0],
        value_parser = KasplexChainSpecParser::parser()
    )]
    pub chain: Arc<KasplexChainSpec>,

    /// The path to the data dir for all ress files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/ress/` or `$HOME/.local/share/ress/`
    /// - Windows: `{FOLDERID_RoamingAppData}/ress/`
    /// - macOS: `$HOME/Library/Application Support/ress/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// Network args.
    #[clap(flatten)]
    pub network: RessNetworkArgs,
}

/// Ress networking args.
#[derive(Clone, Debug, Args)]
pub struct RessNetworkArgs {
    /// Network listening address
    #[arg(long = "addr", value_name = "ADDR", default_value_t = DEFAULT_DISCOVERY_ADDR)]
    pub addr: IpAddr,

    /// Network listening port
    #[arg(long = "port", value_name = "PORT", default_value_t = DEFAULT_DISCOVERY_PORT)]
    pub port: u16,

    /// Secret key to use for this node.
    ///
    /// This will also deterministically set the peer ID. If not specified, it will be set in the
    /// data dir for the chain being used.
    #[arg(long, value_name = "PATH")]
    pub p2p_secret_key: Option<PathBuf>,

    /// Maximum active connections for `ress` subprotocol.
    #[arg(long, default_value_t = 256)]
    pub max_active_connections: u64,

    #[allow(clippy::doc_markdown)]
    /// Comma separated enode URLs of trusted peers for P2P connections.
    ///
    /// --remote-peer enode://abcd@192.168.0.1:30303
    #[arg(long, value_delimiter = ',')]
    pub trusted_peers: Vec<TrustedPeer>,
}

impl RessNetworkArgs {
    /// Returns network socket address.
    pub fn listener_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr, self.port)
    }

    /// Returns path to network secret.
    pub fn network_secret_path(&self, data_dir: &ChainPath<DataDirPath>) -> PathBuf {
        self.p2p_secret_key.clone().unwrap_or_else(|| data_dir.p2p_secret())
    }
}

/// Returns the path to the ress data dir.
///
/// The data dir should contain a subdirectory for each chain, and those chain directories will
/// include all information for that chain, such as the p2p secret.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct DataDirPath;

impl XdgPath for DataDirPath {
    fn resolve() -> Option<PathBuf> {
        data_dir()
    }
}

/// Returns the path to the ress data directory.
///
/// Refer to [`dirs_next::data_dir`] for cross-platform behavior.
pub fn data_dir() -> Option<PathBuf> {
    dirs_next::data_dir().map(|root| root.join("ress"))
}

/// Returns the path to the ress database.
///
/// Refer to [`dirs_next::data_dir`] for cross-platform behavior.
pub fn database_path() -> Option<PathBuf> {
    data_dir().map(|root| root.join("db"))
}

/// An Optional wrapper type around [`PlatformPath`].
///
/// This is useful for when a path is optional, such as the `--data-dir` flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaybePlatformPath<D>(Option<PlatformPath<D>>);

// === impl MaybePlatformPath ===

impl<D: XdgPath> MaybePlatformPath<D> {
    /// Returns the path if it is set, otherwise returns the default path for the given chain.
    pub fn unwrap_or_chain_default(&self, chain: Chain) -> ChainPath<D> {
        ChainPath::new(
            self.0.clone().unwrap_or_else(|| PlatformPath::default().join(chain.to_string())),
            chain,
            Default::default(),
        )
    }
}

impl<D: XdgPath> fmt::Display for MaybePlatformPath<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(path) = &self.0 {
            path.fmt(f)
        } else {
            // NOTE: this is a workaround for making it work with clap's `default_value_t` which
            // computes the default value via `Default -> Display -> FromStr`
            write!(f, "default")
        }
    }
}

impl<D> Default for MaybePlatformPath<D> {
    fn default() -> Self {
        Self(None)
    }
}

impl<D> FromStr for MaybePlatformPath<D> {
    type Err = shellexpand::LookupError<VarError>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let p = match s {
            "default" => {
                // NOTE: this is a workaround for making it work with clap's `default_value_t` which
                // computes the default value via `Default -> Display -> FromStr`
                None
            }
            _ => Some(PlatformPath::from_str(s)?),
        };
        Ok(Self(p))
    }
}

// impl<D> From<PathBuf> for MaybePlatformPath<D> {
//     fn from(path: PathBuf) -> Self {
//         Self(Some(PlatformPath(path, std::marker::PhantomData)))
//     }
// }
