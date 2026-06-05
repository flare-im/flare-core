//! Compile-time feature capability matrix.
//!
//! This module intentionally describes `flare-core` transport capabilities, not
//! IM/Social product behavior. It gives applications and diagnostics a stable
//! way to inspect what this crate was built to expose.

/// Capabilities enabled for the current build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureSet {
    /// Native target build.
    pub native: bool,
    /// WASM target build.
    pub wasm: bool,
    /// Client-side transport stack is enabled.
    pub client: bool,
    /// Server-side transport stack is enabled.
    pub server: bool,
    /// WebSocket transport is enabled for this target.
    pub websocket_transport: bool,
    /// QUIC transport is enabled for this target.
    pub quic_transport: bool,
    /// Hybrid transport racing is available.
    pub hybrid_transport: bool,
    /// Built-in Gzip compression is enabled.
    pub gzip_compression: bool,
    /// Built-in AES-256-GCM encryption is enabled.
    pub aes_256_gcm_encryption: bool,
    /// Raw TCP transport is enabled (native only).
    pub tcp_transport: bool,
}

impl FeatureSet {
    /// Returns the capabilities for the current crate build.
    pub const fn current() -> Self {
        let native = cfg!(not(target_arch = "wasm32"));
        let wasm = cfg!(target_arch = "wasm32");
        let client = cfg!(feature = "client");
        let server = cfg!(all(feature = "server", not(target_arch = "wasm32")));
        let websocket_transport = cfg!(feature = "websocket");
        let quic_transport = cfg!(all(feature = "quic", not(target_arch = "wasm32")));
        let tcp_transport = cfg!(all(feature = "tcp", not(target_arch = "wasm32")));
        let hybrid_transport = cfg!(all(
            feature = "websocket",
            feature = "quic",
            not(target_arch = "wasm32")
        ));

        Self {
            native,
            wasm,
            client,
            server,
            websocket_transport,
            quic_transport,
            hybrid_transport,
            gzip_compression: cfg!(feature = "compression-gzip"),
            aes_256_gcm_encryption: cfg!(feature = "encryption-aes-gcm"),
            tcp_transport,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FeatureSet;

    #[test]
    fn current_feature_set_matches_compile_time_cfg() {
        let features = FeatureSet::current();

        assert_eq!(features.native, cfg!(not(target_arch = "wasm32")));
        assert_eq!(features.wasm, cfg!(target_arch = "wasm32"));
        assert_eq!(features.client, cfg!(feature = "client"));
        assert_eq!(
            features.server,
            cfg!(all(feature = "server", not(target_arch = "wasm32")))
        );
        assert_eq!(features.websocket_transport, cfg!(feature = "websocket"));
        assert_eq!(
            features.quic_transport,
            cfg!(all(feature = "quic", not(target_arch = "wasm32")))
        );
        assert_eq!(
            features.hybrid_transport,
            cfg!(all(
                feature = "websocket",
                feature = "quic",
                not(target_arch = "wasm32")
            ))
        );
        assert_eq!(
            features.gzip_compression,
            cfg!(feature = "compression-gzip")
        );
        assert_eq!(
            features.aes_256_gcm_encryption,
            cfg!(feature = "encryption-aes-gcm")
        );
        assert_eq!(
            features.tcp_transport,
            cfg!(all(feature = "tcp", not(target_arch = "wasm32")))
        );
    }
}
