//! TLS certificate pinning helpers.

use std::fmt;
use std::sync::Arc;

use base64::Engine;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use sha2::{Digest, Sha256};
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::common::config_types::TlsConfig;
use crate::common::error::{FlareError, Result};

#[derive(Clone, Debug, Default)]
pub struct TlsPinningPolicy {
    spki_sha256_pins: Vec<Vec<u8>>,
    certificate_sha256_pins: Vec<Vec<u8>>,
}

impl TlsPinningPolicy {
    pub fn from_tls_config(tls: &TlsConfig) -> Result<Self> {
        let spki_sha256_pins = normalize_pin_list(&tls.spki_sha256_pins, "SPKI SHA-256")?;
        let certificate_sha256_pins =
            normalize_pin_list(&tls.certificate_sha256_pins, "certificate SHA-256")?;
        Ok(Self {
            spki_sha256_pins,
            certificate_sha256_pins,
        })
    }

    pub fn is_enabled(&self) -> bool {
        !self.spki_sha256_pins.is_empty() || !self.certificate_sha256_pins.is_empty()
    }

    pub fn verify_chain(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
    ) -> std::result::Result<(), String> {
        if !self.spki_sha256_pins.is_empty()
            && certificate_chain_der(end_entity, intermediates).any(|certificate| {
                spki_sha256(certificate.as_ref())
                    .map(|actual| self.spki_sha256_pins.iter().any(|pin| pin == &actual))
                    .unwrap_or(false)
            })
        {
            return Ok(());
        }

        if !self.certificate_sha256_pins.is_empty()
            && certificate_chain_der(end_entity, intermediates).any(|certificate| {
                let actual = Sha256::digest(certificate.as_ref()).to_vec();
                self.certificate_sha256_pins
                    .iter()
                    .any(|pin| pin == &actual)
            })
        {
            return Ok(());
        }

        let leaf_spki = spki_sha256(end_entity.as_ref())
            .map(|hash| format!("spki-sha256/{}", base64_pin(&hash)))
            .unwrap_or_else(|err| format!("spki-unavailable({err})"));
        Err(format!("TLS certificate pin mismatch; leaf {leaf_spki}"))
    }
}

#[derive(Debug)]
pub struct PinnedServerCertVerifier {
    delegate: Arc<dyn ServerCertVerifier>,
    policy: TlsPinningPolicy,
}

impl PinnedServerCertVerifier {
    pub fn new(delegate: Arc<dyn ServerCertVerifier>, policy: TlsPinningPolicy) -> Self {
        Self { delegate, policy }
    }
}

impl ServerCertVerifier for PinnedServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        self.delegate.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;

        self.policy
            .verify_chain(end_entity, intermediates)
            .map_err(|_| {
                RustlsError::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
            })?;

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        self.delegate.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        self.delegate.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.delegate.supported_verify_schemes()
    }
}

pub fn normalize_sha256_pin(pin: &str) -> Option<Vec<u8>> {
    let pin = pin.trim();
    let pin = pin
        .strip_prefix("spki-sha256/")
        .or_else(|| pin.strip_prefix("sha256/"))
        .unwrap_or(pin);
    let hex_candidate: String = pin.chars().filter(|ch| *ch != ':').collect();

    if hex_candidate.len() == 64 && hex_candidate.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return hex::decode(hex_candidate).ok();
    }

    base64::engine::general_purpose::STANDARD.decode(pin).ok()
}

pub fn spki_sha256(certificate_der: &[u8]) -> std::result::Result<Vec<u8>, String> {
    let (_, certificate) = X509Certificate::from_der(certificate_der)
        .map_err(|error| format!("parse certificate DER failed: {error}"))?;
    Ok(Sha256::digest(certificate.tbs_certificate.subject_pki.raw).to_vec())
}

pub fn spki_sha256_pin(certificate_der: &[u8]) -> std::result::Result<String, String> {
    spki_sha256(certificate_der).map(|hash| format!("spki-sha256/{}", base64_pin(&hash)))
}

fn normalize_pin_list(raw_pins: &[String], label: &str) -> Result<Vec<Vec<u8>>> {
    raw_pins
        .iter()
        .map(|pin| {
            normalize_sha256_pin(pin)
                .ok_or_else(|| FlareError::protocol_error(format!("invalid {label} pin: {pin}")))
        })
        .collect()
}

fn base64_pin(hash: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(hash)
}

fn certificate_chain_der<'a>(
    end_entity: &'a CertificateDer<'a>,
    intermediates: &'a [CertificateDer<'a>],
) -> impl Iterator<Item = &'a CertificateDer<'a>> {
    std::iter::once(end_entity).chain(intermediates.iter())
}

impl fmt::Display for TlsPinningPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsPinningPolicy")
            .field("spki_sha256_pins", &self.spki_sha256_pins.len())
            .field(
                "certificate_sha256_pins",
                &self.certificate_sha256_pins.len(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_normalization_accepts_base64_and_hex() {
        let bytes = [7u8; 32];
        let base64_pin = format!(
            "spki-sha256/{}",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        );
        let legacy_base64_pin = format!(
            "sha256/{}",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        );
        let hex_pin = hex::encode(bytes);
        let colon_hex_pin = hex_pin
            .as_bytes()
            .chunks(2)
            .map(|chunk| std::str::from_utf8(chunk).unwrap())
            .collect::<Vec<_>>()
            .join(":");

        assert_eq!(normalize_sha256_pin(&base64_pin), Some(bytes.to_vec()));
        assert_eq!(
            normalize_sha256_pin(&legacy_base64_pin),
            Some(bytes.to_vec())
        );
        assert_eq!(normalize_sha256_pin(&hex_pin), Some(bytes.to_vec()));
        assert_eq!(normalize_sha256_pin(&colon_hex_pin), Some(bytes.to_vec()));
    }

    #[test]
    fn spki_pin_is_stable_for_generated_certificate() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate certificate");
        let cert_der = certified.cert.der().to_vec();

        let pin = spki_sha256_pin(&cert_der).expect("spki pin");

        assert!(pin.starts_with("spki-sha256/"));
        assert_eq!(normalize_sha256_pin(&pin).expect("normalize").len(), 32);
    }

    #[test]
    fn policy_matches_leaf_spki_pin() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate certificate");
        let cert_der = certified.cert.der().to_vec();
        let pin = spki_sha256_pin(&cert_der).expect("spki pin");
        let policy =
            TlsPinningPolicy::from_tls_config(&TlsConfig::none().with_spki_sha256_pin(pin))
                .expect("policy");
        let certificate = CertificateDer::from(cert_der);

        policy
            .verify_chain(&certificate, &[])
            .expect("matching pin");
    }

    #[test]
    fn policy_rejects_non_matching_spki_pin() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate certificate");
        let cert_der = certified.cert.der().to_vec();
        let policy =
            TlsPinningPolicy::from_tls_config(&TlsConfig::none().with_spki_sha256_pin(format!(
                "spki-sha256/{}",
                base64::engine::general_purpose::STANDARD.encode([9u8; 32])
            )))
            .expect("policy");
        let certificate = CertificateDer::from(cert_der);

        policy
            .verify_chain(&certificate, &[])
            .expect_err("non-matching pin");
    }
}
