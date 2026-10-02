//! 服务端踢线（KICKED 系统命令）的类型化原因约定。
//!
//! KICKED 的 `message` 与 metadata `reason` 都是类型化原因（大写下划线，如 [`DEVICE_CONFLICT_REASON`]），
//! 客户端按它决定被踢之后怎么办，不必解析给人看的文本。客户端上报给观察者的断开原因就是这个类型化
//! 原因本身。
//!
//! 有的原因表示这次会话已经结束（[`ends_session`]）：
//!
//! - [`DEVICE_CONFLICT_REASON`]：同一用户同一平台的新设备登录，旧设备被顶下线（「已在别处登录」）；
//! - [`TENANT_UNAVAILABLE_REASON`]：所属租户已停用或正在删除。
//!
//! 客户端收到这类踢线后进入终态：不自动重连（包括发送前的重连），由应用重新登录后再显式建连。
//! 自动重连回去只会把新设备再顶下线，两台设备来回互踢；租户不可用时重连也只会被再拒一次。
//!
//! 兼容：flare-core 1.1.1 及更早的服务端，设备冲突的 KICKED 带一段中文说明作 `message`、metadata
//! `reason` 为小写的 `device_conflict`；[`kick_reason`] 把它同样识别为 [`DEVICE_CONFLICT_REASON`]。
//! 旧客户端读新服务端的帧时，`message` 是 `DEVICE_CONFLICT`，按子串找 `device_conflict`（不分大小写）
//! 的旧逻辑照样认得。

use crate::common::device::DevicePlatform;
use crate::common::protocol::SystemCommand;
use crate::common::protocol::builder::kicked;
use crate::common::protocol::flare::core::commands::system_command::Type as SystemType;
use std::collections::HashMap;

/// 设备冲突：同一用户同一平台的新设备登录，旧设备被踢下线。
pub const DEVICE_CONFLICT_REASON: &str = "DEVICE_CONFLICT";

/// 服务端因租户不可用（停用、删除中）拒绝 CONNECT 或踢线时给出的类型化原因。
///
/// CONNECT 被拒时它映射成 `PERMISSION_DENIED`，与 HTTP 入口、消息摄入对同一原因的错误码一致；
/// 它不是鉴权失败，上层不应为它刷新令牌或自动重连。
pub const TENANT_UNAVAILABLE_REASON: &str = "TENANT_UNAVAILABLE";

/// 旧服务端写在设备冲突 KICKED 的 metadata `reason` 里的值。
const LEGACY_DEVICE_CONFLICT_REASON: &str = "device_conflict";

/// 原因里没有可用的类型化原因时，客户端上报的原因。
const UNSPECIFIED_KICK_REASON: &str = "KICKED";

/// 设备冲突时下发给旧连接的 KICKED：`message` 与 metadata `reason` 都是 [`DEVICE_CONFLICT_REASON`]，
/// metadata 另带新设备的平台（`platform`）与设备 ID（`new_device_id`）。不带用户 ID 等账号信息。
pub fn device_conflict_kicked(platform: &DevicePlatform, new_device_id: &str) -> SystemCommand {
    let metadata = HashMap::from([
        (
            "reason".to_string(),
            DEVICE_CONFLICT_REASON.as_bytes().to_vec(),
        ),
        (
            "platform".to_string(),
            format!("{platform:?}").as_bytes().to_vec(),
        ),
        (
            "new_device_id".to_string(),
            new_device_id.as_bytes().to_vec(),
        ),
    ]);
    kicked(DEVICE_CONFLICT_REASON, Some(metadata))
}

/// 这条 KICKED 是否因设备冲突：`message` 是 [`DEVICE_CONFLICT_REASON`]，或 metadata `reason` 是它
/// （不分大小写，旧服务端写的是小写的 `device_conflict`）。
pub fn is_device_conflict_kick(command: &SystemCommand) -> bool {
    command.r#type == SystemType::Kicked as i32
        && (is_device_conflict_reason(&command.message)
            || command
                .metadata
                .get("reason")
                .and_then(|reason| std::str::from_utf8(reason).ok())
                .is_some_and(|reason| {
                    reason
                        .trim()
                        .eq_ignore_ascii_case(LEGACY_DEVICE_CONFLICT_REASON)
                }))
}

/// KICKED 的类型化原因，客户端以它作为断开原因上报。
///
/// 设备冲突（含旧服务端的写法）一律是 [`DEVICE_CONFLICT_REASON`]；其余取 `message`，`message`
/// 为空时取 metadata `reason`，都没有时为 `KICKED`。
pub fn kick_reason(command: &SystemCommand) -> String {
    if is_device_conflict_kick(command) {
        return DEVICE_CONFLICT_REASON.to_string();
    }
    let message = command.message.trim();
    if !message.is_empty() {
        return message.to_string();
    }
    command
        .metadata
        .get("reason")
        .and_then(|reason| std::str::from_utf8(reason).ok())
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .unwrap_or(UNSPECIFIED_KICK_REASON)
        .to_string()
}

/// 断开原因是否为设备冲突：原因就是 [`DEVICE_CONFLICT_REASON`]，或以 `DEVICE_CONFLICT:` 开头。
pub fn is_device_conflict_reason(reason: &str) -> bool {
    has_reason_token(reason, DEVICE_CONFLICT_REASON)
}

/// 这个踢线原因是否结束了会话：设备冲突或租户不可用（原因本身，或以 `原因:` 开头）。
/// 收到它的客户端不再自动重连，由应用重新登录。
pub fn ends_session(reason: &str) -> bool {
    has_reason_token(reason, DEVICE_CONFLICT_REASON)
        || has_reason_token(reason, TENANT_UNAVAILABLE_REASON)
}

fn has_reason_token(reason: &str, token: &str) -> bool {
    reason
        .trim()
        .strip_prefix(token)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_conflict_kick_carries_the_typed_reason_without_account_details() {
        let command = device_conflict_kicked(&DevicePlatform::PC, "new-device");
        assert_eq!(command.r#type, SystemType::Kicked as i32);
        assert_eq!(command.message, DEVICE_CONFLICT_REASON);
        assert_eq!(
            command.metadata.get("reason").map(Vec::as_slice),
            Some(DEVICE_CONFLICT_REASON.as_bytes())
        );
        assert_eq!(
            command.metadata.get("new_device_id").map(Vec::as_slice),
            Some("new-device".as_bytes())
        );
        assert_eq!(
            command.metadata.get("platform").map(Vec::as_slice),
            Some("PC".as_bytes())
        );
        assert!(is_device_conflict_kick(&command));
        assert_eq!(kick_reason(&command), DEVICE_CONFLICT_REASON);
    }

    /// 旧服务端的设备冲突 KICKED：中文说明作 message、metadata reason 为小写 device_conflict。
    #[test]
    fn legacy_device_conflict_kicks_are_recognised() {
        let metadata = HashMap::from([(
            "reason".to_string(),
            LEGACY_DEVICE_CONFLICT_REASON.as_bytes().to_vec(),
        )]);
        let command = kicked(
            "设备冲突：同一用户 (u1) 的同一平台 (PC) 已有其他设备在线，当前设备将被踢下线",
            Some(metadata),
        );
        assert!(is_device_conflict_kick(&command));
        assert_eq!(kick_reason(&command), DEVICE_CONFLICT_REASON);
    }

    #[test]
    fn other_kicks_keep_their_own_reason() {
        let tenant = kicked(TENANT_UNAVAILABLE_REASON, None);
        assert!(!is_device_conflict_kick(&tenant));
        assert_eq!(kick_reason(&tenant), TENANT_UNAVAILABLE_REASON);

        let admin = kicked(
            "",
            Some(HashMap::from([(
                "reason".to_string(),
                b"DEVICE_KICKED".to_vec(),
            )])),
        );
        assert_eq!(kick_reason(&admin), "DEVICE_KICKED");
        assert_eq!(kick_reason(&kicked("", None)), UNSPECIFIED_KICK_REASON);

        // 其他系统命令带同样的原因不算踢线
        let close =
            crate::common::protocol::builder::close(Some(DEVICE_CONFLICT_REASON.to_string()), None);
        assert!(!is_device_conflict_kick(&close));
    }

    #[test]
    fn session_ending_reasons_are_matched_by_token() {
        for reason in [
            "DEVICE_CONFLICT",
            " DEVICE_CONFLICT ",
            "DEVICE_CONFLICT: kicked by the server",
            "TENANT_UNAVAILABLE",
            "TENANT_UNAVAILABLE: server rejected CONNECT",
        ] {
            assert!(ends_session(reason), "{reason}");
        }
        for reason in [
            "DEVICE_KICKED",
            "SESSION_REPLACED",
            "SERVER_SHUTDOWN",
            "DEVICE_CONFLICTS",
            "Closed by client",
            "WebSocket connection closed by peer",
            "",
        ] {
            assert!(!ends_session(reason), "{reason}");
        }
        assert!(is_device_conflict_reason("DEVICE_CONFLICT"));
        assert!(!is_device_conflict_reason("TENANT_UNAVAILABLE"));
    }
}
