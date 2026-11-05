/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use allocative::Allocative;
use buck2_core::soft_error;
use buck2_error::ErrorTag;
use buck2_error::TypedContext;
use remote_execution::REClientError;
use remote_execution::TCode;
use remote_execution::TCodeReasonGroup;

pub fn get_re_error_tag(tcode: &TCode) -> ErrorTag {
    match *tcode {
        TCode::CANCELLED => ErrorTag::ReCancelled,
        TCode::UNKNOWN => ErrorTag::ReUnknown,
        TCode::INVALID_ARGUMENT => ErrorTag::ReInvalidArgument,
        TCode::DEADLINE_EXCEEDED => ErrorTag::ReDeadlineExceeded,
        TCode::NOT_FOUND => ErrorTag::ReNotFound,
        TCode::ALREADY_EXISTS => ErrorTag::ReAlreadyExists,
        TCode::PERMISSION_DENIED => ErrorTag::RePermissionDenied,
        TCode::RESOURCE_EXHAUSTED => ErrorTag::ReResourceExhausted,
        TCode::FAILED_PRECONDITION => ErrorTag::ReFailedPrecondition,
        TCode::ABORTED => ErrorTag::ReAborted,
        TCode::OUT_OF_RANGE => ErrorTag::ReOutOfRange,
        TCode::UNIMPLEMENTED => ErrorTag::ReUnimplemented,
        TCode::INTERNAL => ErrorTag::ReInternal,
        TCode::UNAVAILABLE => ErrorTag::ReUnavailable,
        TCode::DATA_LOSS => ErrorTag::ReDataLoss,
        TCode::UNAUTHENTICATED => ErrorTag::ReUnauthenticated,
        _ => ErrorTag::ReUnknownTcode,
    }
}

pub fn get_re_group_tag(group: &TCodeReasonGroup) -> Option<ErrorTag> {
    match *group {
        TCodeReasonGroup::RE_CONNECTION => Some(ErrorTag::ReConnection),
        TCodeReasonGroup::USER_QUOTA => Some(ErrorTag::ReUserQuota),
        TCodeReasonGroup::USER_BAD_CERTS => Some(ErrorTag::ReUserBadCerts),
        _ => None,
    }
}

#[derive(Allocative, Debug, Clone, buck2_error::Error)]
#[error("Remote Execution Error on {} for ReSession {}\nError: ({})", .re_action, .re_session_id, .message)]
#[buck2(tag = get_re_error_tag(code))]
pub struct RemoteExecutionError {
    re_action: String,
    re_session_id: String,
    pub message: String,
    #[allocative(skip)]
    pub code: TCode,
    #[allocative(skip)]
    pub group: TCodeReasonGroup,
}

impl TypedContext for RemoteExecutionError {
    fn eq(&self, other: &dyn TypedContext) -> bool {
        match (other as &dyn std::any::Any).downcast_ref::<Self>() {
            Some(right) => self.eq(right),
            None => false,
        }
    }

    fn display(&self) -> Option<String> {
        None
    }
}

fn re_error(
    re_action: &str,
    re_session_id: &str,
    message: String,
    code: TCode,
    group: TCodeReasonGroup,
) -> buck2_error::Error {
    let re_error = RemoteExecutionError {
        re_action: re_action.to_owned(),
        re_session_id: re_session_id.to_owned(),
        message,
        code,
        group,
    };
    let error = buck2_error::Error::from(re_error.clone()).context(re_error);
    if let Some(tag) = get_re_group_tag(&group) {
        error.tag([tag])
    } else {
        error.string_tag(&group.to_string())
    }
}

/// Only structured missing-input failures qualify for native CAS recovery.
/// In OSS, the RE client sets this reason group from REAPI PreconditionFailure,
/// not from a status message or from FAILED_PRECONDITION alone.
pub fn is_missing_cas_input(error: &buck2_error::Error) -> bool {
    error
        .find_typed_context::<RemoteExecutionError>()
        .is_some_and(|error| {
            error.code == TCode::FAILED_PRECONDITION
                && error.group == TCodeReasonGroup::DIGEST_NOT_FOUND
        })
}

/// Reuse the native, single-restart recovery boundary. A new daemon discards
/// stale DICE values and presence leases; cache candidates are checked against
/// CAS before their outputs can be accepted again.
pub fn report_cas_corruption(
    error: buck2_error::Error,
    action_cache_is_corrupted: bool,
) -> buck2_error::Error {
    soft_error!(
        "cas_missing_recovery",
        error.clone(),
        daemon_in_memory_state_is_corrupted: true,
        action_cache_is_corrupted: action_cache_is_corrupted
    )
    .err()
    .unwrap_or(error)
}

pub(crate) fn with_error_handler<T>(
    re_action: &str,
    re_session_id: &str,
    result: anyhow::Result<T>,
) -> buck2_error::Result<T> {
    match result {
        Ok(val) => Ok(val),
        Err(e) => {
            let (code, group) = e
                .downcast_ref::<REClientError>()
                .map_or((TCode::UNKNOWN, TCodeReasonGroup::UNKNOWN), |e| {
                    (e.code, e.group)
                });

            Err(re_error(
                re_action,
                re_session_id,
                format!("{e:#}"),
                code,
                group,
            ))
        }
    }
}

pub fn test_re_error(message: &str, code: TCode) -> buck2_error::Error {
    test_re_error_with_group(message, code, TCodeReasonGroup::UNKNOWN)
}

pub(crate) fn test_re_error_with_group(
    message: &str,
    code: TCode,
    group: TCodeReasonGroup,
) -> buck2_error::Error {
    re_error("test", "test", message.to_owned(), code, group)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_re_error() {
        let error: buck2_error::Error = re_error(
            "test",
            "test",
            "test".to_owned(),
            TCode::UNKNOWN,
            TCodeReasonGroup::UNKNOWN,
        );

        let err = error.find_typed_context::<RemoteExecutionError>().unwrap();
        assert_eq!(err.code, TCode::UNKNOWN);
    }

    #[test]
    fn test_missing_cas_requires_typed_reason_and_code() {
        let error = with_error_handler::<()>(
            "execute",
            "test",
            Err(REClientError {
                code: TCode::FAILED_PRECONDITION,
                group: TCodeReasonGroup::DIGEST_NOT_FOUND,
                message: "arbitrary server wording".to_owned(),
            }
            .into()),
        )
        .unwrap_err();
        assert!(is_missing_cas_input(&error));
        for (code, group) in [
            (TCode::FAILED_PRECONDITION, TCodeReasonGroup::UNKNOWN),
            (TCode::UNAUTHENTICATED, TCodeReasonGroup::DIGEST_NOT_FOUND),
            (TCode::PERMISSION_DENIED, TCodeReasonGroup::DIGEST_NOT_FOUND),
            (TCode::NOT_FOUND, TCodeReasonGroup::UNKNOWN),
        ] {
            assert!(!is_missing_cas_input(&test_re_error_with_group(
                "action input missing from CAS",
                code,
                group,
            )));
        }
    }
}
