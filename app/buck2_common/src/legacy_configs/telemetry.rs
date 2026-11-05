/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Redaction at logging boundaries, never on configuration used for execution.
//! All values are private: `$(config section.key)` can obtain credentials from
//! innocently named settings. Names, scope and source paths remain diagnostic;
//! neither literal values nor value-derived hashes belong in native telemetry.

use crate::legacy_configs::configs::split_config_section_and_key;

pub const REDACTED_CONFIG_VALUE: &str = "<redacted>";

/// Mutate only the event copy, after real configuration comparisons are complete.
/// Covers global defaults, local files, explicit --config-file inputs and CLI
/// overrides. Project config references contain paths only.
pub fn redact_buckconfig_input_values(event: &mut buck2_data::BuckconfigInputValues) {
    use buck2_data::buckconfig_component::Data;

    fn redact(value: &mut buck2_data::ConfigValue) {
        value.value.clear();
        value.value.push_str(REDACTED_CONFIG_VALUE);
    }

    for component in &mut event.components {
        match &mut component.data {
            Some(Data::ConfigValue(value)) => redact(value),
            Some(Data::GlobalExternalConfigFile(file)) => {
                file.values.iter_mut().for_each(redact);
            }
            Some(Data::ConfigFile(file)) => {
                if let Some(buck2_data::config_file::Data::GlobalExternalConfig(file)) = &mut file.data {
                    file.values.iter_mut().for_each(redact);
                }
            }
            None => {}
        }
    }
}

/// Preserve validated key/scope identifiers, never an override's value or an
/// unvalidated prefix (for example a padded credential passed as `token==`).
pub fn redact_config_argument(argument: &mut String) {
    let start = config_argument_redaction_start(argument);
    argument.truncate(start);
    argument.push_str(REDACTED_CONFIG_VALUE);
}

fn config_argument_redaction_start(argument: &str) -> usize {
    let Some((scoped_key, _)) = argument.split_once('=') else {
        return 0;
    };
    let key = match scoped_key.split_once("//") {
        Some((cell, key)) if !cell.contains(char::is_whitespace) => key,
        Some(_) => return 0,
        None => scoped_key,
    };
    if split_config_section_and_key(key).is_err() {
        return 0;
    }
    scoped_key.len() + 1
}

/// Locate a value already classified by the real CLI parser. This deliberately
/// does not parse short-option clusters or reinterpret another option's value.
/// Matching the original strings also covers a flag in an argfile consuming an
/// inline value. Repeated copies of a classified assignment are conservatively
/// private, even if another argument happens to contain the same bytes.
pub(crate) fn config_argument_redaction_span(
    argument: &str,
    parsed_assignment: &str,
) -> Option<usize> {
    if parsed_assignment.is_empty() {
        // There are no private bytes; never match the empty suffix of every arg.
        return None;
    }
    let prefix = argument.strip_suffix(parsed_assignment)?;
    if !prefix.is_empty() && !prefix.starts_with('-') {
        return None;
    }
    Some(prefix.len() + config_argument_redaction_start(parsed_assignment))
}

/// Positions refer to the original argv, before command-specific removals.
/// Plans are sorted and deduplicated before construction.
pub(crate) struct ConfigArgvRedactor<'a> {
    position: usize,
    spans: &'a [(usize, usize)],
}

impl<'a> ConfigArgvRedactor<'a> {
    pub(crate) fn new(spans: &'a [(usize, usize)]) -> Self {
        Self { position: 0, spans }
    }

    pub(crate) fn redact(&mut self, argument: &mut String) {
        if let Some(&(position, start)) = self.spans.first() {
            if position == self.position {
                argument.truncate(start);
                argument.push_str(REDACTED_CONFIG_VALUE);
                self.spans = &self.spans[1..];
            }
        }
        self.position += 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::legacy_configs::key::BuckconfigKeyRef;

    use super::*;

    #[test]
    fn config_argument_redaction_preserves_only_validated_identifiers() {
        for (raw, expected) in [
            ("ordinary.choice=synthetic-private-token", "ordinary.choice=<redacted>"),
            ("other//ordinary.choice=synthetic-private-token = // 秘密", "other//ordinary.choice=<redacted>"),
            ("ordinary.choice=", "ordinary.choice=<redacted>"),
            ("synthetic-private-token==", "<redacted>"),
            ("synthetic-private-token", "<redacted>"),
            ("ordinary.=synthetic-private-token", "<redacted>"),
            ("ordinary. choice=synthetic-private-token", "<redacted>"),
            ("bad cell//ordinary.choice=synthetic-private-token", "<redacted>"),
        ] {
            let mut argument = raw.to_owned();
            redact_config_argument(&mut argument);
            assert_eq!(argument, expected);
        }
        assert_eq!(config_argument_redaction_span("--config", ""), None);
        assert_eq!(
            config_argument_redaction_span("-Acsynthetic-private-token==", "synthetic-private-token=="),
            Some(3),
        );
    }

    #[test]
    fn malformed_config_files_do_not_echo_private_lines() {
        for contents in [
            "[ordinary]\n= synthetic-invalid-line-credential\n",
            "[synthetic-invalid-line-credential\n",
            "synthetic-invalid-line-credential\n",
        ] {
            let error = crate::legacy_configs::configs::testing::parse(
                &[("config", contents)],
                "config",
            )
            .unwrap_err();
            assert!(!format!("{error:#}").contains("synthetic-invalid-line-credential"));
        }
    }

    #[test]
    fn typed_config_parse_errors_keep_tags_without_input_or_error_values() {
        #[derive(Debug)]
        struct EchoingValue;

        impl std::str::FromStr for EchoingValue {
            type Err = buck2_error::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "parser echoed {value}",
                ))
            }
        }

        let error = crate::legacy_configs::configs::LegacyBuckConfig::parse_value::<EchoingValue>(
            BuckconfigKeyRef {
                section: "ordinary",
                property: "choice",
            },
            Some("synthetic-parser-credential"),
        )
        .unwrap_err();
        assert!(error.has_tag(buck2_error::ErrorTag::Input));
        let diagnostic = format!("{error:#}");
        assert!(!diagnostic.contains("synthetic-parser-credential"));
        assert!(diagnostic.contains("ordinary.choice"));
        assert!(diagnostic.contains("EchoingValue"));
    }

    #[test]
    fn buckconfig_metadata_redaction_preserves_actual_alias_resolution() {
        let config = crate::legacy_configs::configs::testing::parse(
            &[(
                "config",
                "[ordinary]\nchoice = synthetic-aliased-credential\n[buck2_metadata]\nnote = $(config ordinary.choice)\npurpose = test\n[buck2_re_client]\nhttp_headers = authorization: $(config ordinary.choice)\n",
            )],
            "config",
        )
        .unwrap();
        let metadata = crate::legacy_configs::parse_buckconfig_metadata(&config);
        assert_eq!(metadata["note"], REDACTED_CONFIG_VALUE);
        assert_eq!(metadata["purpose"], REDACTED_CONFIG_VALUE);
        assert_eq!(
            config.get(BuckconfigKeyRef {
                section: "ordinary",
                property: "choice",
            }),
            Some("synthetic-aliased-credential"),
        );
        assert_eq!(
            config.get(BuckconfigKeyRef {
                section: "buck2_re_client",
                property: "http_headers",
            }),
            Some("authorization: synthetic-aliased-credential"),
        );
    }

    #[test]
    fn native_buckconfig_event_redacts_aliases_and_all_value_representations() {
        use buck2_data::buckconfig_component::Data;

        let alias = buck2_data::ConfigValue {
            section: "ordinary".to_owned(),
            key: "choice".to_owned(),
            value: "synthetic-native-credential".to_owned(),
            cell: Some("scoped-cell".to_owned()),
            is_cli: true,
        };
        let private = buck2_data::ConfigValue {
            section: "buck2_re_client".to_owned(),
            key: "http_headers".to_owned(),
            value: "authorization: $(config ordinary.choice)".to_owned(),
            ..Default::default()
        };
        let public = buck2_data::ConfigValue {
            section: "build".to_owned(),
            key: "threads".to_owned(),
            value: "8".to_owned(),
            ..Default::default()
        };
        let file = buck2_data::GlobalExternalConfig {
            values: vec![private, alias.clone(), public],
            origin_path: "/synthetic/included-config".to_owned(),
        };
        let original = buck2_data::BuckconfigInputValues {
            components: vec![
                buck2_data::BuckconfigComponent {
                    data: Some(Data::ConfigValue(alias.clone())),
                },
                buck2_data::BuckconfigComponent {
                    data: Some(Data::GlobalExternalConfigFile(file.clone())),
                },
                buck2_data::BuckconfigComponent {
                    data: Some(Data::ConfigFile(buck2_data::ConfigFile {
                        data: Some(buck2_data::config_file::Data::GlobalExternalConfig(file)),
                    })),
                },
                buck2_data::BuckconfigComponent {
                    data: Some(Data::ConfigFile(buck2_data::ConfigFile {
                        data: Some(buck2_data::config_file::Data::ProjectRelativePath(
                            ".buckconfig".to_owned(),
                        )),
                    })),
                },
            ],
        };
        let mut telemetry = original.clone();
        redact_buckconfig_input_values(&mut telemetry);
        let native_event = buck2_data::BuckEvent {
            data: Some(buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::BuckconfigInputValues(telemetry.clone())),
            })),
            ..Default::default()
        };
        let serialized = serde_json::to_string(&native_event).unwrap();
        assert!(!serialized.contains("synthetic-native-credential"));
        assert!(!serialized.contains("$(config"));
        assert_eq!(serialized.matches(REDACTED_CONFIG_VALUE).count(), 7);
        assert!(serialized.contains("scoped-cell"));
        assert!(serialized.contains("/synthetic/included-config"));
        // Real configuration / previous-command comparison input stays intact.
        assert_eq!(original.components[0].data, Some(Data::ConfigValue(alias)));
        let before = telemetry.clone();
        redact_buckconfig_input_values(&mut telemetry);
        assert_eq!(before, telemetry);
    }
}
