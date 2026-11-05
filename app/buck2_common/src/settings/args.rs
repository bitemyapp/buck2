/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

#[derive(buck2_error::Error, Debug)]
#[buck2(input)]
enum SettingsArgumentError {
    #[error("Could not find equals sign (`=`); expected `section.key=value`")]
    NoEqualsSeparator,
    #[error("Expected a value after `=` in setting")]
    MissingValue,
    #[error("Expected a valid `section.key` in setting")]
    InvalidSectionAndKey,
    #[error("Expected one key-value pair in setting")]
    MultipleKeyValuePairs,
}

/// A settings layer parsed from a single `--setting` argument.
#[derive(Clone, Debug, PartialEq)]
pub struct SettingOverride(toml::Table);

impl SettingOverride {
    /// Converts override into underlying TOML table.
    pub fn into_table(self) -> toml::Table {
        self.0
    }
}

/// Parses one `--setting` argument in the format `section.key=value`.
pub fn parse_setting_flag_arg(raw: &str) -> buck2_error::Result<SettingOverride> {
    let (raw_section_and_key, raw_value) = raw
        .split_once('=')
        .ok_or(SettingsArgumentError::NoEqualsSeparator)?;

    if raw_value.is_empty() {
        return Err(SettingsArgumentError::MissingValue.into());
    }
    let (section, key) = parse_setting_section_and_key(raw_section_and_key)?;

    let value = parse_setting_value(raw_value)?;

    let mut section_table = toml::Table::new();
    section_table.insert(key.to_owned(), value);
    let mut table = toml::Table::new();
    table.insert(section.to_owned(), toml::Value::Table(section_table));
    Ok(SettingOverride(table))
}

fn parse_setting_section_and_key(raw_section_and_key: &str) -> buck2_error::Result<(&str, &str)> {
    let Some((section, key)) = raw_section_and_key.split_once('.') else {
        return Err(SettingsArgumentError::InvalidSectionAndKey.into());
    };

    if section.is_empty()
        || key.is_empty()
        || key.contains('.')
        || raw_section_and_key.chars().any(char::is_whitespace)
    {
        return Err(SettingsArgumentError::InvalidSectionAndKey.into());
    }

    Ok((section, key))
}

fn parse_setting_value(raw_value: &str) -> buck2_error::Result<toml::Value> {
    let Ok(mut table) = toml::from_str::<toml::Table>(&format!("value = {raw_value}")) else {
        return Ok(toml::Value::String(raw_value.to_owned()));
    };
    if table.len() != 1 {
        return Err(SettingsArgumentError::MultipleKeyValuePairs.into());
    }
    Ok(table
        .remove("value")
        .expect("temporary TOML table must contain `value`"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::parser::table;

    #[test]
    fn malformed_setting_diagnostics_do_not_echo_values() {
        for argument in [
            "synthetic-setting-credential",
            "synthetic-setting-credential=",
            "synthetic-setting-credential==",
            "ordinary.choice=1\nanother = \"synthetic-setting-credential\"",
        ] {
            let error = parse_setting_flag_arg(argument).unwrap_err();
            assert!(!format!("{error:#}").contains("synthetic-setting-credential"));
        }
    }

    #[test]
    fn test_parse_setting_flag_arg_valid_formats() -> buck2_error::Result<()> {
        assert_eq!(
            parse_setting_flag_arg("test_section.test_value=cli")?,
            SettingOverride(table("[test_section]\ntest_value = \"cli\""))
        );
        assert_eq!(
            parse_setting_flag_arg("test_section.x=90")?,
            SettingOverride(table("[test_section]\nx = 90"))
        );
        Ok(())
    }

    #[test]
    fn test_parse_setting_flag_arg_whitespace() -> buck2_error::Result<()> {
        assert_eq!(
            parse_setting_flag_arg("test_section.x= value with whitespace  ")?,
            SettingOverride(table("[test_section]\nx = \" value with whitespace  \""))
        );
        assert!(parse_setting_flag_arg("test flag=true").is_err());
        assert!(parse_setting_flag_arg("test_section. test_flag=true").is_err());
        Ok(())
    }

    #[test]
    fn test_parse_setting_flag_arg_invalid_formats() {
        for raw in [
            "test_flag",
            "test_flag=true",
            "=true",
            "test_flag=",
            ".test_flag=true",
            "test_section.=true",
            "a.b.c=true",
            "a=false\nb=true",
        ] {
            assert!(parse_setting_flag_arg(raw).is_err(), "accepted `{raw}`");
        }
    }
}
