/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

#![feature(used_with_arg)]

use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use buck2_client::commands::build::BuildCommand;
use buck2_client::commands::bxl::BxlCommand;
use buck2_client::commands::clean::CleanCommand;
use buck2_client::commands::cleanall::CleanallCommand;
use buck2_client::commands::ctargets::ConfiguredTargetsCommand;
use buck2_client::commands::expand_external_cell::ExpandExternalCellsCommand;
use buck2_client::commands::explain::ExplainCommand;
use buck2_client::commands::help_env::HelpEnvCommand;
use buck2_client::commands::init::InitCommand;
use buck2_client::commands::install::InstallCommand;
use buck2_client::commands::kill::KillCommand;
use buck2_client::commands::killall::KillallCommand;
use buck2_client::commands::lsp::LspCommand;
use buck2_client::commands::profile::ProfileCommand;
use buck2_client::commands::query::aquery::AqueryCommand;
use buck2_client::commands::query::cquery::CqueryCommand;
use buck2_client::commands::query::uquery::UqueryCommand;
use buck2_client::commands::root::RootCommand;
use buck2_client::commands::run::RunCommand;
use buck2_client::commands::server::ServerCommand;
use buck2_client::commands::status::StatusCommand;
use buck2_client::commands::subscribe::SubscribeCommand;
use buck2_client::commands::targets::TargetsCommand;
use buck2_client::commands::test::TestCommand;
use buck2_client_ctx::agent_context::AgentContext;
use buck2_client_ctx::agent_context::AgentContextEntry;
use buck2_client_ctx::agent_context::parse_agent_context;
use buck2_client_ctx::argfiles::expand_argv;
use buck2_client_ctx::client_ctx::BuckSubcommand;
use buck2_client_ctx::client_ctx::ClientCommandContext;
use buck2_client_ctx::client_metadata::ClientMetadata;
use buck2_client_ctx::client_metadata::parse_client_metadata;
use buck2_client_ctx::common::BuckArgMatches;
use buck2_client_ctx::exit_result::ClientIoError;
use buck2_client_ctx::exit_result::ExitResult;
use buck2_client_ctx::immediate_config::ImmediateConfigContext;
use buck2_client_ctx::version::BuckVersion;
use buck2_cmd_audit_client::AuditCommand;
use buck2_cmd_debug_client::DebugCommand;
use buck2_cmd_log_client::LogCommand;
use buck2_cmd_rage_client::rage::RageCommand;
use buck2_cmd_starlark_client::StarlarkCommand;
use buck2_common::argv::Argv;
use buck2_common::argv::ConfigArgvRedactions;
use buck2_common::argv::ExpandedArgv;
use buck2_common::invocation_paths::RESERVED_BUCK_OUT_PREFIX;
use buck2_common::invocation_paths_result::InvocationPathsResult;
use buck2_common::invocation_roots::get_invocation_paths_result;
use buck2_common::settings::args::SettingOverride;
use buck2_common::settings::args::parse_setting_flag_arg;
use buck2_core::buck2_env;
use buck2_core::buck2_env_name;
use buck2_data::ErrorReport;
use buck2_error::BuckErrorContext;
use buck2_error::ErrorTag;
use buck2_error::ExitCode;
use buck2_error::buck2_error;
use buck2_error::conversion::clap::buck_error_clap_parser;
use buck2_event_observer::verbosity::Verbosity;
use buck2_fs::paths::file_name::FileNameBuf;
use buck2_util::threads::thread_spawn_scoped;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::builder::TypedValueParser;
use dupe::Dupe;

use crate::check_user_allowed::check_user_allowed;
use crate::process_context::ProcessContext;

mod check_user_allowed;
mod cli_style;
pub(crate) mod commands;
pub mod panic;
pub mod process_context;

fn parse_isolation_dir(s: &str) -> buck2_error::Result<FileNameBuf> {
    if s.starts_with(RESERVED_BUCK_OUT_PREFIX) {
        return Err(buck2_error::buck2_error!(
            buck2_error::ErrorTag::Input,
            "Isolation dir names starting with `{RESERVED_BUCK_OUT_PREFIX}` are reserved for buck2's internal use"
        ));
    }
    FileNameBuf::try_from(s.to_owned()).buck_error_context("isolation dir must be a directory name")
}

const AGENT_CONTEXT_ARG_ID: &str = "agent_context";

/// Options of `buck2` command, before subcommand.
#[derive(Clone, Debug, clap::Parser)]
#[clap(next_help_heading = "Universal Options")]
struct BeforeSubcommandOptions {
    /// The name of the directory that Buck2 creates within buck-out for writing outputs and daemon
    /// information. If one is not provided, Buck2 creates a directory with the default name.
    ///
    /// Instances of Buck2 share a daemon if and only if their isolation directory is identical.
    /// The isolation directory also influences the output paths provided by Buck2,
    /// and as a result using a non-default isolation dir will cause cache misses (and slower builds).
    #[clap(
        value_parser = buck_error_clap_parser(parse_isolation_dir),
        env("BUCK_ISOLATION_DIR"),
        long,
        global = true,
        default_value="v2"
    )]
    isolation_dir: FileNameBuf,

    /// How verbose buck should be while logging.
    ///
    /// Values:
    /// 0 = Quiet, errors only;
    /// 1 = Show status. Default;
    /// 2 = more info about errors;
    /// 3 = more info about everything;
    /// 4 = more info about everything + stderr;
    ///
    /// It can be combined with specific log items (stderr, full_failed_command, commands, actions,
    /// status, stats, success) to fine-tune the verbosity of the log. Example usage "-v=1,stderr"
    #[clap(
        short = 'v',
        long = "verbose",
        default_value = "1",
        global = true,
        env = buck2_env_name!("BUCK_VERBOSE"),
        value_parser = buck_error_clap_parser(Verbosity::try_from_cli)
    )]
    verbosity: Verbosity,

    /// The oncall executing this command
    #[clap(long, global = true)]
    oncall: Option<String>,

    /// Metadata key-value pairs to inject into Buck2's logging. Client metadata must be of the
    /// form `key=value`, where `key` is a snake_case identifier, and will be sent to backend
    /// datasets.
    #[clap(long, global = true, value_parser = buck_error_clap_parser(parse_client_metadata))]
    client_metadata: Vec<ClientMetadata>,

    /// Override a Buck setting using `section.key=value`.
    #[clap(
        long = "setting",
        value_name = "SECTION.KEY=VALUE",
        global = true,
        num_args = 1,
        value_parser = buck_error_clap_parser(parse_setting_flag_arg)
    )]
    settings: Vec<SettingOverride>,

    /// Agent context key=value pairs for telemetry.
    /// Used by AI agents to pass structured metadata. Schema is defined via buckconfig.
    /// Entries can be comma-separated or passed as separate flags.
    /// Examples:
    ///   --agent-context intent=fix,attempt=2,prior_error=missing_target
    ///   --agent-context intent=build --agent-context attempt=1
    #[clap(
        id = AGENT_CONTEXT_ARG_ID,
        long = "agent-context",
        value_name = "AGENT_CONTEXT",
        value_delimiter = ',',
        value_parser = buck_error_clap_parser(parse_agent_context)
    )]
    agent_context: Vec<AgentContextEntry>,

    /// Do not launch a daemon process, run buck server in client process.
    ///
    /// Note even when running in no-buckd mode, it still writes state files.
    /// In particular, this command effectively kills buckd process
    /// running with the same isolation directory.
    ///
    /// This is an unsupported option used only for development work.
    #[clap(env("BUCK2_NO_BUCKD"), long, global(true), hide(true))]
    // Env var is BUCK2_NO_BUCKD instead of NO_BUCKD env var from buck1 because no buckd
    // is not supported for production work for buck2 and lots of places already set
    // NO_BUCKD=1 for buck1.
    no_buckd: bool,

    /// Print buck wrapper help.
    #[clap(skip)] // @oss-enable
    // @oss-disable: #[clap(long)]
    help_wrapper: bool,
}

#[rustfmt::skip] // Formatting in internal and in OSS versions disagree after oss markers applied.
fn help() -> &'static str {
    concat!(
        "A build system\n",
        "\n",
        "Documentation: https://buck2.build/docs/\n", // @oss-enable
        // @oss-disable: "Documentation: https://internalfb.com/intern/staticdocs/buck2/docs/\n",
    )
}

#[derive(Debug, clap::Parser)]
#[clap(
    name = "buck2",
    about(Some(help())),
    version(BuckVersion::get_version_for_clap()),
    styles = cli_style::get_styles(),
)]
pub(crate) struct Opt {
    #[clap(subcommand)]
    cmd: CommandKind,
    #[clap(flatten)]
    common_opts: BeforeSubcommandOptions,
}

impl Opt {
    fn clap_command() -> clap::Command {
        // Clap replaces a global argument's entire value list at subcommand boundaries.
        // Local registrations preserve independently supplied entries for from_clap_matches.
        let command = Self::command();
        let agent_context = command
            .get_arguments()
            .find(|arg| arg.get_id() == AGENT_CONTEXT_ARG_ID)
            .expect("agent_context is declared in BeforeSubcommandOptions")
            .clone();

        fn add_to_subcommands(command: clap::Command, arg: &clap::Arg) -> clap::Command {
            command
                .mut_subcommands(|subcommand| add_to_subcommands(subcommand.arg(arg.clone()), arg))
        }

        add_to_subcommands(command, &agent_context)
    }

    fn from_clap_matches(mut matches: &clap::ArgMatches) -> Result<Self, clap::Error> {
        let mut opt = Self::from_arg_matches(matches)?;
        while let Some((_, subcommand)) = matches.subcommand() {
            if let Some(entries) = subcommand.get_many::<AgentContextEntry>(AGENT_CONTEXT_ARG_ID) {
                opt.common_opts.agent_context.extend(entries.cloned());
            }
            matches = subcommand;
        }
        Ok(opt)
    }

    pub(crate) fn exec(
        self,
        process: ProcessContext<'_>,
        immediate_config: &ImmediateConfigContext,
        matches: BuckArgMatches<'_>,
        argv: Argv,
    ) -> ExitResult {
        let subcommand_matches = matches.unwrap_subcommand();

        self.cmd.exec(
            process,
            immediate_config,
            subcommand_matches,
            argv,
            self.common_opts,
        )
    }
}

/// Capture actual CLI value consumption rather than reparsing short-option
/// clusters or treating Clap's logical indices as physical argv positions.
/// Capture happens before global settings can overwrite earlier value lists.
fn parse_argv(
    argv: Vec<String>,
    expanded: ExpandedArgv,
) -> Result<(Argv, clap::ArgMatches), clap::Error> {
    fn install_parsers(
        command: clap::Command,
        config: &clap::builder::ValueParser,
        settings: &clap::builder::ValueParser,
    ) -> clap::Command {
        let has_config = command.get_arguments().any(|arg| arg.get_id() == "config_values");
        let command = if has_config {
            command.mut_arg("config_values", |arg| arg.value_parser(config.clone()))
        } else {
            command
        };
        let has_settings = command.get_arguments().any(|arg| arg.get_id() == "settings");
        let command = if has_settings {
            command.mut_arg("settings", |arg| arg.value_parser(settings.clone()))
        } else {
            command
        };
        command.mut_subcommands(|child| install_parsers(child, config, settings))
    }

    let source = Arc::new((argv, expanded));
    let redactions = Arc::new(Mutex::new(ConfigArgvRedactions::default()));
    let command = {
        let source = Arc::clone(&source);
        let redactions = Arc::clone(&redactions);
        let capture = move |value: &str| {
            redactions
                .lock()
                .expect("CLI redaction capture was poisoned")
                .record(&source.0, &source.1, value);
        };
        let capture_setting = capture.clone();
        let config = clap::builder::StringValueParser::new().map(move |value| {
            capture(&value);
            value
        });
        let settings = buck_error_clap_parser(move |value: &str| {
            capture_setting(value);
            parse_setting_flag_arg(value)
        });
        install_parsers(Opt::clap_command(), &config.into(), &settings.into())
    };
    let matches = command.try_get_matches_from(source.1.args())?;
    let (argv, expanded) = Arc::try_unwrap(source)
        .unwrap_or_else(|_| panic!("CLI parser retained argument source"));
    let redactions = Arc::try_unwrap(redactions)
        .unwrap_or_else(|_| panic!("CLI parser retained redaction capture"))
        .into_inner()
        .expect("CLI redaction capture was poisoned");
    Ok((Argv::new(argv, expanded, redactions), matches))
}

pub fn exec(process: ProcessContext<'_>) -> ExitResult {
    let cwd = process.shared.working_dir.clone();
    let mut immediate_config = ImmediateConfigContext::new(&cwd);
    let arg0_override = buck2_env!("BUCK2_ARG0")?;
    let expanded_args = expand_argv(
        arg0_override,
        process.shared.args.to_vec(),
        &mut immediate_config,
        &cwd,
    )
    .buck_error_context("Error expanding argsfiles")?;

    let (argv, matches) = match parse_argv(process.shared.args.to_vec(), expanded_args) {
        Ok(parsed) => parsed,
        Err(e) => {
            // Print colorized output, ExitResult::report will not colorize.
            // `ClientIoError` so that a closed stdout exits quietly instead of as a buck2 failure,
            // e.g. `buck2 build --help | head`.
            e.print().map_err(ClientIoError::from)?;
            return if e.exit_code() == 0 {
                ExitResult::success()
            } else {
                let e = buck2_error::Error::from(e).tag([ErrorTag::ClapMatch]);
                ExitResult::status_with_emitted_errors(
                    ExitCode::UserError,
                    vec![ErrorReport::from(&e)],
                )
            };
        }
    };
    let mut opt = ParsedArgv::parse(argv, matches)?;
    let setting_arg_layers = opt
        .opt
        .common_opts
        .settings
        .drain(..)
        .map(SettingOverride::into_table)
        .collect();
    immediate_config.set_setting_arg_layers(setting_arg_layers)?;

    let client_metadata = ClientMetadata::from_env()?;
    if !client_metadata.is_empty() {
        // insert the `client_metadata` at the beginning of the list, so that the client id metadata from the env var could be overridden by the cli arg
        opt.opt
            .common_opts
            .client_metadata
            .splice(0..0, client_metadata);
    }

    let agent_env_metadata = AgentContextEntry::from_env()?;
    if !agent_env_metadata.is_empty() {
        opt.opt
            .common_opts
            .agent_context
            .splice(0..0, agent_env_metadata);
    }

    // If --client-metadata=? was not set and from_env did not find "id", then
    // if we are running in a terminal, we add id=terminal-fallback to
    // opt.opt.common_opts.client_metadata to transmit to scuba that the client
    // is an end user: https://fburl.com/scuba/buck2_builds/n4klo51d
    let has_client_id = opt
        .opt
        .common_opts
        .client_metadata
        .iter()
        .any(|m| m.key == "id");

    if !has_client_id {
        use std::io::IsTerminal;
        let client_id = if std::io::stdin().is_terminal() {
            Some("terminal-fallback")
        } else {
            // Check if running from VSCode
            let is_vscode = std::env::var("VSCODE_PID")
                .ok()
                .is_some_and(|v| !v.is_empty())
                || std::env::var("TERM_PROGRAM").ok().as_deref() == Some("vscode");
            if is_vscode {
                Some("vscode-fallback")
            } else {
                None
            }
        };

        if let Some(val) = client_id {
            opt.opt.common_opts.client_metadata.push(ClientMetadata {
                key: "id".to_owned(),
                value: val.to_owned(),
            });
        }
    }
    opt.exec(process, &immediate_config)
}

struct ParsedArgv {
    opt: Opt,
    argv: Argv,
    matches: clap::ArgMatches,
}

impl ParsedArgv {
    fn parse(argv: Argv, matches: clap::ArgMatches) -> buck2_error::Result<Self> {
        let opt = Opt::from_clap_matches(&matches)?;

        if opt.common_opts.help_wrapper {
            return Err(buck2_error!(
                buck2_error::ErrorTag::Tier0,
                "`--help-wrapper` should have been handled by the wrapper"
            ));
        }

        match &opt.cmd {
            #[cfg(not(client_only))]
            CommandKind::Daemon(..) | CommandKind::Forkserver(..) => {}
            CommandKind::Clean(..) | CommandKind::Cleanall(..) => {}
            _ => {
                check_user_allowed()?;
            }
        }

        Ok(ParsedArgv { opt, argv, matches })
    }

    fn exec(
        self,
        process: ProcessContext<'_>,
        immediate_config: &ImmediateConfigContext,
    ) -> ExitResult {
        let expanded_args = self.argv.expanded_argv.clone();
        self.opt.exec(
            process,
            immediate_config,
            BuckArgMatches::from_clap(&self.matches, &expanded_args),
            self.argv,
        )
    }
}

#[derive(Debug, clap::Subcommand)]
pub(crate) enum CommandKind {
    #[cfg(not(client_only))]
    #[clap(hide = true)]
    Daemon(buck2_daemon::daemon::DaemonCommand),
    #[cfg(not(client_only))]
    #[clap(hide = true)]
    Forkserver(crate::commands::forkserver::ForkserverCommand),
    #[cfg(not(client_only))]
    #[clap(hide = true)]
    InternalTestRunner(crate::commands::internal_test_runner::InternalTestRunnerCommand),
    #[clap(subcommand)]
    Audit(AuditCommand),
    Aquery(AqueryCommand),
    Build(BuildCommand),
    Bxl(BxlCommand),
    // TODO(nga): implement `buck2 help-buckconfig` too
    //   https://www.internalfb.com/tasks/?t=183528129
    HelpEnv(HelpEnvCommand),
    Test(TestCommand),
    Cquery(CqueryCommand),
    Init(InitCommand),
    Explain(ExplainCommand),
    ExpandExternalCell(ExpandExternalCellsCommand),
    Install(InstallCommand),
    Kill(KillCommand),
    Killall(KillallCommand),
    Root(RootCommand),
    /// Alias for `uquery`.
    Query(UqueryCommand),
    Run(RunCommand),
    Server(ServerCommand),
    Status(StatusCommand),
    #[clap(subcommand)]
    Starlark(StarlarkCommand),
    /// Alias for `utargets`.
    Targets(TargetsCommand),
    Utargets(TargetsCommand),
    Ctargets(ConfiguredTargetsCommand),
    Uquery(UqueryCommand),
    #[clap(subcommand, hide = true)]
    Debug(DebugCommand),
    #[clap(hide = true)]
    Complete(buck2_cmd_completion_client::complete::CompleteCommand),
    Completion(buck2_cmd_completion_client::completion::CompletionCommand),
    Docs(buck2_cmd_docs_client::DocsCommand),
    #[clap(subcommand)]
    Profile(ProfileCommand),
    #[clap(hide(true))] // @oss-enable
    Rage(RageCommand),
    Clean(CleanCommand),
    Cleanall(CleanallCommand),
    #[clap(subcommand)]
    Log(LogCommand),
    Lsp(LspCommand),
    Subscribe(SubscribeCommand),
}

impl CommandKind {
    pub(crate) fn exec(
        self,
        process: ProcessContext<'_>,
        immediate_config: &ImmediateConfigContext,
        matches: BuckArgMatches<'_>,
        argv: Argv,
        common_opts: BeforeSubcommandOptions,
    ) -> ExitResult {
        let paths_result = get_invocation_paths_result(
            &process.shared.working_dir,
            common_opts.isolation_dir.clone(),
        );

        // Handle the daemon command earlier: it wants to fork, but the things we do below might
        // want to create threads.
        #[cfg(not(client_only))]
        if let CommandKind::Daemon(cmd) = self {
            process.events_ctx.log_invocation_record = false;
            return cmd
                .exec(
                    process.shared.log_reload_handle.dupe(),
                    paths_result.get_result()?,
                    false,
                    Box::new(|| {}),
                )
                .into();
        }
        thread::scope(|scope| {
            // Spawn a thread to have stack size independent on linker/environment.
            match thread_spawn_scoped("buck2-main", scope, move || {
                self.exec_no_daemon(
                    common_opts,
                    process,
                    immediate_config,
                    matches,
                    argv,
                    paths_result,
                )
            }) {
                Ok(t) => match t.join() {
                    Ok(res) => res,
                    Err(_) => ExitResult::bail("Main thread panicked"),
                },
                Err(e) => ExitResult::bail(format_args!("Failed to start main thread: {e}")),
            }
        })
    }

    fn exec_no_daemon(
        self,
        common_opts: BeforeSubcommandOptions,
        process: ProcessContext<'_>,
        immediate_config: &ImmediateConfigContext,
        matches: BuckArgMatches<'_>,
        argv: Argv,
        paths: InvocationPathsResult,
    ) -> ExitResult {
        if common_opts.no_buckd {
            // `no_buckd` can't work in a client-only binary
            if let Some(res) = ExitResult::retry_command_with_full_binary()? {
                return res;
            }
        }

        let fb = buck2_common::fbinit::get_or_init_fbcode_globals();

        let ProcessContext {
            trace_id,
            events_ctx,
            shared,
            runtime,
            start_time,
        } = process;

        let runtime = runtime.get_or_init()?;

        let start_in_process_daemon = if common_opts.no_buckd {
            #[cfg(not(client_only))]
            {
                buck2_daemon::no_buckd::start_in_process_daemon(
                    immediate_config.daemon_startup_config()?,
                    paths.clone().get_result()?,
                    runtime,
                )?
            }
            #[cfg(client_only)]
            {
                unreachable!() // case covered above
            }
        } else {
            None
        };

        let command_ctx = ClientCommandContext::new(
            fb,
            immediate_config,
            paths,
            shared.working_dir.clone(),
            common_opts.verbosity,
            start_time,
            start_in_process_daemon,
            argv,
            trace_id.dupe(),
            &mut shared.stdin,
            &mut shared.restarter,
            runtime,
            common_opts.oncall,
            common_opts.client_metadata,
            common_opts.isolation_dir,
            AgentContext::new(common_opts.agent_context),
        );
        if let Some(recorder) = events_ctx.recorder.as_mut() {
            recorder.update_for_client_ctx(&command_ctx, self.command_name());
        }

        match self {
            #[cfg(not(client_only))]
            CommandKind::Daemon(..) => unreachable!("Checked earlier"),
            #[cfg(not(client_only))]
            CommandKind::Forkserver(cmd) => cmd.exec(
                matches,
                command_ctx,
                events_ctx,
                shared.log_reload_handle.dupe(),
            ),
            #[cfg(not(client_only))]
            CommandKind::InternalTestRunner(cmd) => cmd.exec(matches, command_ctx, events_ctx),
            CommandKind::Aquery(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Build(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Bxl(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Test(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Cquery(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::HelpEnv(cmd) => cmd.exec(matches, command_ctx),
            CommandKind::Kill(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Killall(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Clean(cmd) => cmd.exec(matches, command_ctx, events_ctx),
            CommandKind::Cleanall(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Root(cmd) => cmd.exec(matches, command_ctx).into(),
            CommandKind::Query(cmd) => {
                buck2_client_ctx::eprintln!(
                    "WARNING: \"buck2 query\" is an alias for \"buck2 uquery\". Consider using \"buck2 cquery\" or \"buck2 uquery\" explicitly."
                )?;
                command_ctx.exec(cmd, matches, events_ctx)
            }
            CommandKind::Server(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Status(cmd) => cmd.exec(matches, command_ctx).into(),
            CommandKind::Targets(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Utargets(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Ctargets(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Audit(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Starlark(cmd) => cmd.exec(matches, command_ctx, events_ctx),
            CommandKind::Run(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Uquery(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Debug(cmd) => cmd.exec(matches, command_ctx, events_ctx),
            CommandKind::Complete(cmd) => cmd.exec(matches, command_ctx, events_ctx),
            CommandKind::Completion(cmd) => cmd.exec(Opt::clap_command(), matches, command_ctx),
            CommandKind::Docs(cmd) => {
                cmd.exec(Opt::clap_command(), matches, command_ctx, events_ctx)
            }
            CommandKind::Profile(cmd) => cmd.exec(matches, command_ctx, events_ctx),
            CommandKind::Rage(cmd) => cmd.exec(matches, command_ctx),
            CommandKind::Init(cmd) => cmd.exec(matches, command_ctx),
            CommandKind::Explain(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Install(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Log(cmd) => cmd.exec(matches, command_ctx, events_ctx),
            CommandKind::Lsp(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::Subscribe(cmd) => command_ctx.exec(cmd, matches, events_ctx),
            CommandKind::ExpandExternalCell(cmd) => command_ctx.exec(cmd, matches, events_ctx),
        }
    }

    fn command_name(&self) -> &'static str {
        match self {
            #[cfg(not(client_only))]
            CommandKind::Daemon(_) => "daemon",
            #[cfg(not(client_only))]
            CommandKind::Forkserver(_) => "forkserver",
            #[cfg(not(client_only))]
            CommandKind::InternalTestRunner(_) => "internal-test-runner",
            CommandKind::Aquery(cmd) => cmd.logging_name(),
            CommandKind::Build(cmd) => cmd.logging_name(),
            CommandKind::Bxl(cmd) => cmd.logging_name(),
            CommandKind::Test(cmd) => cmd.logging_name(),
            CommandKind::Cquery(cmd) => cmd.logging_name(),
            CommandKind::HelpEnv(_) => "help-env",
            CommandKind::Kill(cmd) => cmd.logging_name(),
            CommandKind::Killall(cmd) => cmd.logging_name(),
            CommandKind::Clean(cmd) => cmd.command_name(),
            CommandKind::Cleanall(cmd) => cmd.logging_name(),
            CommandKind::Root(_) => "root",
            CommandKind::Query(cmd) => cmd.logging_name(),
            CommandKind::Server(cmd) => cmd.logging_name(),
            CommandKind::Status(_) => "status",
            CommandKind::Targets(cmd) => cmd.logging_name(),
            CommandKind::Utargets(cmd) => cmd.logging_name(),
            CommandKind::Ctargets(cmd) => cmd.logging_name(),
            CommandKind::Audit(cmd) => cmd.logging_name(),
            CommandKind::Starlark(cmd) => cmd.command_name(),
            CommandKind::Run(cmd) => cmd.logging_name(),
            CommandKind::Uquery(cmd) => cmd.logging_name(),
            CommandKind::Debug(_) => "debug",
            CommandKind::Complete(_) => "complete",
            CommandKind::Completion(_) => "completion",
            CommandKind::Docs(_) => "docs",
            CommandKind::Profile(_) => "profile",
            CommandKind::Rage(_) => "rage",
            CommandKind::Init(_) => "init",
            CommandKind::Explain(cmd) => cmd.logging_name(),
            CommandKind::Install(cmd) => cmd.logging_name(),
            CommandKind::Log(cmd) => cmd.command_name(),
            CommandKind::Lsp(cmd) => cmd.logging_name(),
            CommandKind::Subscribe(cmd) => cmd.logging_name(),
            CommandKind::ExpandExternalCell(cmd) => cmd.logging_name(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_config_argv_private(argv: &Argv, secret: &str) {
        for sanitized in [
            argv.clone().no_need_to_sanitize(),
            argv.clone().redacted(Default::default()),
        ] {
            assert!(!sanitized.argv.join(" ").contains(secret));
            assert!(!sanitized.expanded_argv.args().collect::<Vec<_>>().join(" ").contains(secret));
        }
        assert!(!buck2_common::argv::get_representative_config_flags(&argv.expanded_argv)
            .join(" ")
            .contains(secret));
    }

    #[test]
    fn cli_parser_classifies_clustered_config_values_without_mutating_execution() {
        for value in [
            "ordinary.choice=synthetic-private-token",
            "ordinary.choice=synthetic-private-token= // Unicode 秘密",
            "synthetic-private-token==",
            "ordinary.choice=",
        ] {
            for config_args in [
                vec!["-Ac".to_owned(), value.to_owned()],
                vec![format!("-Ac{value}")],
            ] {
                let mut args = vec!["buck2".to_owned(), "cquery".to_owned()];
                args.extend(config_args);
                // The duplicate also exercises idempotent capture and spans.
                args.extend(["-c".to_owned(), value.to_owned(), "//:target".to_owned()]);
                let (argv, matches) = parse_argv(
                    args.clone(),
                    ExpandedArgv::from_literals(args.clone()),
                ).unwrap();
                assert_eq!(argv.argv, args);
                assert_eq!(argv.expanded_argv.args().collect::<Vec<_>>(), args);
                assert_eq!(
                    matches.subcommand().unwrap().1
                        .get_many::<String>("config_values").unwrap()
                        .map(String::as_str).collect::<Vec<_>>(),
                    vec![value, value],
                );
                assert_config_argv_private(&argv, "synthetic-private-token");
                let sanitized = argv.clone().no_need_to_sanitize();
                assert!(sanitized.argv.iter().any(|arg| arg.contains("<redacted>")));
                if value == "synthetic-private-token==" {
                    assert!(sanitized.argv.iter().any(|arg| arg == "<redacted>" || arg == "-Ac<redacted>"));
                } else {
                    assert!(sanitized.argv.iter().any(|arg| arg.contains("ordinary.choice=<redacted>")));
                }
                assert_eq!(argv.argv, args);
            }
        }
    }

    #[test]
    fn cli_parser_does_not_reinterpret_a_value_taking_short_option_as_config() {
        let args: Vec<String> = [
            "buck2", "cquery", "-acordinary.choice=public", "//:target",
        ].into_iter().map(str::to_owned).collect();
        let (argv, matches) = parse_argv(
            args.clone(),
            ExpandedArgv::from_literals(args.clone()),
        ).unwrap();
        assert!(matches.subcommand().unwrap().1.get_many::<String>("config_values").is_none());
        assert_eq!(argv.no_need_to_sanitize().argv, args);
    }

    #[test]
    fn cli_capture_redacts_overwritten_global_settings_without_changing_toml_values() {
        let args: Vec<String> = [
            "buck2",
            "--setting", "ordinary.choice=\"synthetic-first-private-token = // 秘密\"",
            "cquery",
            "--setting=ordinary.choice=\"synthetic-second-private-token\"",
            "//:target",
        ].into_iter().map(str::to_owned).collect();
        let baseline = Opt::clap_command().try_get_matches_from(&args).unwrap();
        let (argv, matches) = parse_argv(
            args.clone(),
            ExpandedArgv::from_literals(args.clone()),
        ).unwrap();
        assert_eq!(
            Opt::from_clap_matches(&matches).unwrap().common_opts.settings,
            Opt::from_clap_matches(&baseline).unwrap().common_opts.settings,
        );
        assert_config_argv_private(&argv, "synthetic-first-private-token");
        assert_config_argv_private(&argv, "synthetic-second-private-token");
        assert_eq!(argv.argv, args);
    }

    #[test]
    fn cli_argfile_flags_redact_inline_values_and_preserve_source_paths() -> buck2_error::Result<()> {
        let project = buck2_core::fs::project::ProjectRootTemp::new()?;
        project.write_file(".buckconfig", "[cells]\nroot = .\n");
        let cwd = buck2_fs::working_dir::AbsWorkingDir::unchecked_new(
            project.path().root().to_owned(),
        );
        for flag in ["-c", "--config", "--setting"] {
            project.write_file("modefile", flag);
            for reference in [vec!["@modefile"], vec!["--flagfile", "modefile"]] {
                let mut args: Vec<String> = ["buck2", "cquery"]
                    .into_iter().chain(reference).map(str::to_owned).collect();
                args.extend([
                    "ordinary.choice=synthetic-boundary-private-token".to_owned(),
                    "//:target".to_owned(),
                ]);
                let mut context = ImmediateConfigContext::new(&cwd);
                let expanded = expand_argv(None, args.clone(), &mut context, &cwd)?;
                assert!(expanded.iter().any(|(arg, source)| {
                    arg == flag && matches!(source, buck2_common::argv::ExpandedArgSource::Flagfile(_))
                }));
                let (argv, _) = parse_argv(args.clone(), expanded).unwrap();
                assert_config_argv_private(&argv, "synthetic-boundary-private-token");
                for sanitized in [
                    argv.clone().no_need_to_sanitize(),
                    argv.clone().redacted(Default::default()),
                ] {
                    assert!(sanitized.argv.iter().any(|arg| arg == "@modefile" || arg == "modefile"));
                    assert!(sanitized.argv.iter().any(|arg| arg == "ordinary.choice=<redacted>"));
                }
                assert_eq!(argv.argv, args);
                assert!(argv.expanded_argv.args().any(|arg| arg.contains("synthetic-boundary-private-token")));
            }
        }
        Ok(())
    }

    #[test]
    fn cli_removal_keeps_original_redaction_positions() {
        let args: Vec<String> = [
            "buck2", "run",
            "-c", "ordinary.choice=synthetic-native-private-token",
            "//:target", "--", "-c", "synthetic-program-private-token",
        ].into_iter().map(str::to_owned).collect();
        let extra: Vec<String> = ["-c", "synthetic-program-private-token"]
            .into_iter().map(str::to_owned).collect();
        let (argv, _) = parse_argv(
            args.clone(),
            ExpandedArgv::from_literals(args.clone()),
        ).unwrap();
        let sanitized = argv.clone().redacted(extra.iter().collect());
        assert!(!sanitized.argv.join(" ").contains("private-token"));
        assert_eq!(sanitized.expanded_argv.args().collect::<Vec<_>>(), sanitized.argv);
        assert!(sanitized.argv.iter().any(|arg| arg == "ordinary.choice=<redacted>"));
        assert_eq!(argv.argv, args);
    }

    #[test]
    fn cli_setting_errors_do_not_echo_malformed_keys_or_values() {
        for value in [
            "synthetic-private-token==",
            "synthetic-private-token=",
            "synthetic-private-token",
            "ordinary.choice=1\nanother=\"synthetic-private-token\"",
        ] {
            let args: Vec<String> = ["buck2", "cquery", "--setting", value, "//:target"]
                .into_iter().map(str::to_owned).collect();
            let error = match parse_argv(args.clone(), ExpandedArgv::from_literals(args)) {
                Err(error) => error,
                Ok(_) => panic!("malformed setting unexpectedly parsed"),
            };
            assert!(!error.to_string().contains("synthetic-private-token"));
        }
    }

    fn agent_context(args: &[&str]) -> Vec<AgentContextEntry> {
        let matches = Opt::clap_command()
            .try_get_matches_from(std::iter::once("buck2").chain(args.iter().copied()))
            .expect("test arguments should parse");
        Opt::from_clap_matches(&matches)
            .expect("test arguments should construct Opt")
            .common_opts
            .agent_context
    }

    fn entries(values: &[&str]) -> Vec<AgentContextEntry> {
        values
            .iter()
            .map(|value| parse_agent_context(value).unwrap())
            .collect()
    }

    #[test]
    fn agent_context_accumulates_across_subcommand() {
        for args in [
            [
                "--agent-context",
                "direct_call=true",
                "--agent-context",
                "reason=verify",
                "build",
                "//foo:bar",
            ],
            [
                "build",
                "//foo:bar",
                "--agent-context",
                "direct_call=true",
                "--agent-context",
                "reason=verify",
            ],
            [
                "--agent-context",
                "direct_call=true",
                "build",
                "//foo:bar",
                "--agent-context",
                "reason=verify",
            ],
        ] {
            assert_eq!(
                agent_context(&args),
                entries(&["direct_call=true", "reason=verify"])
            );
        }
    }

    #[test]
    fn agent_context_preserves_nested_and_repeated_entries() {
        assert_eq!(
            agent_context(&[
                "--agent-context=reason=outer",
                "docs",
                "--agent-context=reason=middle",
                "agent",
                "--agent-context=reason=inner",
            ]),
            entries(&["reason=outer", "reason=middle", "reason=inner"]),
        );
    }

    #[test]
    fn agent_context_preserves_comma_separated_entries() {
        assert_eq!(
            agent_context(&[
                "--agent-context=direct_call=true,advice_ack=build_intent",
                "build",
                "//foo:bar",
                "--agent-context",
                "direct_call=false,advice_ack=other",
            ]),
            entries(&[
                "direct_call=true",
                "advice_ack=build_intent",
                "direct_call=false",
                "advice_ack=other",
            ]),
        );
    }

    #[test]
    fn agent_context_excludes_arguments_after_separator() {
        for command in ["run", "test"] {
            assert_eq!(
                agent_context(&[
                    "--agent-context",
                    "direct_call=true",
                    command,
                    "//foo:bar",
                    "--",
                    "--agent-context",
                    "direct_call=false",
                ]),
                entries(&["direct_call=true"]),
            );
        }
    }

    #[test]
    fn agent_context_excludes_implicit_run_arguments() {
        assert_eq!(
            agent_context(&[
                "--agent-context",
                "direct_call=true",
                "run",
                "//foo:bar",
                "program-arg",
                "--agent-context",
                "direct_call=false",
            ]),
            entries(&["direct_call=true"]),
        );
    }
}
