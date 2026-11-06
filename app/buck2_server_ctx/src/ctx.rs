/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_build_signals::env::BuildSignalsContext;
use buck2_build_signals::env::DeferredBuildSignals;
use buck2_build_signals::env::HasCriticalPathBackend;
use buck2_certs::validate::CertState;
use buck2_cli_proto::client_context::ExitWhen;
use buck2_cli_proto::client_context::PreemptibleWhen;
use buck2_common::legacy_configs::dice::HasInjectedLegacyConfigs;
use buck2_common::legacy_configs::telemetry::redact_buckconfig_input_values;
use buck2_core::fs::project::ProjectRoot;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::pattern::pattern::ParsedPattern;
use buck2_core::pattern::pattern::ParsedPatternWithModifiers;
use buck2_core::pattern::pattern_type::ConfiguredProvidersPatternExtra;
use buck2_data::CommandCriticalEnd;
use buck2_data::CommandCriticalStart;
use buck2_data::DiceCriticalSectionEnd;
use buck2_data::DiceCriticalSectionStart;
use buck2_events::dispatch::EventDispatcher;
use buck2_execute::materialize::materializer::Materializer;
use buck2_fs::paths::file_name::FileName;
use buck2_fs::working_dir::AbsWorkingDir;
use buck2_hash::IntentionallyStdHashMap;
use buck2_util::early_command_timing::EarlyCommandTimingBuilder;
use buck2_wrapper_common::invocation_id::TraceId;
use dice::DiceComputations;
use dice::DiceTransaction;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use futures::future::BoxFuture;

use crate::concurrency::CommandEvents;
use crate::concurrency::CommandTransactionObserver;
use crate::concurrency::ConcurrencyHandler;
use crate::concurrency::DiceUpdater;
use crate::experiment_util::get_experiment_tags;
use crate::stderr_output_guard::StderrOutputGuard;

#[derive(Allocative, Debug)]
pub struct PreviousCommandDataInternal {
    pub external_and_local_configs: Vec<buck2_data::BuckconfigComponent>,
    pub sanitized_argv: Vec<String>,
    pub trace_id: TraceId,
}

#[derive(Allocative, Debug, Default)]
pub struct PreviousCommandData {
    pub data: Option<PreviousCommandDataInternal>,
}

impl PreviousCommandData {
    pub fn process_current_command(
        &mut self,
        event_dispatcher: EventDispatcher,
        current_external_and_local_configs: Vec<buck2_data::BuckconfigComponent>,
        current_sanitized_argv: Vec<String>,
        current_trace: TraceId,
    ) {
        if let Some(PreviousCommandDataInternal {
            external_and_local_configs: external_configs,
            sanitized_argv,
            trace_id,
        }) = self.data.as_ref()
        {
            if *current_external_and_local_configs != *external_configs {
                event_dispatcher.instant_event(buck2_data::PreviousCommandWithMismatchedConfig {
                    sanitized_argv: sanitized_argv.clone(),
                    trace_id: trace_id.to_string(),
                });
            }
        }

        self.data = Some(PreviousCommandDataInternal {
            external_and_local_configs: current_external_and_local_configs,
            sanitized_argv: current_sanitized_argv,
            trace_id: current_trace,
        });
    }
}

#[derive(Allocative, Debug, Default)]
pub struct LockedPreviousCommandData {
    pub data: Mutex<PreviousCommandData>,
}
impl LockedPreviousCommandData {
    pub fn new() -> Arc<Self> {
        Arc::new(LockedPreviousCommandData {
            data: Mutex::new(PreviousCommandData { data: None }),
        })
    }
}

#[derive(Clone, Dupe)]
pub struct DispatcherEvents(pub EventDispatcher);

impl CommandEvents for DispatcherEvents {
    fn instant(&self, data: buck2_data::instant_event::Data) {
        self.0.instant_event(data);
    }

    fn trace_id(&self) -> &TraceId {
        self.0.trace_id()
    }

    fn console_warning(&self, message: String) {
        self.0.console_warning(message);
    }

    fn span<'a, R: Send + 'a>(
        &self,
        start: buck2_data::span_start_event::Data,
        fut: BoxFuture<'a, (R, buck2_data::span_end_event::Data)>,
    ) -> BoxFuture<'a, R> {
        Box::pin(self.0.span_async(start, fut))
    }
}

/// Emits the buckconfig-derived telemetry for a command: the comparison against the previous
/// command's config, the experiment tags, and the config values themselves.
/// Concurrent equivalent-state commands update previous-command telemetry in observer completion
/// order, so the stored UUID is a comparison anchor rather than a strict admission predecessor.
struct BuckconfigTelemetry<'a> {
    events: EventDispatcher,
    project_root: &'a ProjectRoot,
    previous_command_data: Arc<LockedPreviousCommandData>,
    sanitized_argv: Vec<String>,
    trace_id: TraceId,
}

#[async_trait]
impl CommandTransactionObserver for BuckconfigTelemetry<'_> {
    async fn on_transaction_committed(
        &self,
        transaction: &DiceTransaction,
    ) -> buck2_error::Result<()> {
        if !transaction
            .ctx()
            .is_injected_external_buckconfig_data_key_set()
            .await?
        {
            return Ok(());
        }

        let external_configs = transaction
            .ctx()
            .get_injected_external_buckconfig_data()
            .await?;
        let current_external_and_local_configs: Vec<buck2_data::BuckconfigComponent> =
            external_configs
                .get_buckconfig_components(self.project_root)
                .await;

        self.previous_command_data
            .data
            .lock()
            .unwrap()
            .process_current_command(
                self.events.dupe(),
                current_external_and_local_configs.clone(),
                self.sanitized_argv.clone(),
                self.trace_id.dupe(),
            );

        self.events.instant_event(buck2_data::TagEvent {
            tags: get_experiment_tags(&current_external_and_local_configs),
        });
        let mut config_event = buck2_data::BuckconfigInputValues {
            components: current_external_and_local_configs,
        };
        // Keep execution and mismatch detection on real values; only the event
        // copy is redacted, before any native event sink can serialize it.
        redact_buckconfig_input_values(&mut config_event);
        self.events.instant_event(config_event);

        Ok(())
    }
}

#[async_trait]
pub trait ServerCommandContextTrait: Send + Sync {
    fn working_dir(&self) -> &ProjectRelativePath;

    fn working_dir_abs(&self) -> &AbsWorkingDir;

    fn command_name(&self) -> &str;

    fn isolation_prefix(&self) -> &FileName;

    fn project_root(&self) -> &ProjectRoot;

    fn cert_state(&self) -> CertState;

    fn materializer(&self) -> Arc<dyn Materializer>;

    /// exposes the dice for scoped access, but isn't intended to be callable by anyone
    async fn dice_accessor<'a>(
        &'a self,
        private: PrivateStruct,
    ) -> buck2_error::Result<DiceAccessor<'a>>;

    fn events(&self) -> &EventDispatcher;

    fn previous_command_data(&self) -> Arc<LockedPreviousCommandData>;

    fn stderr(&self) -> buck2_error::Result<StderrOutputGuard<'_>>;

    async fn command_start_event(
        &self,
        data: buck2_data::command_start::Data,
    ) -> buck2_error::Result<buck2_data::CommandStart>;

    async fn request_metadata(
        &self,
    ) -> buck2_error::Result<IntentionallyStdHashMap<String, String>>;

    async fn config_metadata(
        &self,
        ctx: &mut DiceComputations<'_>,
    ) -> buck2_error::Result<IntentionallyStdHashMap<String, String>>;

    fn log_target_pattern(
        &self,
        providers_patterns: &[ParsedPattern<ConfiguredProvidersPatternExtra>],
    );

    fn log_target_pattern_with_modifiers(
        &self,
        providers_patterns_with_modifiers: &[ParsedPatternWithModifiers<
            ConfiguredProvidersPatternExtra,
        >],
    );

    fn cancellation_context(&self) -> &CancellationContext;

    fn command_start(&self) -> Instant;
}

pub struct PrivateStruct(());

pub struct DiceAccessor<'a> {
    pub dice_handler: Arc<ConcurrencyHandler>,
    pub setup: Box<dyn DiceUpdater + 'a>,
    pub is_nested_invocation: bool,
    pub sanitized_argv: Vec<String>,
    pub preemptible: PreemptibleWhen,
    pub build_signals: Box<dyn DeferredBuildSignals>,
    pub exit_when: ExitWhen,
}

#[async_trait]
pub trait ServerCommandDiceContext {
    async fn with_dice_ctx<'v, F, Fut, R>(&'v self, exec: F) -> buck2_error::Result<R>
    where
        F: FnOnce(&'v dyn ServerCommandContextTrait, DiceTransaction) -> Fut + Send,
        Fut: Future<Output = buck2_error::Result<R>> + Send,
        R: Send;

    async fn with_dice_ctx_maybe_exclusive<'v, F, Fut, R>(
        &'v self,
        exec: F,
        exclusive_cmd: Option<String>,
    ) -> buck2_error::Result<R>
    where
        F: FnOnce(&'v dyn ServerCommandContextTrait, DiceTransaction) -> Fut + Send,
        Fut: Future<Output = buck2_error::Result<R>> + Send,
        R: Send;
}

#[async_trait]
impl ServerCommandDiceContext for dyn ServerCommandContextTrait + '_ {
    /// Allows running a section of code that uses the shared DiceTransaction
    async fn with_dice_ctx<'v, F, Fut, R>(&'v self, exec: F) -> buck2_error::Result<R>
    where
        F: FnOnce(&'v dyn ServerCommandContextTrait, DiceTransaction) -> Fut + Send,
        Fut: Future<Output = buck2_error::Result<R>> + Send,
        R: Send,
    {
        self.with_dice_ctx_maybe_exclusive(exec, None).await
    }

    async fn with_dice_ctx_maybe_exclusive<'v, F, Fut, R>(
        &'v self,
        exec: F,
        exclusive_cmd: Option<String>,
    ) -> buck2_error::Result<R>
    where
        F: FnOnce(&'v dyn ServerCommandContextTrait, DiceTransaction) -> Fut + Send,
        Fut: Future<Output = buck2_error::Result<R>> + Send,
        R: Send,
    {
        let DiceAccessor {
            dice_handler,
            setup,
            is_nested_invocation,
            sanitized_argv,
            preemptible,
            build_signals,
            exit_when,
        } = self.dice_accessor(PrivateStruct(())).await?;

        let early_command_timing = EarlyCommandTimingBuilder::new(self.command_start());

        let transaction_observer = BuckconfigTelemetry {
            events: self.events().dupe(),
            project_root: self.project_root(),
            previous_command_data: self.previous_command_data(),
            sanitized_argv: sanitized_argv.clone(),
            trace_id: self.events().trace_id().dupe(),
        };

        let events = self.events().dupe();
        events
            .span_async(DiceCriticalSectionStart {}, async move {
                (
                    dice_handler
                        .enter(
                            DispatcherEvents(self.events().dupe()),
                            &*setup,
                            |dice, early_command_timing| async move {
                                let events = self.events().dupe();

                                let request_metadata = self.request_metadata().await?;
                                let config_metadata = self.config_metadata(&mut dice.ctx()).await?;
                                events
                                    .span_async(
                                        CommandCriticalStart {
                                            metadata: config_metadata.clone(),
                                            dice_version: dice.version().to_string(),
                                        },
                                        async move {
                                            let res = buck2_build_signals::env::scope(
                                                build_signals,
                                                self.events().dupe(),
                                                dice.per_transaction_data()
                                                    .get_critical_path_backend(),
                                                BuildSignalsContext {
                                                    command_name: self.command_name().to_owned(),
                                                    metadata: request_metadata
                                                        .into_iter()
                                                        .chain(
                                                            config_metadata.iter().map(|(k, v)| {
                                                                (k.clone(), v.clone())
                                                            }),
                                                        )
                                                        .collect(),
                                                    isolation_prefix: self
                                                        .isolation_prefix()
                                                        .to_owned(),
                                                    early_command_timing: early_command_timing
                                                        .finish_early_command_timing(),
                                                },
                                                || exec(self, dice),
                                            )
                                            .await;

                                            (
                                                res,
                                                CommandCriticalEnd {
                                                    metadata: config_metadata,
                                                },
                                            )
                                        },
                                    )
                                    .await
                            },
                            is_nested_invocation,
                            sanitized_argv,
                            exclusive_cmd,
                            self.cancellation_context(),
                            preemptible,
                            &transaction_observer,
                            exit_when,
                            early_command_timing,
                        )
                        .await,
                    DiceCriticalSectionEnd {},
                )
            })
            .await?
    }
}

#[cfg(test)]
mod telemetry_tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use buck2_events::Event;
    use buck2_events::EventSink;
    use buck2_events::daemon_id::DaemonId;

    use super::*;

    struct CountEvents(Arc<AtomicUsize>);

    impl EventSink for CountEvents {
        fn send(&self, _event: Event) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn redaction_keeps_private_config_comparisons_exact() {
        let count = Arc::new(AtomicUsize::new(0));
        let dispatcher = EventDispatcher::new(
            TraceId::null(),
            DaemonId::null(),
            CountEvents(count.clone()),
        );
        let mut previous = PreviousCommandData::default();
        for (value, expected_mismatches) in [
            ("synthetic-first-credential", 0),
            ("synthetic-first-credential", 0),
            ("synthetic-rotated-credential", 1),
        ] {
            let mut event = buck2_data::BuckconfigInputValues {
                components: vec![buck2_data::BuckconfigComponent {
                    data: Some(buck2_data::buckconfig_component::Data::ConfigValue(
                        buck2_data::ConfigValue {
                            section: "ordinary".to_owned(),
                            key: "choice".to_owned(),
                            value: value.to_owned(),
                            ..Default::default()
                        },
                    )),
                }],
            };
            // This is the observer's ordering: compare/store real values first,
            // then redact only the separate native event payload.
            previous.process_current_command(
                dispatcher.dupe(),
                event.components.clone(),
                Vec::new(),
                TraceId::null(),
            );
            redact_buckconfig_input_values(&mut event);
            assert_eq!(count.load(Ordering::Relaxed), expected_mismatches);
            let stored = &previous.data.as_ref().unwrap().external_and_local_configs[0];
            let Some(buck2_data::buckconfig_component::Data::ConfigValue(stored)) = &stored.data else {
                panic!("missing stored config");
            };
            assert_eq!(stored.value, value);
            let Some(buck2_data::buckconfig_component::Data::ConfigValue(logged)) = &event.components[0].data else {
                panic!("missing event config");
            };
            assert_eq!(logged.value, "<redacted>");
        }
    }
}
