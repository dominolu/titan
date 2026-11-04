//! Local interactive frontend. No command is ever passed to an operating-system shell.
use super::{
    client::{self, ClientError, ClientOptions},
    frontend::Cli,
};
use anyhow::{Context, Result, ensure};
use clap::{CommandFactory, Parser};
use std::{
    collections::BTreeMap,
    io::{self, IsTerminal, Read, Write},
    path::PathBuf,
    time::Duration,
};

const GROUPS: &[&str] = &["strategy", "account", "research", "config"];
const BUILTINS: &[&str] = &["help", "use", "back", "history", "exit"];
const MAX_LINE: usize = 4096;
const MAX_HISTORY: usize = 100;
const MAX_HISTORY_BYTES: u64 = 512 * 1024;

/// Run only an interactive terminal session; redirected input is never consumed.
pub fn run(options: ClientOptions, initial_group: Option<&str>) -> Result<()> {
    ensure!(
        initial_group.is_none_or(|group| GROUPS.contains(&group)),
        "unknown shell group"
    );
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() || options.json || options.ndjson {
        return print_help(initial_group, &[]);
    }
    #[cfg(unix)]
    {
        run_terminal(options, initial_group)
    }
    #[cfg(not(unix))]
    {
        print_help(initial_group, &[])
    }
}

fn print_help(group: Option<&str>, path: &[String]) -> Result<()> {
    let mut command = Cli::command();
    command.build();
    if let Some(group) = group {
        command = command
            .find_subcommand(group)
            .context("unknown shell group")?
            .clone();
    }
    for part in path {
        let Some(child) = command.find_subcommand(part).cloned() else {
            writeln!(
                io::stdout(),
                "No help for that command. Use help to list this group."
            )?;
            return Ok(());
        };
        command = child;
    }
    command.print_long_help()?;
    writeln!(io::stdout())?;
    writeln!(
        io::stdout(),
        "Shell: strategy/account/research/config enter a group; use selects a profile at root or target in a group. help, Tab, Up/Down, history, back, exit. Ctrl-C clears the line or ends local observation; Ctrl-D exits an empty prompt. Exiting never stops background services or strategies."
    )?;
    Ok(())
}

fn merge_options(base: &ClientOptions, parsed: ClientOptions) -> ClientOptions {
    let mut merged = base.clone();
    macro_rules! override_option {
        ($field:ident) => {
            if parsed.$field.is_some() {
                merged.$field = parsed.$field;
            }
        };
    }
    override_option!(profile);
    override_option!(config);
    override_option!(url);
    override_option!(username);
    override_option!(password_env);
    override_option!(node);
    override_option!(environment);
    merged.no_auto_start |= parsed.no_auto_start;
    if parsed.json {
        merged.json = true;
        merged.ndjson = false;
    }
    if parsed.ndjson {
        merged.ndjson = true;
        merged.json = false;
    }
    merged
}

/// Minimal quote/backslash tokenizer; no expansion, substitution, pipes or shell invocation.
fn words(line: &str) -> Result<Vec<String>> {
    let mut result = Vec::new();
    let mut token = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for ch in line.chars() {
        if escaped {
            token.push(ch);
            escaped = false;
            started = true;
            continue;
        }
        if ch == '\\' && quote != Some('\'') {
            escaped = true;
            started = true;
            continue;
        }
        if let Some(delimiter) = quote {
            if ch == delimiter {
                quote = None;
            } else {
                token.push(ch);
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            started = true;
        } else if ch.is_whitespace() {
            if started {
                result.push(std::mem::take(&mut token));
                started = false;
            }
        } else {
            token.push(ch);
            started = true;
        }
    }
    ensure!(
        !escaped && quote.is_none(),
        ClientError::new("INVALID_INPUT", "unfinished quote or escape")
    );
    if started {
        result.push(token);
    }
    Ok(result)
}

fn safe_label(text: &str, maximum: usize) -> String {
    text.chars()
        .filter(|ch| !ch.is_control())
        .take(maximum)
        .collect()
}

fn history_safe(line: &str) -> bool {
    if line.is_empty() || line.len() > MAX_LINE || line.chars().any(char::is_control) {
        return false;
    }
    let Ok(tokens) = words(line) else {
        return false;
    };
    if tokens.is_empty()
        || !matches!(
            tokens[0].as_str(),
            "strategy"
                | "account"
                | "research"
                | "config"
                | "help"
                | "use"
                | "back"
                | "history"
                | "exit"
                | "quit"
                | "check"
                | "ls"
                | "show"
                | "status"
                | "start"
                | "pause"
                | "resume"
                | "stop"
                | "logs"
                | "balance"
                | "balances"
                | "positions"
                | "orders"
                | "risk"
                | "fills"
        )
        || tokens.len() > 32
        || tokens.iter().any(|token| {
            let lower = token.to_ascii_lowercase();
            token.is_empty()
                || token.len() > 128
                || !token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                || [
                    "password",
                    "secret",
                    "credential",
                    "token",
                    "dsn",
                    "url",
                    "endpoint",
                    "private-key",
                    "client-config",
                ]
                .iter()
                .any(|word| lower.contains(word))
        })
    {
        return false;
    }
    // Configuration and research arguments may contain connection data or arbitrary source parameters.
    !tokens.iter().any(|token| {
        matches!(
            token.as_str(),
            "config" | "database" | "research" | "build" | "backtest" | "report"
        )
    })
}

struct ContextState {
    options: ClientOptions,
    group: Option<String>,
    targets: BTreeMap<String, String>,
}
impl ContextState {
    fn prompt(&self) -> String {
        let profile = super::frontend_config::resolved_options(&self.options)
            .ok()
            .and_then(|options| options.profile)
            .unwrap_or_else(|| "unselected".into());
        let group = self.group.as_deref().unwrap_or("root");
        let target = self
            .group
            .as_ref()
            .and_then(|group| self.targets.get(group))
            .map(String::as_str)
            .unwrap_or("-");
        format!(
            "titan[{}/{}/{}]> ",
            safe_label(&profile, 16),
            group,
            safe_label(target, 16)
        )
    }
    fn expanded(&self, mut input: Vec<String>) -> Vec<String> {
        let explicit_group = input
            .first()
            .is_some_and(|word| GROUPS.contains(&word.as_str()));
        if !explicit_group && let Some(group) = &self.group {
            input.insert(0, group.clone());
        }
        if let Ok(matches) = Cli::command().try_get_matches_from(
            std::iter::once("titan_cli").chain(input.iter().map(String::as_str)),
        ) && let Some((group, group_matches)) = matches.subcommand()
            && let Some((_, action)) = group_matches.subcommand()
            && action
                .try_get_one::<String>("target")
                .is_ok_and(|target| target.is_none())
            && let Some(target) = self.targets.get(group)
        {
            input.push(target.clone());
        }
        input
    }
}

#[cfg(unix)]
fn run_terminal(options: ClientOptions, initial_group: Option<&str>) -> Result<()> {
    let mut state = ContextState {
        options,
        group: initial_group.map(str::to_owned),
        targets: BTreeMap::new(),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let mut history = History::load();
    writeln!(
        io::stdout(),
        "Titan interactive client. help for commands; exit leaves background services and strategies running."
    )?;
    loop {
        let prompt = state.prompt();
        let line = match read_line(&prompt, state.group.as_deref(), &history.lines)? {
            LineResult::Line(line) => line,
            LineResult::Interrupted => continue,
            LineResult::Eof => return Ok(()),
        };
        if line.trim().is_empty() {
            continue;
        }
        let input = match words(&line) {
            Ok(input) => input,
            Err(error) => {
                client::render_error(&error, false);
                continue;
            }
        };
        if input.is_empty() {
            continue;
        }
        history.add(&line);
        if input.len() == 1 && matches!(input[0].as_str(), "exit" | "quit") {
            return Ok(());
        }
        if input[0] == "back" && input.len() == 1 {
            state.group = None;
            continue;
        }
        if input[0] == "history" && input.len() == 1 {
            for (index, entry) in history.lines.iter().enumerate() {
                writeln!(io::stdout(), "{:>3}  {entry}", index + 1)?;
            }
            continue;
        }
        if input[0] == "help" {
            let root_group = input
                .get(1)
                .is_some_and(|word| GROUPS.contains(&word.as_str()));
            print_help(
                if root_group {
                    None
                } else {
                    state.group.as_deref()
                },
                &input[1..],
            )?;
            continue;
        }
        if input.len() == 1 && GROUPS.contains(&input[0].as_str()) {
            state.group = Some(input[0].clone());
            continue;
        }
        if input[0] == "use"
            && let Some(group) = state.group.as_ref()
        {
            if input.len() == 1 {
                writeln!(io::stdout(), "{}", state.prompt())?;
                continue;
            }
            if input.len() != 2
                || input[1].is_empty()
                || input[1].len() > 256
                || input[1].chars().any(char::is_control)
            {
                client::render_error(
                    &ClientError::new(
                        "INVALID_INPUT",
                        "use expects one exact target or quoted unique name",
                    )
                    .into(),
                    false,
                );
                continue;
            }
            if group == "config" {
                writeln!(
                    io::stdout(),
                    "Configuration commands have no target context. back, then use PROFILE to select a profile."
                )?;
            } else {
                state.targets.insert(group.clone(), input[1].clone());
            }
            continue;
        }
        let root_use = state.group.is_none() && input[0] == "use";
        let expanded = if root_use || input[0] == "check" {
            input
        } else {
            state.expanded(input)
        };
        let parsed =
            match Cli::try_parse_from(std::iter::once("titan_cli".to_owned()).chain(expanded)) {
                Ok(parsed) => parsed,
                Err(error) => {
                    error.print()?;
                    continue;
                }
            };
        let Some(command) = parsed.command else {
            print_help(state.group.as_deref(), &[])?;
            continue;
        };
        let profile_change = match &command {
            super::frontend::Command::Use { profile } => Some(profile.clone()),
            _ => None,
        };
        let options = merge_options(&state.options, parsed.options);
        let result = runtime.block_on(async {
            tokio::select! {
                result = super::frontend::dispatch(options, command) => result,
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    Err(ClientError::new("INTERRUPTED", "local command observation interrupted; remote operations were not cancelled").into())
                }
            }
        });
        match result {
            Ok(()) => {
                if let Some(profile) = profile_change {
                    state.options.profile = Some(profile);
                    state.targets.clear();
                }
            }
            Err(error) => client::render_error(&error, false),
        }
    }
}

/// Completion uses Clap metadata only. It never resolves remote targets or authenticates.
fn completions(line: &str, cursor: usize, group: Option<&str>) -> (usize, Vec<String>) {
    let before = &line[..cursor];
    let start = before
        .char_indices()
        .rev()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(0, |(index, ch)| index + ch.len_utf8());
    let prefix = &before[start..];
    let mut tokens: Vec<&str> = before[..start].split_whitespace().collect();
    let mut command = Cli::command();
    command.build();
    if let Some(group) = group
        && tokens.first().is_none_or(|word| !GROUPS.contains(word))
        && let Some(child) = command.find_subcommand(group)
    {
        command = child.clone();
    }
    while let Some(word) = tokens.first().copied() {
        let Some(child) = command.find_subcommand(word).cloned() else {
            break;
        };
        command = child;
        tokens.remove(0);
    }
    let mut candidates = Vec::new();
    if prefix.starts_with('-') {
        for argument in command
            .get_arguments()
            .filter(|argument| !argument.is_hide_set())
        {
            if let Some(long) = argument.get_long() {
                candidates.push(format!("--{long}"));
            }
        }
    } else if tokens.is_empty() {
        candidates.extend(
            command
                .get_subcommands()
                .filter(|child| !child.is_hide_set())
                .map(|child| child.get_name().to_owned()),
        );
        if command.get_name() == "strategy" {
            candidates.push("status".into());
        }
        if start == 0 {
            candidates.extend(GROUPS.iter().chain(BUILTINS).map(|word| (*word).to_owned()));
        }
    }
    candidates.retain(|candidate| candidate.starts_with(prefix));
    candidates.sort();
    candidates.dedup();
    candidates.truncate(32);
    (start, candidates)
}

#[cfg(unix)]
struct History {
    path: Option<PathBuf>,
    lines: Vec<String>,
}
#[cfg(unix)]
impl History {
    fn load() -> Self {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
        let path = (|| -> Result<PathBuf> {
            let base = std::env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state"))
                })
                .context("no private history location")?;
            ensure!(
                base.is_absolute(),
                "history state directory must be absolute"
            );
            let directory = base.join("titan/cli");
            std::fs::create_dir_all(&directory)?;
            let metadata = std::fs::symlink_metadata(&directory)?;
            ensure!(
                metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && metadata.uid() == unsafe { libc::geteuid() },
                "unsafe history directory"
            );
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
            let path = directory.join("history.json");
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) => ensure!(
                    metadata.is_file()
                        && !metadata.file_type().is_symlink()
                        && metadata.uid() == unsafe { libc::geteuid() }
                        && metadata.nlink() == 1
                        && metadata.mode() & 0o077 == 0,
                    "unsafe history file"
                ),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            Ok(path)
        })()
        .ok();
        let mut lines = Vec::new();
        if let Some(path) = &path
            && let Ok(file) = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
            && file.metadata().is_ok_and(|metadata| {
                metadata.is_file()
                    && metadata.uid() == unsafe { libc::geteuid() }
                    && metadata.mode() & 0o077 == 0
                    && metadata.len() <= MAX_HISTORY_BYTES
            })
        {
            let mut text = String::new();
            if file
                .take(MAX_HISTORY_BYTES + 1)
                .read_to_string(&mut text)
                .is_ok()
                && let Ok(saved) = serde_json::from_str::<Vec<String>>(&text)
            {
                lines = saved
                    .into_iter()
                    .filter(|line| history_safe(line))
                    .rev()
                    .take(MAX_HISTORY)
                    .collect();
                lines.reverse();
            }
        }
        Self { path, lines }
    }
    fn add(&mut self, line: &str) {
        if !history_safe(line) || self.lines.last().is_some_and(|last| last == line) {
            return;
        }
        self.lines.push(line.to_owned());
        if self.lines.len() > MAX_HISTORY {
            self.lines.remove(0);
        }
        let Some(path) = &self.path else {
            return;
        };
        let saved = (|| -> Result<()> {
            let mut temporary = tempfile::NamedTempFile::new_in(
                path.parent().context("history has no directory")?,
            )?;
            serde_json::to_writer(temporary.as_file_mut(), &self.lines)?;
            temporary.as_file_mut().sync_all()?;
            temporary
                .persist(path)
                .map_err(|_| anyhow::anyhow!("cannot save private history"))?;
            Ok(())
        })();
        if saved.is_err() {
            self.path = None;
        }
    }
}

#[cfg(unix)]
struct TerminalGuard {
    original: libc::termios,
}
#[cfg(unix)]
impl TerminalGuard {
    fn enter() -> Result<Self> {
        let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
        ensure!(
            unsafe { libc::tcgetattr(libc::STDIN_FILENO, original.as_mut_ptr()) } == 0,
            "cannot read terminal settings"
        );
        let original = unsafe { original.assume_init() };
        let mut raw = original;
        unsafe {
            libc::cfmakeraw(&mut raw);
        }
        raw.c_oflag = original.c_oflag;
        ensure!(
            unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw) } == 0,
            "cannot enter terminal input mode"
        );
        let guard = Self { original };
        io::stdout().write_all(b"\x1b[?2004h")?;
        io::stdout().flush()?;
        Ok(guard)
    }
}
#[cfg(unix)]
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = io::stdout().write_all(b"\x1b[?2004l");
        let _ = io::stdout().flush();
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.original);
        }
    }
}
#[cfg(unix)]
fn input_byte(timeout: Option<Duration>) -> io::Result<Option<u8>> {
    let mut descriptor = libc::pollfd {
        fd: libc::STDIN_FILENO,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let milliseconds = timeout.map_or(-1, |timeout| {
            timeout.as_millis().min(i32::MAX as u128) as i32
        });
        let ready = unsafe { libc::poll(&mut descriptor, 1, milliseconds) };
        if ready == 0 {
            return Ok(None);
        }
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        let mut byte = 0_u8;
        let read = unsafe { libc::read(libc::STDIN_FILENO, (&mut byte as *mut u8).cast(), 1) };
        if read == 1 {
            return Ok(Some(byte));
        }
        if read == 0 {
            return Ok(None);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}
#[cfg(unix)]
enum LineResult {
    Line(String),
    Interrupted,
    Eof,
}
fn display_width(ch: char) -> usize {
    match ch as u32 {
        0x0300..=0x036f | 0x1ab0..=0x1aff | 0x1dc0..=0x1dff | 0xfe00..=0xfe0f | 0xfe20..=0xfe2f => {
            0
        }
        0x1100..=0x115f
        | 0x2329..=0x232a
        | 0x2e80..=0xa4cf
        | 0xac00..=0xd7a3
        | 0xf900..=0xfaff
        | 0xfe10..=0xfe19
        | 0xfe30..=0xfe6f
        | 0xff00..=0xff60
        | 0xffe0..=0xffe6
        | 0x1f300..=0x1faff
        | 0x20000..=0x3ffff => 2,
        _ => 1,
    }
}
#[cfg(unix)]
fn redraw(prompt: &str, line: &str, cursor: usize) -> Result<()> {
    let mut size = std::mem::MaybeUninit::<libc::winsize>::zeroed();
    let columns =
        if unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, size.as_mut_ptr()) } == 0 {
            usize::from(unsafe { size.assume_init() }.ws_col).clamp(20, 500)
        } else {
            80
        };
    let mut visible_prompt = String::new();
    let mut prompt_width = 0;
    for ch in prompt.chars() {
        let width = display_width(ch);
        if prompt_width + width > columns.saturating_sub(12) {
            break;
        }
        visible_prompt.push(ch);
        prompt_width += width;
    }
    let available = columns.saturating_sub(prompt_width + 1);
    let mut start = 0;
    while line[start..cursor]
        .chars()
        .map(display_width)
        .sum::<usize>()
        >= available
    {
        let Some(ch) = line[start..cursor].chars().next() else {
            break;
        };
        start += ch.len_utf8();
    }
    let mut visible = String::new();
    let mut used = 0;
    for ch in line[start..].chars() {
        let width = display_width(ch);
        if used + width > available {
            break;
        }
        visible.push(ch);
        used += width;
    }
    let position = prompt_width
        + line[start..cursor]
            .chars()
            .map(display_width)
            .sum::<usize>();
    let mut stdout = io::stdout().lock();
    write!(stdout, "\r\x1b[2K{visible_prompt}{visible}\r")?;
    if position > 0 {
        write!(stdout, "\x1b[{position}C")?;
    }
    stdout.flush()?;
    Ok(())
}
#[cfg(unix)]
fn previous_boundary(line: &str, cursor: usize) -> usize {
    line[..cursor]
        .char_indices()
        .next_back()
        .map_or(0, |(index, _)| index)
}
#[cfg(unix)]
fn next_boundary(line: &str, cursor: usize) -> usize {
    cursor + line[cursor..].chars().next().map_or(0, char::len_utf8)
}
#[cfg(unix)]
fn read_line(prompt: &str, group: Option<&str>, history: &[String]) -> Result<LineResult> {
    let _terminal = TerminalGuard::enter()?;
    let mut line = String::new();
    let mut cursor = 0;
    let mut history_index = history.len();
    let mut draft = String::new();
    redraw(prompt, &line, cursor)?;
    loop {
        let Some(byte) = input_byte(None)? else {
            writeln!(io::stdout())?;
            return Ok(LineResult::Eof);
        };
        match byte {
            b'\r' | b'\n' => {
                writeln!(io::stdout())?;
                return Ok(LineResult::Line(line));
            }
            3 => {
                writeln!(io::stdout(), "^C")?;
                return Ok(LineResult::Interrupted);
            }
            4 if line.is_empty() => {
                writeln!(io::stdout())?;
                return Ok(LineResult::Eof);
            }
            4 => {
                if cursor < line.len() {
                    let end = next_boundary(&line, cursor);
                    line.replace_range(cursor..end, "");
                }
            }
            1 => cursor = 0,
            5 => cursor = line.len(),
            11 => line.truncate(cursor),
            21 => {
                line.replace_range(..cursor, "");
                cursor = 0;
            }
            8 | 127 => {
                if cursor > 0 {
                    let previous = previous_boundary(&line, cursor);
                    line.replace_range(previous..cursor, "");
                    cursor = previous;
                }
            }
            b'\t' => {
                let (start, choices) = completions(&line, cursor, group);
                if choices.len() == 1 {
                    let replacement = format!("{} ", choices[0]);
                    if line.len() - (cursor - start) + replacement.len() <= MAX_LINE {
                        line.replace_range(start..cursor, &replacement);
                        cursor = start + replacement.len();
                    }
                } else if !choices.is_empty() {
                    writeln!(io::stdout(), "\n{}", choices.join("  "))?;
                }
            }
            27 => {
                let mut sequence = Vec::new();
                for _ in 0..8 {
                    let Some(next) = input_byte(Some(Duration::from_millis(30)))? else {
                        break;
                    };
                    sequence.push(next);
                    if next.is_ascii_alphabetic() || next == b'~' {
                        break;
                    }
                }
                match sequence.as_slice() {
                    b"[A" => {
                        if history_index == history.len() {
                            draft = line.clone();
                        }
                        if history_index > 0 {
                            history_index -= 1;
                            line = history[history_index].clone();
                            cursor = line.len();
                        }
                    }
                    b"[B" => {
                        if history_index < history.len() {
                            history_index += 1;
                            line = if history_index == history.len() {
                                draft.clone()
                            } else {
                                history[history_index].clone()
                            };
                            cursor = line.len();
                        }
                    }
                    b"[C" => cursor = next_boundary(&line, cursor),
                    b"[D" => cursor = previous_boundary(&line, cursor),
                    b"[H" | b"OH" | b"[1~" => cursor = 0,
                    b"[F" | b"OF" | b"[4~" => cursor = line.len(),
                    b"[3~" => {
                        if cursor < line.len() {
                            let end = next_boundary(&line, cursor);
                            line.replace_range(cursor..end, "");
                        }
                    }
                    b"[200~" => {
                        let mut pasted = Vec::new();
                        let mut suffix = Vec::new();
                        loop {
                            let Some(next) = input_byte(None)? else {
                                return Ok(LineResult::Eof);
                            };
                            if next == 3 {
                                writeln!(io::stdout(), "^C")?;
                                return Ok(LineResult::Interrupted);
                            }
                            suffix.push(next);
                            if suffix.ends_with(b"\x1b[201~") {
                                for byte in &suffix[..suffix.len() - 6] {
                                    if pasted.len() < MAX_LINE {
                                        pasted.push(*byte);
                                    }
                                }
                                break;
                            }
                            if suffix.len() > 6 {
                                let first = suffix.remove(0);
                                if pasted.len() < MAX_LINE {
                                    pasted.push(first);
                                }
                            }
                        }
                        for ch in String::from_utf8_lossy(&pasted).chars() {
                            let ch = if ch == '\n' || ch == '\r' || ch == '\t' {
                                ' '
                            } else {
                                ch
                            };
                            if !ch.is_control() && line.len() + ch.len_utf8() <= MAX_LINE {
                                line.insert(cursor, ch);
                                cursor += ch.len_utf8();
                            }
                        }
                    }
                    _ => {}
                }
            }
            0..=31 => {}
            _ => {
                let mut bytes = vec![byte];
                if byte >= 128 {
                    let expected = if byte & 0xe0 == 0xc0 {
                        2
                    } else if byte & 0xf0 == 0xe0 {
                        3
                    } else if byte & 0xf8 == 0xf0 {
                        4
                    } else {
                        1
                    };
                    for _ in 1..expected {
                        if let Some(next) = input_byte(None)? {
                            bytes.push(next);
                        }
                    }
                }
                if let Ok(text) = std::str::from_utf8(&bytes)
                    && line.len() + text.len() <= MAX_LINE
                    && !text.chars().any(char::is_control)
                {
                    line.insert_str(cursor, text);
                    cursor += text.len();
                }
            }
        }
        redraw(prompt, &line, cursor)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tokenizer_has_quotes_but_no_expansion() {
        assert_eq!(
            words("show 'a name' \"$HOME\" ;$(test)").unwrap(),
            vec!["show", "a name", "$HOME", ";$(test)"]
        );
        assert!(words("show 'unfinished").is_err());
    }
    #[test]
    fn private_history_rejects_configuration_and_secret_shapes() {
        assert!(history_safe("strategy show example-run"));
        assert!(!history_safe("config database postgres://fixture-dsn"));
        assert!(!history_safe("show --password-env TEST_PASSWORD"));
        assert!(!history_safe("config show"));
        assert!(!history_safe("show secret://env/TEST"));
    }
    #[test]
    fn target_context_is_not_added_to_list_or_explicit_target() {
        let state = ContextState {
            options: ClientOptions::default(),
            group: Some("strategy".into()),
            targets: BTreeMap::from([("strategy".into(), "selected".into())]),
        };
        assert_eq!(state.expanded(vec!["ls".into()]), vec!["strategy", "ls"]);
        assert_eq!(
            state.expanded(vec!["show".into()]),
            vec!["strategy", "show", "selected"]
        );
        assert_eq!(
            state.expanded(vec!["stop".into(), "--timeout".into(), "10".into()]),
            vec!["strategy", "stop", "--timeout", "10", "selected"]
        );
        assert_eq!(
            state.expanded(vec!["show".into(), "explicit".into()]),
            vec!["strategy", "show", "explicit"]
        );
    }
}
