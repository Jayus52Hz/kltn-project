import json
import os
import queue
import shlex
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env"
STATE_FILE = ROOT / "state.json"
LOG_FILE = ROOT / "bridge.log"
TELEGRAM_LIMIT = 3900


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        env[key.strip()] = value
    return env


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def log(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def cap_words(text: str, max_words: int) -> str:
    if max_words <= 0:
        return text
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "\n\n[Output truncated by Telegram bridge.]"


class TelegramClient:
    def __init__(self, token: str) -> None:
        self.base_url = f"https://api.telegram.org/bot{token}/"

    def call(self, method: str, payload: dict | None = None, timeout: int = 60) -> dict:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base_url + method, data=data, headers=headers)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
        result = json.loads(body)
        if not result.get("ok"):
            raise RuntimeError(result)
        return result

    def send_message(self, chat_id: int, text: str) -> None:
        if not text:
            return
        for start in range(0, len(text), TELEGRAM_LIMIT):
            chunk = text[start : start + TELEGRAM_LIMIT]
            self.call(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": chunk,
                    "disable_web_page_preview": True,
                },
                timeout=30,
            )


class Bridge:
    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.state = load_state()
        self.client = TelegramClient(config["TELEGRAM_BOT_TOKEN"])
        self.workspace_root = Path(config.get("WORKSPACE_ROOT", ".")).expanduser().resolve()
        self.codex_command = config.get("CODEX_COMMAND", "codex")
        self.codex_extra_args = shlex.split(config.get("CODEX_EXTRA_ARGS", ""))
        self.prompt_suffix = config.get("CODEX_PROMPT_SUFFIX", "").strip()
        self.send_progress_chunks = config.get("SEND_PROGRESS_CHUNKS", "false").lower() == "true"
        self.output_max_words = int(config.get("OUTPUT_MAX_WORDS", "500"))
        self.codex_timeout = int(config.get("CODEX_TIMEOUT_SECONDS", "1800"))
        self.poll_timeout = int(config.get("TELEGRAM_POLL_TIMEOUT_SECONDS", "50"))
        self.claim_code = config.get("CLAIM_CODE", "")
        self.allowed_chat_id = self._configured_chat_id()
        self.repo_names = [
            item.strip()
            for item in config.get("ALLOWED_REPOS", "").split(",")
            if item.strip()
        ]
        self.current_repo = self.state.get("current_repo") or self._default_repo()
        self.job_queue: queue.Queue[tuple[int, str]] = queue.Queue()
        self.active_process: subprocess.Popen | None = None
        self.cancel_requested = False

    def _configured_chat_id(self) -> int | None:
        raw = self.config.get("TELEGRAM_ALLOWED_CHAT_ID") or self.state.get("allowed_chat_id")
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            raise ValueError("TELEGRAM_ALLOWED_CHAT_ID must be numeric")

    def _default_repo(self) -> str:
        if self.repo_names:
            return self.repo_names[0]
        return "."

    def _repo_path(self, repo_name: str) -> Path:
        repo = (self.workspace_root / repo_name).resolve()
        try:
            repo.relative_to(self.workspace_root)
        except ValueError as exc:
            raise ValueError("Repo path escaped WORKSPACE_ROOT") from exc
        if not repo.exists() or not repo.is_dir():
            raise ValueError(f"Repo does not exist: {repo_name}")
        return repo

    def _is_authorized(self, chat_id: int) -> bool:
        return self.allowed_chat_id is not None and chat_id == self.allowed_chat_id

    def start(self) -> None:
        worker = threading.Thread(target=self._worker_loop, daemon=True)
        worker.start()
        self.client.call("deleteWebhook", {"drop_pending_updates": True}, timeout=30)
        self._poll_loop()

    def _poll_loop(self) -> None:
        offset = self.state.get("telegram_offset", 0)
        log("Bridge started")
        while True:
            try:
                response = self.client.call(
                    "getUpdates",
                    {
                        "offset": offset,
                        "timeout": self.poll_timeout,
                        "allowed_updates": ["message"],
                    },
                    timeout=self.poll_timeout + 10,
                )
                for update in response.get("result", []):
                    offset = update["update_id"] + 1
                    self.state["telegram_offset"] = offset
                    save_state(self.state)
                    self._handle_update(update)
            except (urllib.error.URLError, TimeoutError, RuntimeError) as exc:
                log(f"Polling error: {exc}")
                time.sleep(5)

    def _handle_update(self, update: dict) -> None:
        message = update.get("message") or {}
        text = (message.get("text") or "").strip()
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        if not text or chat_id is None:
            return

        if text.startswith("/claim"):
            self._handle_claim(chat_id, text)
            return

        if not self._is_authorized(chat_id):
            self.client.send_message(
                chat_id,
                "This bot is locked. Send /claim <code> from the authorized Telegram account.",
            )
            return

        if text in {"/start", "/help"}:
            self._send_help(chat_id)
        elif text == "/repos":
            self._send_repos(chat_id)
        elif text.startswith("/use "):
            self._use_repo(chat_id, text.removeprefix("/use ").strip())
        elif text == "/status":
            self._send_status(chat_id)
        elif text == "/cancel":
            self._cancel(chat_id)
        elif text.startswith("/run "):
            self._enqueue(chat_id, text.removeprefix("/run ").strip())
        elif text.startswith("/"):
            self.client.send_message(chat_id, "Unknown command. Send /help.")
        else:
            self._enqueue(chat_id, text)

    def _handle_claim(self, chat_id: int, text: str) -> None:
        if self.allowed_chat_id is not None:
            self.client.send_message(chat_id, "This bridge is already claimed.")
            return
        if not self.claim_code:
            self.client.send_message(chat_id, "CLAIM_CODE is not configured on the PC.")
            return
        provided = text.removeprefix("/claim").strip()
        if provided != self.claim_code:
            self.client.send_message(chat_id, "Invalid claim code.")
            return
        self.allowed_chat_id = chat_id
        self.state["allowed_chat_id"] = chat_id
        save_state(self.state)
        self.client.send_message(chat_id, f"Bridge claimed by chat_id {chat_id}. Send /help.")

    def _send_help(self, chat_id: int) -> None:
        self.client.send_message(
            chat_id,
            "\n".join(
                [
                    "Codex bridge commands:",
                    "/repos - list allowed repos",
                    "/use <repo> - switch active repo",
                    "/run <prompt> - run Codex in active repo",
                    "/status - show active repo and queue",
                    "/cancel - stop current Codex process",
                    "",
                    "Plain messages are treated as Codex prompts.",
                ]
            ),
        )

    def _send_repos(self, chat_id: int) -> None:
        repos = self.repo_names or ["."]
        self.client.send_message(chat_id, "Allowed repos:\n" + "\n".join(f"- {name}" for name in repos))

    def _use_repo(self, chat_id: int, repo_name: str) -> None:
        if repo_name not in (self.repo_names or ["."]):
            self.client.send_message(chat_id, "Repo is not in ALLOWED_REPOS. Send /repos.")
            return
        try:
            self._repo_path(repo_name)
        except ValueError as exc:
            self.client.send_message(chat_id, str(exc))
            return
        self.current_repo = repo_name
        self.state["current_repo"] = repo_name
        save_state(self.state)
        self.client.send_message(chat_id, f"Active repo: {repo_name}")

    def _send_status(self, chat_id: int) -> None:
        active = "yes" if self.active_process and self.active_process.poll() is None else "no"
        self.client.send_message(
            chat_id,
            f"Active repo: {self.current_repo}\nRunning: {active}\nQueued jobs: {self.job_queue.qsize()}",
        )

    def _cancel(self, chat_id: int) -> None:
        self.cancel_requested = True
        if self.active_process and self.active_process.poll() is None:
            self.active_process.terminate()
            self.client.send_message(chat_id, "Cancel requested.")
        else:
            self.client.send_message(chat_id, "No active Codex process.")

    def _enqueue(self, chat_id: int, prompt: str) -> None:
        if not prompt:
            self.client.send_message(chat_id, "Prompt is empty.")
            return
        self.job_queue.put((chat_id, prompt))
        self.client.send_message(chat_id, f"Queued. Position: {self.job_queue.qsize()}")

    def _worker_loop(self) -> None:
        while True:
            chat_id, prompt = self.job_queue.get()
            self.cancel_requested = False
            try:
                self._run_codex(chat_id, prompt)
            except Exception as exc:
                log(f"Worker error: {exc}")
                self.client.send_message(chat_id, f"Bridge error: {exc}")
            finally:
                self.active_process = None
                self.job_queue.task_done()

    def _run_codex(self, chat_id: int, prompt: str) -> None:
        repo = self._repo_path(self.current_repo)
        effective_prompt = prompt
        if self.prompt_suffix:
            effective_prompt = f"{prompt}\n\n{self.prompt_suffix}"
        command = [self.codex_command, "exec", *self.codex_extra_args, effective_prompt]
        printable = " ".join(shlex.quote(part) for part in command)
        log(f"Running in {repo}: {printable}")
        self.client.send_message(chat_id, f"Running Codex in {self.current_repo}...")
        self.active_process = subprocess.Popen(
            command,
            cwd=str(repo),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output_lines: list[str] = []
        started = time.time()
        assert self.active_process.stdout is not None
        while True:
            if self.cancel_requested:
                break
            if time.time() - started > self.codex_timeout:
                self.active_process.terminate()
                output_lines.append(f"\nTimed out after {self.codex_timeout} seconds.")
                break
            line = self.active_process.stdout.readline()
            if line:
                output_lines.append(line.rstrip())
                if self.send_progress_chunks and len(output_lines) % 40 == 0:
                    self.client.send_message(chat_id, "\n".join(output_lines[-40:]))
            elif self.active_process.poll() is not None:
                break
            else:
                time.sleep(0.2)

        return_code = self.active_process.wait(timeout=10)
        tail = "\n".join(output_lines[-80:]).strip()
        status = "completed" if return_code == 0 else f"exited with code {return_code}"
        message = f"Codex {status} in {self.current_repo}."
        if tail:
            message += "\n\nLast output:\n" + cap_words(tail, self.output_max_words)
        self.client.send_message(chat_id, message)


def main() -> None:
    config = load_env(ENV_FILE)
    token = config.get("TELEGRAM_BOT_TOKEN")
    if not token or token.startswith("PASTE_"):
        raise SystemExit(f"Set TELEGRAM_BOT_TOKEN in {ENV_FILE}")
    if not config.get("WORKSPACE_ROOT"):
        raise SystemExit(f"Set WORKSPACE_ROOT in {ENV_FILE}")
    Bridge(config).start()


if __name__ == "__main__":
    main()
