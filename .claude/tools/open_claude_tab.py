"""Open a new Claude Code tab in VS Code by firing its keybinding.

The whole of `.claude/workflows/start-a-session.md`, and runbook row `O8`.

WHY A KEYSTROKE. Three routes to open a tab were measured dead on 2026-09-06
(the `code` CLI, the `vscode://Anthropic.claude-code/open` URI, the IDE
WebSocket server) -- RUNBOOK.md section 2, `O8`, has what each one did.
Synthesising the keystroke was NOT among them; it is the fourth route.

WHAT IT DOES, in order, stopping at the first thing that is false:

  1. `~/.claude/settings.json` -> `remoteControlAtStartup` must be true, or the
     new tab comes up with no Remote Control bridge and a phone never sees it.
  2. `%APPDATA%/Code/User/keybindings.json` -> whatever key is bound to
     `claude-vscode.editor.open`. THE BINDING IS THE SOURCE OF TRUTH: this
     script does not hardcode ctrl+alt+c, so rebinding the key moves this
     command with it, and an unbound command is a precondition failure with
     the fix printed beside it.
  3. finds the VS Code window, brings it to the foreground, sends that key.

NEVER SEND `ctrl+shift+escape` OR `ctrl+escape`. Both are taken by Windows
before VS Code sees them -- Task Manager and the START MENU. ⚠️ MEASURED
2026-09-06: `ctrl+escape` (the extension's own `claude-vscode.blur` binding)
opened the Start Menu, the text meant for VS Code went into Windows Search, and
Enter launched a browser. A key a shell synthesises is aimed at the WHOLE
DESKTOP, not at VS Code.

⚠️ WHY THIS SCRIPT TAKES NO PARAMETERS, AND DO NOT ADD ANY. A session name, a
model and an effort level were all attempted on 2026-09-06 and every route was
MEASURED DEAD. Synthetic keystrokes reach VS Code's KEYBINDING DISPATCHER but
not much else: a bound chord works from anywhere, plain text reaches the chat
input once `claude-vscode.focus` has run -- and a slash command still does not
execute, the command palette does not open, and `renameSessionTab` does not
rename. RUNBOOK.md section 2, `O8`, has the table, and `TAB-1` in ISSUES.md is
the code. ⚠️ It also has the FALSE POSITIVE that nearly closed the case early:
`effortLevel` read back as `xhigh` in a session that was never typed into,
because `"ultracode": true` sets it -- a metric that cannot fail (rule 21).

Windows only -- it calls SendInput. Exit 0 on success, 1 on a failed
precondition or a send that did not land.

  python .claude/tools/open_claude_tab.py --check     # resolve everything, send nothing
  python .claude/tools/open_claude_tab.py             # open the tab and leave it
  python .claude/tools/open_claude_tab.py --session-name "pool__ta prune"       --model sonnet --effort xhigh
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import pathlib
import sys
import time
from ctypes import wintypes

sys.stdout.reconfigure(encoding="utf-8")  # CLAUDE.md rule 18

TARGET_COMMAND = "claude-vscode.editor.open"
# ---------------------------------------------------------------------------
# JSONC -- keybindings.json carries // comments and trailing commas
# ---------------------------------------------------------------------------
def strip_jsonc(text: str) -> str:
    """Remove // and /* */ comments and trailing commas, respecting strings."""
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == '"':
            j = i + 1
            while j < n:
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == '"':
                    break
                j += 1
            out.append(text[i : j + 1])
            i = j + 1
        elif text.startswith("//", i):
            nl = text.find("\n", i)
            if nl < 0:
                break
            i = nl
        elif text.startswith("/*", i):
            end = text.find("*/", i + 2)
            i = n if end < 0 else end + 2
        else:
            out.append(c)
            i += 1
    s = "".join(out)
    res: list[str] = []
    i, n = 0, len(s)
    while i < n:
        if s[i] == ",":
            j = i + 1
            while j < n and s[j].isspace():
                j += 1
            if j < n and s[j] in "}]":
                i += 1
                continue
        res.append(s[i])
        i += 1
    return "".join(res)


# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------
def check_remote_control() -> tuple[bool, str]:
    path = pathlib.Path.home() / ".claude" / "settings.json"
    if not path.exists():
        return False, f"{path} does not exist"
    try:
        value = json.loads(path.read_text(encoding="utf-8")).get("remoteControlAtStartup")
    except json.JSONDecodeError as exc:
        return False, f"{path} is not valid JSON: {exc}"
    if value is True:
        return True, "True"
    return False, (
        f"is {value!r}; set remoteControlAtStartup to true in {path} -- the schema "
        "calls it 'Start Remote Control bridge automatically each session'"
    )


def find_keybinding() -> tuple[str | None, str]:
    appdata = os.environ.get("APPDATA")
    if not appdata:
        return None, "APPDATA is not set -- not a Windows user session?"
    path = pathlib.Path(appdata) / "Code" / "User" / "keybindings.json"
    if not path.exists():
        return None, f"{path} does not exist"
    try:
        entries = json.loads(strip_jsonc(path.read_text(encoding="utf-8")))
    except json.JSONDecodeError as exc:
        return None, f"{path} is not valid JSONC: {exc}"
    for entry in entries:
        if isinstance(entry, dict) and entry.get("command") == TARGET_COMMAND:
            if entry.get("key"):
                return entry["key"], str(path)
    return None, (
        f"no key is bound to {TARGET_COMMAND} in {path}; bind one, or fall back "
        "to Ctrl+Shift+P -> 'Claude Code: Open in New Tab'"
    )


# ---------------------------------------------------------------------------
# Win32
# ---------------------------------------------------------------------------
user32 = ctypes.WinDLL("user32", use_last_error=True)
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

INPUT_KEYBOARD = 1
KEYEVENTF_KEYUP = 0x0002
SW_RESTORE = 9
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000

ULONG_PTR = ctypes.c_ulonglong if ctypes.sizeof(ctypes.c_void_p) == 8 else ctypes.c_ulong


class KEYBDINPUT(ctypes.Structure):
    _fields_ = [
        ("wVk", wintypes.WORD),
        ("wScan", wintypes.WORD),
        ("dwFlags", wintypes.DWORD),
        ("time", wintypes.DWORD),
        ("dwExtraInfo", ULONG_PTR),
    ]


class _InputUnion(ctypes.Union):
    # The pad is MOUSEINPUT's size: INPUT must come out at 40 bytes on x64 or
    # SendInput rejects the whole array and returns 0 with no other signal.
    _fields_ = [("ki", KEYBDINPUT), ("_pad", ctypes.c_byte * 32)]


class INPUT(ctypes.Structure):
    _fields_ = [("type", wintypes.DWORD), ("u", _InputUnion)]


user32.SendInput.argtypes = (wintypes.UINT, ctypes.POINTER(INPUT), ctypes.c_int)
user32.SendInput.restype = wintypes.UINT
user32.VkKeyScanW.argtypes = (wintypes.WCHAR,)
user32.VkKeyScanW.restype = ctypes.c_short

# ⚠️ Declare every HWND-taking call. Without argtypes ctypes passes a Python int
# as a 32-bit C int while the callee reads a 64-bit HWND, so the upper half is
# whatever was in the register -- it happens to work for small handles and is a
# silent wrong-handle call for large ones.
for _name, _argtypes, _restype in (
    ("SetForegroundWindow", (wintypes.HWND,), wintypes.BOOL),
    ("BringWindowToTop", (wintypes.HWND,), wintypes.BOOL),
    ("IsIconic", (wintypes.HWND,), wintypes.BOOL),
    ("IsWindowVisible", (wintypes.HWND,), wintypes.BOOL),
    ("ShowWindow", (wintypes.HWND, ctypes.c_int), wintypes.BOOL),
    ("GetForegroundWindow", (), wintypes.HWND),
    ("GetWindowTextLengthW", (wintypes.HWND,), ctypes.c_int),
    ("GetWindowTextW", (wintypes.HWND, wintypes.LPWSTR, ctypes.c_int), ctypes.c_int),
    ("GetWindowThreadProcessId", (wintypes.HWND, ctypes.POINTER(wintypes.DWORD)),
     wintypes.DWORD),
    ("AttachThreadInput", (wintypes.DWORD, wintypes.DWORD, wintypes.BOOL),
     wintypes.BOOL),
):
    _fn = getattr(user32, _name)
    _fn.argtypes = _argtypes
    _fn.restype = _restype

NAMED_KEYS: dict[str, int] = {
    "escape": 0x1B, "esc": 0x1B, "tab": 0x09, "enter": 0x0D, "space": 0x20,
    "backspace": 0x08, "delete": 0x2E, "insert": 0x2D, "home": 0x24,
    "end": 0x23, "pageup": 0x21, "pagedown": 0x22,
    "up": 0x26, "down": 0x28, "left": 0x25, "right": 0x27,
}
NAMED_KEYS.update({f"f{i}": 0x6F + i for i in range(1, 25)})

MODIFIERS: dict[str, int] = {
    "ctrl": 0x11, "control": 0x11, "shift": 0x10, "alt": 0x12,
    "meta": 0x5B, "win": 0x5B, "cmd": 0x5B,
}


def parse_chord(key: str) -> list[tuple[list[int], int]]:
    """'ctrl+alt+c' -> [([VK_CONTROL, VK_MENU], VK_C)]. A space separates chords."""
    chords: list[tuple[list[int], int]] = []
    for part in key.strip().split():
        mods: list[int] = []
        main: int | None = None
        for token in part.split("+"):
            t = token.strip().lower()
            if not t:
                continue
            if t in MODIFIERS:
                mods.append(MODIFIERS[t])
            elif t in NAMED_KEYS:
                main = NAMED_KEYS[t]
            elif len(t) == 1:
                vk = user32.VkKeyScanW(t)
                if vk == -1:
                    raise ValueError(f"cannot map key {t!r} on this keyboard layout")
                main = vk & 0xFF
            else:
                raise ValueError(f"unknown key token {token!r} in {key!r}")
        if main is None:
            raise ValueError(f"no non-modifier key in {part!r}")
        chords.append((mods, main))
    if not chords:
        raise ValueError(f"empty keybinding {key!r}")
    return chords


def _key_event(vk: int, up: bool) -> INPUT:
    event = INPUT(type=INPUT_KEYBOARD)
    event.u.ki = KEYBDINPUT(
        wVk=vk, wScan=0, dwFlags=KEYEVENTF_KEYUP if up else 0, time=0, dwExtraInfo=0
    )
    return event


def _send(events: list[INPUT]) -> None:
    array = (INPUT * len(events))(*events)
    sent = user32.SendInput(len(events), array, ctypes.sizeof(INPUT))
    if sent != len(events):
        raise OSError(
            f"SendInput sent {sent}/{len(events)}: WinError {ctypes.get_last_error()}"
        )


def send_chords(chords: list[tuple[list[int], int]]) -> None:
    for mods, main in chords:
        events = [_key_event(m, False) for m in mods]
        events += [_key_event(main, False), _key_event(main, True)]
        events += [_key_event(m, True) for m in reversed(mods)]
        _send(events)
        time.sleep(0.05)


# ---------------------------------------------------------------------------
# Window
# ---------------------------------------------------------------------------
def _process_name(pid: int) -> str:
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return ""
    try:
        size = wintypes.DWORD(260)
        buf = ctypes.create_unicode_buffer(size.value)
        if kernel32.QueryFullProcessImageNameW(handle, 0, buf, ctypes.byref(size)):
            return pathlib.PurePath(buf.value).name
        return ""
    finally:
        kernel32.CloseHandle(handle)


def find_vscode_windows() -> list[tuple[int, str, int]]:
    """[(hwnd, title, pid)] for visible, titled windows owned by Code.exe."""
    found: list[tuple[int, str, int]] = []
    prototype = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def callback(hwnd, _lparam):
        if not user32.IsWindowVisible(hwnd):
            return True
        length = user32.GetWindowTextLengthW(hwnd)
        if length <= 0:
            return True
        buf = ctypes.create_unicode_buffer(length + 1)
        user32.GetWindowTextW(hwnd, buf, length + 1)
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        if _process_name(pid.value).lower() == "code.exe":
            found.append((hwnd, buf.value, pid.value))
        return True

    user32.EnumWindows(prototype(callback), 0)
    return found


def pick_window(
    windows: list[tuple[int, str, int]], hint: str
) -> tuple[int, str, int]:
    """Prefer a window whose title names this workspace; else the first."""
    if hint:
        for window in windows:
            if hint.lower() in window[1].lower():
                return window
    return windows[0]


def focus(hwnd: int) -> tuple[bool, str]:
    """Foreground the window. Returns (ok, reason) -- reason names the failure.

    Windows blocks SetForegroundWindow from a background process, so attach to
    the current foreground thread's input queue first -- and then VERIFY, because
    SetForegroundWindow can return non-zero having done nothing.

    ⚠️ MEASURED 2026-09-06: on an UNATTENDED desktop (`quser` idle 5+ days, screen
    NOT locked -- the input desktop was still `Default`) `GetForegroundWindow`
    returns NULL, and `SetForegroundWindow` and `SwitchToThisWindow` both fail
    returning 0 with `GetLastError() == 0`. There is no foreground to take and no
    focused window for SendInput to reach, so this route needs an ATTENDED
    desktop. That is why the caller must not send the key when this returns False:
    keys with no focus window go nowhere, silently.
    """
    if user32.IsIconic(hwnd):
        user32.ShowWindow(hwnd, SW_RESTORE)
    foreground = user32.GetForegroundWindow()
    if not foreground:
        # Try anyway -- an absent foreground is one of the documented cases where
        # SetForegroundWindow is allowed to succeed -- but say so if it does not.
        user32.SetForegroundWindow(hwnd)
        time.sleep(0.2)
        if user32.GetForegroundWindow() == hwnd:
            return True, "ok"
        return False, (
            "NO WINDOW HOLDS THE FOREGROUND -- the desktop is unattended. Nothing "
            "has keyboard focus, so the key would go nowhere. Attend the machine "
            "(click the VS Code window) and re-run, or press the key by hand"
        )
    tid_fg = user32.GetWindowThreadProcessId(foreground, None)
    tid_me = kernel32.GetCurrentThreadId()
    attached = bool(user32.AttachThreadInput(tid_me, tid_fg, True)) if tid_fg else False
    try:
        user32.BringWindowToTop(hwnd)
        user32.SetForegroundWindow(hwnd)
    finally:
        if attached:
            user32.AttachThreadInput(tid_me, tid_fg, False)
    for _ in range(30):
        if user32.GetForegroundWindow() == hwnd:
            return True, "ok"
        time.sleep(0.05)
    return False, (
        f"another window kept the foreground (hwnd {user32.GetForegroundWindow()}); "
        "Windows refused the focus steal"
    )


# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--check", action="store_true",
        help="verify the preconditions and the window, then send nothing",
    )
    parser.add_argument(
        "--window-hint", default=pathlib.Path.cwd().name, metavar="STR",
        help="prefer the VS Code window whose title contains STR "
             "(default: this directory's name)",
    )
    args = parser.parse_args()

    if sys.platform != "win32":
        print(f"platform            {sys.platform} -- this script is Windows only")
        return 1

    ok, detail = check_remote_control()
    print(f"remote_control      {detail}")
    if not ok:
        return 1

    key, where = find_keybinding()
    if key is None:
        print(f"keybinding          NOT BOUND -- {where}")
        return 1
    print(f"keybinding          {key}  ({TARGET_COMMAND})")
    try:
        chords = parse_chord(key)
    except ValueError as exc:
        print(f"keybinding          UNPARSEABLE -- {exc}")
        return 1

    windows = find_vscode_windows()
    if not windows:
        print("vscode_window       NONE FOUND -- no visible Code.exe window")
        return 1
    hwnd, title, pid = pick_window(windows, args.window_hint)
    print(f"vscode_window       {title!r} (pid {pid}, {len(windows)} candidate(s))")

    if args.check:
        print("result              CHECK ONLY -- nothing sent")
        return 0

    ok, reason = focus(hwnd)
    if not ok:
        print(f"result              NOT SENT -- {reason}")
        return 1

    time.sleep(0.15)  # the window has focus; let it settle before the chord
    try:
        send_chords(chords)
    except OSError as exc:
        print(f"result              SEND FAILED -- {exc}")
        return 1
    print(f"result              SENT {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
