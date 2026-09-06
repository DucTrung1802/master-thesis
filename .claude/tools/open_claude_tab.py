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
  3. finds the VS Code window and delivers that key to it, by whichever of two
     routes the desktop allows -- see below.

TWO DELIVERY ROUTES, AND THE SECOND IS WHY THIS WORKS UNATTENDED.

  SendInput   the desktop has a foreground window: take it, then synthesise the
              chord. Real input, aimed at the whole desktop.
  PostMessage NOTHING holds the foreground -- `GetForegroundWindow()` returns
              NULL and every documented way of taking it fails. Attach to VS
              Code's own input queue, hold the modifiers in the key state the
              two threads then SHARE, and POST the letter straight to its
              window. No foreground is involved, so none is needed.

⚠️ **THE POSTED ROUTE IS 1-FOR-7, AND THE SCORE IS THE POINT** (measured
2026-09-06, all on one unattended desktop). It opened the tab on the FIRST
attempt -- the title went from `bs_HOSE_FPT.csv - master-thesis - ...` to
`Claude Code - master-thesis - ...` -- and then landed nothing on six further
tries: `ctrl+,`, `ctrl+w` and `ctrl+pagedown`, to the top-level window and to the
`Chrome_RenderWidgetHostHWND` child, before and after `SwitchToThisWindow`. **The
one thing that had changed is which editor held focus**: a text editor when it
worked, a Claude tab -- a WEBVIEW -- for every failure. That is a THEORY (a
posted message reaches Chromium's focused frame, and a webview does not forward
it to VS Code's keybinding dispatcher), not a measurement, and it is written
down as one.

⚠️ **SO THIS ROUTE IS BEST-EFFORT AND THE EXIT CODE SAYS SO.** It costs nothing
to try, it cannot open a duplicate tab (every send is verified before a retry),
and when it cannot be SHOWN to have opened a tab the command fails and tells you
to press the key by hand. What it must never do is report the send as the
outcome (rule 21).

⚠️ **WHY SendInput CANNOT BE MADE TO WORK HERE, MEASURED THE SAME DAY.** Beyond
the five foreground calls above, a window CREATED BY THIS PROCESS -- topmost,
shown, `focus_force`d -- also failed to become the foreground: `GetForegroundWindow()`
stayed 0 throughout. **On this desktop no process can hold the foreground at
all**, which is a fact about the session and not about VS Code, and it is why
"attend the machine" remains the reliable answer.

⚠️ AND THE TITLE IS THE POST-CONDITION, NOT A DECORATION (rule 21). `SENT` is a
metric that cannot fail -- SendInput and PostMessage both succeed into an empty
desktop. What is checked is that the WINDOW TITLE CHANGED. ⚠️ It cannot decide
when the active editor is ALREADY a Claude tab, because opening a second one
does not change the title; that case is reported as unverifiable rather than as
a pass (rule 2).

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

⚠️ THOSE ARE THE ONLY TWO FORMS. A `--session-name / --model / --effort` example
stood here until 2026-09-06, three flags this script has never had, directly
under the paragraph saying it takes none -- `TAB-1` is the measurement that
removed them and the example outlived it.
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
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101

# ⚠️ A modifier's GENERIC virtual key is what `GetKeyState` answers, but an app
# that asks for the specific side gets nothing unless the left-hand one is set
# too -- so both go into the shared key state, and only for the modifiers this
# chord actually names. Setting every side unconditionally is how a `ctrl+k`
# would arrive carrying a phantom ALT.
LEFT_OF = {0x11: 0xA2, 0x10: 0xA0, 0x12: 0xA4}

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


user32.GetKeyboardState.argtypes = (ctypes.c_char_p,)
user32.GetKeyboardState.restype = wintypes.BOOL
user32.SetKeyboardState.argtypes = (ctypes.c_char_p,)
user32.SetKeyboardState.restype = wintypes.BOOL
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
    ("PostMessageW", (wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM),
     wintypes.BOOL),
    ("MapVirtualKeyW", (wintypes.UINT, wintypes.UINT), wintypes.UINT),
    ("GetClassNameW", (wintypes.HWND, wintypes.LPWSTR, ctypes.c_int), ctypes.c_int),
    ("SetActiveWindow", (wintypes.HWND,), wintypes.HWND),
    ("SetFocus", (wintypes.HWND,), wintypes.HWND),
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


def _lparam(vk: int, up: bool) -> int:
    """The lParam a real WM_KEY* carries: repeat count 1, the scan code, and the
    transition/previous-state bits on the way up. Chromium reads the scan code."""
    value = 1 | (user32.MapVirtualKeyW(vk, 0) << 16)
    return value | (1 << 30) | (1 << 31) if up else value


def post_chords(hwnd: int, tid: int, chords: list[tuple[list[int], int]]) -> str:
    """Deliver the chord WITHOUT a foreground window. Returns how it was sent.

    ⚠️ **THE MODIFIERS ARE NOT POSTED, THEY ARE HELD IN THE KEY STATE** -- and that
    is the whole trick. An app reads `ctrl` with `GetKeyState`, which answers from
    the calling THREAD's input state, not from the message; posting a WM_KEYDOWN
    for VK_CONTROL would leave that state untouched and VS Code would see a bare
    `c`. `AttachThreadInput` makes our thread and VS Code's SHARE one input state,
    so `SetKeyboardState` here is what VS Code's `GetKeyState` reads there.

    ⚠️ **AND THE STATE IS PUT BACK.** A left-over ALT in the shared state is a
    keyboard that has gone strange for whoever sits down next, which is a worse
    failure than not opening the tab -- so the restore is in a `finally`.
    """
    attached = bool(user32.AttachThreadInput(kernel32.GetCurrentThreadId(), tid, True))
    try:
        # Not required for the post to land, but it is what makes the window the
        # one its own thread considers focused -- measured as active+focus after
        # this call on a desktop with no foreground at all.
        user32.SetActiveWindow(hwnd)
        user32.SetFocus(hwnd)
        for mods, main in chords:
            state = ctypes.create_string_buffer(256)
            user32.GetKeyboardState(state)
            for vk in mods:
                state[vk] = bytes([0x80])
                if vk in LEFT_OF:
                    state[LEFT_OF[vk]] = bytes([0x80])
            user32.SetKeyboardState(state)
            try:
                user32.PostMessageW(hwnd, WM_KEYDOWN, main, _lparam(main, False))
                time.sleep(0.05)
                user32.PostMessageW(hwnd, WM_KEYUP, main, _lparam(main, True))
                time.sleep(0.05)
            finally:
                clear = ctypes.create_string_buffer(256)
                user32.GetKeyboardState(clear)
                for vk in mods:
                    clear[vk] = bytes([0x00])
                    if vk in LEFT_OF:
                        clear[LEFT_OF[vk]] = bytes([0x00])
                user32.SetKeyboardState(clear)
    finally:
        if attached:
            user32.AttachThreadInput(kernel32.GetCurrentThreadId(), tid, False)
    return "PostMessage" + ("" if attached else " (NOT attached -- modifiers may not read)")


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


def window_title(hwnd: int) -> str:
    length = user32.GetWindowTextLengthW(hwnd)
    buf = ctypes.create_unicode_buffer(length + 1)
    user32.GetWindowTextW(hwnd, buf, length + 1)
    return buf.value


def render_child(hwnd: int) -> int | None:
    """VS Code's `Chrome_RenderWidgetHostHWND`, the second place to post to.

    ⚠️ Only ever tried when a post to the TOP-LEVEL window demonstrably did
    nothing -- and "demonstrably" is the whole condition, see `main`. A blind
    retry is how one command opens two tabs.
    """
    found: list[int] = []
    prototype = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def callback(child, _lparam):
        cls = ctypes.create_unicode_buffer(256)
        user32.GetClassNameW(child, cls, 256)
        if "RenderWidgetHost" in cls.value:
            found.append(child)
        return True

    user32.EnumChildWindows.argtypes = (wintypes.HWND, prototype, wintypes.LPARAM)
    user32.EnumChildWindows(hwnd, prototype(callback), 0)
    return found[0] if found else None


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
    returning 0 with `GetLastError() == 0`. Re-measured the same day with the
    whole documented ladder -- ShowWindow + BringWindowToTop, the
    SPI_SETFOREGROUNDLOCKTIMEOUT=0 + AllowSetForegroundWindow(ASFW_ANY) dance, an
    AttachThreadInput to the TARGET thread, SwitchToThisWindow -- and all five
    left it at 0. **There is no foreground to take on such a desktop and no way
    to make one.**

    ⚠️ THAT IS NO LONGER THE END OF THE COMMAND. SendInput needs a foreground;
    `post_chords` does not, and `main` switches to it here rather than refusing.
    What the caller must still not do is send with SendInput when this is False:
    those keys go nowhere, silently.
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
            "no window holds the foreground -- the desktop is unattended, and "
            "SendInput would go nowhere"
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
    tid = user32.GetWindowThreadProcessId(hwnd, None)
    print(f"vscode_window       {title!r} (pid {pid}, tid {tid}, "
          f"{len(windows)} candidate(s))")

    if args.check:
        print("result              CHECK ONLY -- nothing sent")
        return 0

    # ⚠️ THE TITLE BEFORE, because it is the only post-condition available and it
    # has to be read before anything is sent. `~/.claude/projects/<slug>/` was the
    # other candidate and was measured DEAD 2026-09-06: a new tab writes no
    # session file until somebody types into it.
    before = window_title(hwnd)
    # ⚠️ AND IT CANNOT DECIDE WHEN THE ACTIVE EDITOR IS ALREADY A CLAUDE TAB -- the
    # title of the second one is the title of the first. Recorded as unverifiable
    # rather than counted as a pass (rule 2), and it is also what stops the retry
    # below from opening a duplicate.
    decidable = not before.startswith("Claude Code")

    ok, reason = focus(hwnd)
    if ok:
        time.sleep(0.15)  # the window has focus; let it settle before the chord
        try:
            send_chords(chords)
        except OSError as exc:
            print(f"result              SEND FAILED -- {exc}")
            return 1
        how = "SendInput"
    else:
        # ⚠️ NOT A FALLBACK IN THE SENSE OF "WORSE" -- it is the route that does not
        # touch the foreground at all, and on an unattended desktop it is the only
        # one that works. SendInput keeps first refusal because it is the older
        # measurement and because it is real input; whether the posted route also
        # works with a foreground present is UNMEASURED, so nothing here assumes it.
        print(f"foreground          {reason}")
        how = post_chords(hwnd, tid, chords)

    changed = ""
    for _ in range(24):                       # up to 3 s for the tab to open
        time.sleep(0.125)
        now = window_title(hwnd)
        if now != before:
            changed = now
            break

    if changed:
        print(f"result              OPENED via {how} -- {before!r} -> {changed!r}")
        return 0
    # ⚠️ **THE TWO ROUTES EARN DIFFERENT BENEFIT OF THE DOUBT, AND THE MEASUREMENTS
    # ARE WHY.** SendInput put real input into a window that verifiably held the
    # foreground, so an unchanged title is genuinely ambiguous when the active
    # editor was already a Claude tab. The posted route is 1-for-7 and every one
    # of the six failures was in exactly that state -- so there, "cannot tell" and
    # "did not work" are the same answer, and the honest one is the second.
    if how.startswith("SendInput"):
        if not decidable:
            print(f"result              SENT {key} via {how} -- ⚠️ NOT VERIFIED: the active "
                  f"editor was already a Claude tab, so the title cannot change either way. "
                  f"Real input reached a focused window, so the tab is probably open")
            return 0
        print(f"result              SENT {key} via {how} -- ⚠️ THE TITLE DID NOT CHANGE, so "
              f"nothing says a tab opened. Press {key} by hand")
        return 1
    if not decidable:
        print(f"result              NOT VERIFIED -- posted {key}, and the active editor was "
              f"already a Claude tab so the title cannot say. ⚠️ THAT IS THE STATE IN WHICH "
              f"the posted route was measured NOT to land (1-for-7). Press {key} by hand")
        return 1

    # The post demonstrably did nothing, so a second one cannot duplicate a tab.
    child = render_child(hwnd)
    if child is None:
        print(f"result              NOT OPENED -- posted to {hwnd} and the title did not "
              f"change; no Chrome_RenderWidgetHostHWND to try. Press {key} by hand")
        return 1
    post_chords(child, tid, chords)
    for _ in range(24):
        time.sleep(0.125)
        now = window_title(hwnd)
        if now != before:
            print(f"result              OPENED via PostMessage to the render child "
                  f"{child} -- {before!r} -> {now!r}")
            return 0
    print(f"result              NOT OPENED -- posted to {hwnd} and to {child}, the title "
          f"never changed. Press {key} by hand")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
