#!/usr/bin/env python3
"""
setup_instance.py — Configure a new controller instance from a copied directory.
Run from the root of the copied project directory after duplicating the folder.
"""

import os
import re
import json
import sys
import shutil

CURRENT_NAME    = "ExxonController"
CURRENT_NAS_TAG = "ExxonProject"
CURRENT_PORT    = 52424

# Files where the app name string appears (comments, headers, UI text, etc.)
TEXT_FILES = [
    "app.py",
    "templates/login.html",
    "templates/index.html",
    "core/data_logger.py",
    "core/nas_relay.py",
    "static/js/map.js",
    "static/css/style.css",
]


# ── Port formula ───────────────────────────────────────────────────────────────

def compute_suggested_port(name: str) -> int | None:
    """
    Derive a port from the name by concatenating each letter's A=1..Z=26 position,
    stopping before the result would exceed 65535. Returns None if result < 1024.
    Example: 'ExxonController' → 5,24,24 → 52424
    """
    digits = ""
    for ch in name:
        if not ch.isalpha():
            continue
        num = str(ord(ch.upper()) - 64)
        candidate = digits + num
        if int(candidate) > 65535:
            break
        digits = candidate
    if not digits:
        return None
    port = int(digits)
    return port if port >= 1024 else None


# ── Sibling instance detection ─────────────────────────────────────────────────

def detect_sibling_ports() -> dict[str, int]:
    """Scan sibling directories for other controller instances and their ports."""
    ports: dict[str, int] = {}
    base = os.path.dirname(os.path.abspath("."))
    try:
        for entry in os.listdir(base):
            sibling = os.path.join(base, entry)
            app_path = os.path.join(sibling, "app.py")
            if os.path.abspath(sibling) == os.path.abspath("."):
                continue
            if not os.path.isfile(app_path):
                continue
            with open(app_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            m = re.search(
                r"socketio\.run\(app,\s*host='0\.0\.0\.0',\s*port=(\d+)", content
            )
            if m:
                ports[entry] = int(m.group(1))
    except Exception:
        pass
    return ports


# ── Input helpers ──────────────────────────────────────────────────────────────

def validate_name(name: str) -> bool:
    return bool(re.match(r"^[A-Za-z0-9]+$", name)) and len(name) >= 2


def prompt_name() -> str:
    while True:
        name = input("  New instance name (letters/numbers only, e.g. MixingRig): ").strip()
        if not name:
            print("    Name cannot be empty.")
        elif not validate_name(name):
            print("    Invalid: letters and numbers only — no spaces or special characters.")
        elif name == CURRENT_NAME:
            print(f"    That is already the current name. Enter a different one.")
        else:
            return name


def prompt_port(suggested: int | None, taken: dict[str, int]) -> int:
    default = suggested or 8000

    if suggested:
        print(f"  Suggested port (derived from name formula): {suggested}")
    else:
        print("  Could not derive a valid port from this name (would be < 1024).")
        print(f"  Defaulting to {default}.")

    if taken:
        print("  Ports detected in sibling instances:")
        for dirname, port in taken.items():
            print(f"    {dirname}: {port}")

    while True:
        raw = input(f"  Port number [{default}]: ").strip()
        chosen = default if not raw else None

        if chosen is None:
            try:
                chosen = int(raw)
            except ValueError:
                print("    Please enter a valid integer.")
                continue

        if not (1024 <= chosen <= 65535):
            print("    Must be between 1024 and 65535.")
            continue

        if chosen in taken.values():
            conflict = next(d for d, p in taken.items() if p == chosen)
            yn = input(
                f"    Warning: port {chosen} is already used by '{conflict}'. Use it anyway? [y/N]: "
            ).strip().lower()
            if yn != "y":
                continue

        return chosen


def ask_yn(prompt: str) -> bool:
    return input(f"  {prompt} [y/N]: ").strip().lower() == "y"


# ── File manipulation ──────────────────────────────────────────────────────────

def replace_in_file(path: str, old: str, new: str) -> bool:
    if not os.path.isfile(path):
        return False
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    updated = content.replace(old, new)
    if updated == content:
        return False
    with open(path, "w", encoding="utf-8") as f:
        f.write(updated)
    return True


def update_port_in_app(new_port: int) -> bool:
    path = "app.py"
    if not os.path.isfile(path):
        return False
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    updated = content
    # socketio.run() call
    updated = re.sub(
        r"(socketio\.run\(app,\s*host='0\.0\.0\.0',\s*port=)\d+",
        rf"\g<1>{new_port}",
        updated,
    )
    # startup banner print statement
    updated = re.sub(
        r"(Server: http://0\.0\.0\.0:)\d+",
        rf"\g<1>{new_port}",
        updated,
    )
    # module docstring URL
    updated = re.sub(
        r"(http://<host>:)\d+",
        rf"\g<1>{new_port}",
        updated,
    )
    if updated == content:
        return False
    with open(path, "w", encoding="utf-8") as f:
        f.write(updated)
    return True


def clear_devices_json(clear_alicat: bool, clear_peripherals: bool) -> list[str]:
    path = "config/devices.json"
    cleared = []
    if not os.path.isfile(path):
        return cleared
    with open(path, "r") as f:
        data = json.load(f)
    if clear_alicat:
        data["alicat"] = {}
        cleared.append("flow controllers")
    if clear_peripherals:
        data["peripherals"] = {}
        cleared.append("peripherals")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return cleared


def clear_experiments() -> int:
    exp_dir = "config/experiments"
    if not os.path.isdir(exp_dir):
        return 0
    count = 0
    for fn in os.listdir(exp_dir):
        if fn.endswith(".json"):
            os.remove(os.path.join(exp_dir, fn))
            count += 1
    return count


def clear_solenoid_checklist() -> bool:
    path = "config/solenoid_checklist.json"
    if not os.path.isfile(path):
        return False
    with open(path, "w") as f:
        json.dump([], f)
    return True


def clear_emission_points() -> bool:
    path = "config/emission_points.json"
    if not os.path.isfile(path):
        return False
    with open(path, "w") as f:
        json.dump({"emission_points": {}, "ep_order": []}, f, indent=2)
    return True


def clear_completed_runs() -> int:
    runs_dir = os.path.join("Data", "Experiments")
    if not os.path.isdir(runs_dir):
        return 0
    count = 0
    for entry in os.scandir(runs_dir):
        if entry.is_dir():
            shutil.rmtree(entry.path)
            count += 1
        elif entry.is_file() and entry.name.endswith(".zip"):
            os.remove(entry.path)
            count += 1
    return count


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not os.path.isfile("app.py"):
        print("Error: run setup_instance.py from the project root directory (where app.py lives).")
        sys.exit(1)

    print()
    print("=" * 60)
    print("  Controller Instance Setup")
    print("=" * 60)
    print(f"  Current name : {CURRENT_NAME}")
    print()

    # Step 1 — Name
    print("Step 1 of 4: Instance name")
    new_name = prompt_name()

    # Step 2 — Port
    print("\nStep 2 of 4: Port assignment")
    suggested   = compute_suggested_port(new_name)
    sibling_ports = detect_sibling_ports()
    new_port    = prompt_port(suggested, sibling_ports)

    # Step 3 — Firewall reminder
    print("\nStep 3 of 4: Firewall")
    print(f"  Port {new_port} must be allowed through your firewall before starting.")
    print()
    print("  Windows (run Command Prompt as Administrator):")
    print(f'    netsh advfirewall firewall add rule name="{new_name}" dir=in action=allow protocol=TCP localport={new_port}')
    print()
    print("  Linux / WSL:")
    print(f"    sudo ufw allow {new_port}/tcp")
    print()
    input("  Press Enter when ready to continue...")

    # Step 4 — Config clearing
    print("\nStep 4 of 4: Clear existing configuration")
    print("  The current directory contains devices and experiments from the original instance.")
    clear_alicat   = ask_yn("Clear all flow controller devices?")
    clear_periphs  = ask_yn("Clear all peripherals (Phidgets, etc.)?")
    clear_exps     = ask_yn("Clear all experiments?")
    clear_checklist = ask_yn("Clear the experiment pre-run checklist?")
    clear_eps      = ask_yn("Clear all emission points? (DEFAULT is always preserved)")
    clear_runs     = ask_yn("Clear all completed runs (Data/Experiments/)?")


    # Apply ────────────────────────────────────────────────────────────────────
    print("\nApplying changes...")

    for fpath in TEXT_FILES:
        if replace_in_file(fpath, CURRENT_NAME, new_name):
            print(f"  [name]  {fpath}")

    # NAS path tag (settings.json stores "ExxonProject", not "ExxonController")
    if replace_in_file("config/settings.json", CURRENT_NAS_TAG, f"{new_name}Project"):
        print(f"  [nas]   config/settings.json  ({CURRENT_NAS_TAG} → {new_name}Project)")

    if update_port_in_app(new_port):
        print(f"  [port]  app.py → {new_port}")

    if clear_alicat or clear_periphs:
        cleared = clear_devices_json(clear_alicat, clear_periphs)
        print(f"  [clear] devices.json — {', '.join(cleared)}")

    if clear_exps:
        n = clear_experiments()
        print(f"  [clear] experiments — removed {n} file(s)")

    if clear_checklist:
        if clear_solenoid_checklist():
            print("  [clear] solenoid_checklist.json")

    if clear_eps:
        if clear_emission_points():
            print("  [clear] emission_points.json (DEFAULT preserved — injected at runtime)")

    if clear_runs:
        n = clear_completed_runs()
        print(f"  [clear] completed runs — removed {n} item(s) from Data/Experiments/")

    hb = "config/heartbeat.json"
    if os.path.exists(hb):
        os.remove(hb)
        print("  [clear] heartbeat.json")

    print()
    print("=" * 60)
    print(f"  Done!  '{new_name}' is configured on port {new_port}.")
    print(f"  Start the server with:  python app.py")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
