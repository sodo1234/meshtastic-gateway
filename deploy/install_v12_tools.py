"""Install the v12 helper tools from this checkout into another project directory.

Usage: python deploy/install_v12_tools.py TARGET
No credentials are copied or written. TYPESAFE_API_KEY is read from the environment at runtime.
"""
import argparse
import shutil
from pathlib import Path

FILES = (
    "tools/jev_decide.py",
    "tools/dispatch.py",
    "tools/jev_hook.py",
    "tools/agent_status/server.py",
    "tools/agent_status/start.ps1",
    "tools/agent_status/README.md",
)


def install(source, target):
    copied = []
    for relative in FILES:
        src = source / relative
        dst = target / relative
        if not src.is_file():
            raise FileNotFoundError(src)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied.append(relative)
    return copied


def main():
    parser = argparse.ArgumentParser(description="Install Meshtastic Gateway v12 tools (without secrets).")
    parser.add_argument("target", type=Path, help="destination project directory")
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    for path in install(source, args.target.resolve()):
        print(path)
    print("Installed v12 tools. Set TYPESAFE_API_KEY in the runtime environment if JEV is used.")


if __name__ == "__main__":
    main()
