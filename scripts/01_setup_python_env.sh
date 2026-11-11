#!/bin/bash
# STEP 1 - install the few Python libraries that are missing (user-level, no sudo).
# Needs internet from the lab machine. If there is none, install the wheels on your laptop,
# or skip dashboards and use only what is already installed (see docs/TROUBLESHOOTING).
cd "$(dirname "$0")/.."
pip3 install --user -r requirements.txt || echo "pip failed - see docs/TROUBLESHOOTING.md (section: no internet)"
