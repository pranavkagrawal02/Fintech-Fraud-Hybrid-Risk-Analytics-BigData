#!/bin/bash
# STEP 0 - run first in the lab. Screenshot the output (Figure 3 in the report).
cd "$(dirname "$0")"
echo "================ HOST ================"; hostname; date; whoami
echo; echo "================ JAVA PROCESSES (jps) - which daemons run on this node ================"
jps 2>/dev/null || echo "jps not found (is JAVA_HOME set?)"
echo; echo "================ PYTHON ================"; python3 --version
python3 - <<'PY'
import importlib
for m in ["pymongo", "pandas", "numpy", "matplotlib", "seaborn", "plotly", "streamlit", "pyspark"]:
    try:
        mod = importlib.import_module(m); print(f"  OK       {m:<11s} {getattr(mod, '__version__', '')}")
    except Exception:
        print(f"  MISSING  {m}")
PY
echo; echo "================ MONGODB ================"
if command -v mongosh >/dev/null; then MSH=mongosh; elif command -v mongo >/dev/null; then MSH=mongo; fi
if [ -n "$MSH" ]; then $MSH --quiet --eval 'print("MongoDB version: " + db.version()); print("Replica set / topology: " + JSON.stringify(db.hello ? db.hello().setName : "n/a"))' 2>&1 | head -5
else echo "mongosh/mongo shell not found in PATH"; fi
echo; echo "================ SPARK ================"
spark-submit --version 2>&1 | grep -E "version|Scala|Using Spark" | head -4
echo; echo "================ HDFS / YARN (optional) ================"
hdfs dfsadmin -report 2>/dev/null | head -8 || echo "hdfs not available"
yarn node -list 2>/dev/null | head -8
echo; echo "================ DISK ================"; df -h . | tail -2
echo; echo "NOTE: if a library is MISSING run: bash 01_setup_python_env.sh"
