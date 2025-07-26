"""
Main script for the upload_base_sfmc project.

This entry point discovers and executes each of the ETL scripts defined in
``scripts/``. Each of those scripts contains a ``run()`` function that
connects to the staging database, extracts the appropriate records, writes
them to the ``outputs/`` directory and then uploads them via SFTP to
Marketing Cloud. The logic here simply imports each module by name and
calls its ``run()`` function if present. The list of modules to run is
declared in the ``SCRIPTS`` constant below.

Note:
    The underlying scripts rely on direct credentials and network
    connectivity to the StageBI SQL Server and the Marketing Cloud SFTP
    endpoint. When running in a CI/CD environment such as GitHub Actions,
    ensure that the necessary drivers (e.g. ODBC Driver 17 for SQL Server)
    and the dependencies defined in ``requirements.txt`` are available and
    that any secrets are provided as environment variables or GitHub
    repository secrets.

Usage:
    python main.py

This will sequentially execute all scripts defined in ``SCRIPTS``.
"""

import importlib
import traceback
import datetime


# ------------------------------------------------------------------
# List of module names (without the ``.py`` extension) inside the
# ``scripts`` package. These names should correspond exactly to the
# filenames in the scripts/ directory. When ``main.py`` is invoked
# directly, each of these modules will be imported and, if the module
# exposes a ``run()`` function, that function will be called.
SCRIPTS = [
    "CrmClientes_OdP",
    "CrmClientes_WMB",
    "CrmClientes_UTP",
    "CrmTransacional_WMB",
    "CrmTransacional_OdP",
    "CrmTransacional_UTP",
]


def executar(script_name: str) -> None:
    """Dynamically imports the given script and executes its run() function.

    Args:
        script_name: The name of the script module within the ``scripts``
            package (without the .py extension).

    If the module does not define a run() function, nothing will happen.
    Any exceptions thrown by the script are caught and printed along with
    a traceback.
    """
    try:
        modulo = importlib.import_module(f"scripts.{script_name}")
        if hasattr(modulo, "run"):
            modulo.run()
    except Exception as exc:
        print(f"❌ Erro ao executar {script_name}.py → {exc}")
        traceback.print_exc()


def main() -> None:
    """Iterates over all scripts and executes them sequentially."""
    inicio = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{inicio}] 🚀 Iniciando execução dos scripts")

    for script in SCRIPTS:
        print(f"→ Executando {script}.py")
        executar(script)

    fim = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{fim}] ✅ Execução concluída!")


if __name__ == "__main__":
    main()
