import sys
from pathlib import Path


def main() -> None:
    application = Path(__file__).resolve().parents[2] / "data_prep_studio" / "app.py"
    if not application.is_file():
        raise FileNotFoundError(f"Streamlit app not found: {application}")
    from streamlit.web import cli

    sys.argv = ["streamlit", "run", str(application)]
    cli.main()
