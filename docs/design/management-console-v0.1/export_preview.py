"""Assemble the review artifact on titan-build; no project build required."""

from pathlib import Path
import subprocess


def main():
    addresses = subprocess.check_output(["hostname", "-I"], text=True).split()
    if __import__("os").environ["TITAN_BUILD_ADDRESS"] not in addresses:
        raise SystemExit("Export on the designated build server titan-build")
    root = Path(__file__).resolve().parent
    page = (root / "index.html").read_text()
    style = (root / "styles.css").read_text()
    script = (root / "app.js").read_text()
    page = page.replace(
        '<link rel="stylesheet" href="./styles.css" />',
        f"<style>\n{style}\n</style>",
    ).replace('<script src="./app.js" defer></script>', "")
    page = page.replace("</body>", f"<script>\n{script}\n</script>\n</body>")
    (root / "preview.html").write_text(page)
    print("Exported preview.html from index.html, styles.css and app.js")


if __name__ == "__main__":
    main()
