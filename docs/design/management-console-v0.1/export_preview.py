"""Assemble the review artifact on 192.168.3.88; no project build required."""

from pathlib import Path
import subprocess


def main():
    addresses = subprocess.check_output(["hostname", "-I"], text=True).split()
    if "192.168.3.88" not in addresses:
        raise SystemExit("Export on the designated build server 192.168.3.88")
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
