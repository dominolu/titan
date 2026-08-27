import argparse

from .bundle import load_bundle
from .render import render_html, render_quantstats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle")
    parser.add_argument("--output", required=True)
    parser.add_argument("--renderer", choices=("native", "quantstats"), default="native")
    args = parser.parse_args()
    bundle = load_bundle(args.bundle)
    if args.renderer == "quantstats":
        render_quantstats(bundle, args.output)
    else:
        render_html(bundle, args.output)


if __name__ == "__main__":
    main()
