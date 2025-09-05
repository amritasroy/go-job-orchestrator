import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--name", default="there")
args = parser.parse_args()
print(f"Hello, {args.name}! From Python job.")
