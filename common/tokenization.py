"""Shared tokenization rules for both engines."""
import re


def tokenize(line):
    return re.findall(r"[^\W_]+", line.lower())
