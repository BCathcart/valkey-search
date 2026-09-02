import hashlib
import os

_DIR = os.path.dirname(os.path.abspath(__file__))

GENERATOR_FILE = "generate_divergence.py"
ANSWER_FILE = "divergence-answers.pickle.gz"

GENERATOR_PATH = os.path.join(_DIR, GENERATOR_FILE)
ANSWER_PATH = os.path.join(_DIR, ANSWER_FILE)


def compute_generator_hash():
    """SHA256 of the generator source.

    Stored in the answer file so the replay test can tell when the pickle is
    stale relative to the commands that produced it. Scoped to this one file on
    purpose: unlike compatibility/, nothing else here feeds the answers, and a
    directory-wide hash would couple this pickle to unrelated edits.
    """
    with open(GENERATOR_PATH, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()
