"""Synthetic privacy guard regressions; run on the designated build server."""
import importlib.util
import unittest
import json
import subprocess
import tempfile
from pathlib import Path

spec = importlib.util.spec_from_file_location("privacy_guard", Path(__file__).with_name("check_repository_privacy.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class PrivacyGuardTests(unittest.TestCase):
    def kinds(self, text):
        return {x["kind"] for x in guard.inspect_text("example.md", text)}

    def test_personal_path(self):
        self.assertIn("personal-directory", self.kinds("/home/" + "alice/project"))

    def test_infrastructure_address(self):
        self.assertIn("infrastructure-address", self.kinds(".".join(["8"] * 4)))

    def test_example_network_and_loopback(self):
        self.assertFalse(self.kinds("198.51.100.10 127.0.0.1 0.0.0.0"))

    def test_access_token(self):
        self.assertIn("access-token", self.kinds("gh" + "p_" + "A" * 36))

    def test_private_key(self):
        self.assertIn("private-key", self.kinds("-----BEGIN " + "PRIVATE KEY-----"))

    def test_public_contract(self):
        self.assertFalse(self.kinds(next(iter(guard.PUBLIC_ADDRESSES))))

    def test_unreviewed_wallet(self):
        self.assertIn("unreviewed-wallet", self.kinds("0x" + "123456789a" * 4))

    def test_private_evidence(self):
        self.assertEqual(guard.inspect_content("docs/verification/day/result.json", b"{}")[0]["kind"], "private-evidence-export")

    def test_public_summary(self):
        self.assertFalse(guard.inspect_content("docs/verification/day/README.md", b"Synthetic verification summary"))

    def test_no_value_echo(self):
        private = "/Users/" + "alice/private"
        self.assertNotIn(private, str(guard.inspect_text("example.md", private)))

    def test_private_tooling_ref_and_explicit_outgoing_object(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args, data=None):
                return subprocess.check_output(["git", *args], cwd=root, input=data, stderr=subprocess.DEVNULL)
            git("init", "-q")
            (root / "README.md").write_text("Public summary")
            git("add", "README.md")
            git("-c", "user.name=fixture", "-c", "user.email=fixture@users.noreply.github.com", "-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture")
            private = ("/home/" + "alice/private").encode()
            sha = git("hash-object", "-w", "--stdin", data=private).decode().strip()
            git("update-ref", "refs/private/audit", sha)
            command = ["python3", str(Path(guard.__file__).resolve()), "--root", str(root), "--history"]
            normal = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(normal.returncode, 0, normal.stdout + normal.stderr)
            outgoing = subprocess.run(command + ["--revision", sha], capture_output=True, text=True)
            self.assertEqual(outgoing.returncode, 1)
            self.assertTrue(json.loads(outgoing.stdout)["findings"])

    def test_a_published_commit_under_review_is_reported_but_not_refused(self):
        # A push names the commits it adds. One of them may already be published (the outgoing end
        # of a repeated push, an already merged branch): its identity is reported so the migration
        # keeps a list, and its blobs are reviewed like any other, but it does not fail a push for
        # something that is already public.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args, data=None):
                return subprocess.check_output(["git", *args], cwd=root, input=data, stderr=subprocess.DEVNULL)
            git("init", "-q")
            (root / "README.md").write_text("Public summary")
            git("add", "README.md")
            commit = ["-c", "user.name=fixture", "-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture"]
            git("-c", "user.email=" + "author@example.com", *commit)
            published = git("rev-parse", "HEAD").decode().strip()
            command = ["python3", str(Path(guard.__file__).resolve()), "--root", str(root), "--history"]
            reported = json.loads(subprocess.run(command + ["--revision", published], capture_output=True, text=True).stdout)
            self.assertEqual(reported["reviewed_commits_with_published_identity"], [published])
            self.assertNotIn("author@example.com", json.dumps(reported))

    def test_a_revision_may_name_several_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.DEVNULL)
            git("init", "-q")
            (root / "README.md").write_text("Public summary")
            git("add", "README.md")
            git("-c", "user.name=fixture", "-c", "user.email=fixture@users.noreply.github.com",
                "-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture")
            head = git("rev-parse", "HEAD").decode().strip()
            command = ["python3", str(Path(guard.__file__).resolve()), "--root", str(root), "--history"]
            outcome = subprocess.run(command + ["--revision", head + " --not --branches --tags --remotes"],
                                     capture_output=True, text=True)
            self.assertEqual(outcome.returncode, 0, outcome.stdout + outcome.stderr)


if __name__ == "__main__":
    unittest.main()
