"""Synthetic privacy guard regressions; run on the designated build server."""
import importlib.util
import unittest
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


if __name__ == "__main__":
    unittest.main()
