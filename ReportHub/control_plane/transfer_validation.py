import base64
import binascii
import ipaddress
import re
from pathlib import PurePosixPath

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, rsa
from django.core.exceptions import ValidationError


def validate_transfer_host(value):
    try:
        ipaddress.ip_address(value)
        return
    except ValueError:
        pass
    labels = value.rstrip(".").split(".")
    if len(value) > 253 or not all(
        re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
        for label in labels
    ):
        raise ValidationError("Enter a hostname or IP address, without a scheme, port or path.")


def validate_host_key_sha256(value):
    if not re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", value):
        raise ValidationError("Enter an SSH SHA256 fingerprint (SHA256: followed by 43 base64 characters).")
    try:
        digest = base64.b64decode(value[7:] + "=", validate=True)
    except binascii.Error:
        raise ValidationError("Enter a valid SSH SHA256 fingerprint.") from None
    if base64.b64encode(digest).decode().rstrip("=") != value[7:]:
        raise ValidationError("Enter a valid SSH SHA256 fingerprint.")


def validate_remote_directory(value):
    if (
        not value.startswith("/")
        or value.startswith("//")
        or ".." in PurePosixPath(value).parts
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValidationError("Enter an absolute SFTP directory without '..' or control characters.")


def load_ssh_private_key(value, passphrase):
    key_data = value.encode()
    password = passphrase.encode() if passphrase else None
    loader = (
        serialization.load_ssh_private_key
        if key_data.startswith(b"-----BEGIN OPENSSH PRIVATE KEY-----")
        else serialization.load_pem_private_key
    )
    try:
        key = loader(key_data, password=password)
    except (ValueError, TypeError, UnsupportedAlgorithm):
        raise ValidationError("Enter a valid SSH private key and its matching passphrase.") from None
    if isinstance(key, (rsa.RSAPrivateKey, ed25519.Ed25519PrivateKey)):
        return key
    if isinstance(key, ec.EllipticCurvePrivateKey) and isinstance(
        key.curve, (ec.SECP256R1, ec.SECP384R1, ec.SECP521R1)
    ):
        return key
    raise ValidationError("Use an RSA, Ed25519 or NIST ECDSA private key.")


def validate_ssh_private_key(value, passphrase):
    load_ssh_private_key(value, passphrase)
