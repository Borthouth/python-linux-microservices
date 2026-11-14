import base64
from contextlib import contextmanager
import errno
import hashlib
import hmac
import io

import paramiko
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from django.core.exceptions import ValidationError

from control_plane.transfer_validation import load_ssh_private_key

class DeliveryError(Exception):
    """A delivery error safe to display without exposing credentials."""


class PinnedHostKeyPolicy(paramiko.MissingHostKeyPolicy):
    def __init__(self, fingerprint):
        self.fingerprint = fingerprint

    def missing_host_key(self, client, hostname, key):
        actual = "SHA256:" + base64.b64encode(
            hashlib.sha256(key.asbytes()).digest()
        ).decode().rstrip("=")
        if not hmac.compare_digest(actual, self.fingerprint):
            raise DeliveryError("Server host-key fingerprint mismatch. No file was uploaded.")


def _private_key(destination):
    key = load_ssh_private_key(destination.private_key, destination.private_key_passphrase)
    openssh = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH,
        serialization.NoEncryption(),
    ).decode()
    key_type = (
        paramiko.Ed25519Key if isinstance(key, ed25519.Ed25519PrivateKey)
        else paramiko.RSAKey if isinstance(key, rsa.RSAPrivateKey)
        else paramiko.ECDSAKey
    )
    return key_type.from_private_key(io.StringIO(openssh))


@contextmanager
def sftp_connection(destination):
    if destination.protocol != "sftp":
        raise DeliveryError("Only SFTP delivery is supported.")
    try:
        destination.full_clean(validate_unique=False, validate_constraints=False)
    except ValidationError:
        raise DeliveryError("The destination configuration is invalid. Review its settings.") from None
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(PinnedHostKeyPolicy(destination.host_key_sha256))
    try:
        password_auth = destination.authentication == destination.Authentication.PASSWORD
        client.connect(
            hostname=destination.host, port=destination.port, username=destination.username,
            password=destination.password if password_auth else None,
            pkey=None if password_auth else _private_key(destination),
            look_for_keys=False, allow_agent=False,
            timeout=10, banner_timeout=10, auth_timeout=10, channel_timeout=30,
        )
        with client.open_sftp() as sftp:
            sftp.get_channel().settimeout(30)
            yield sftp
    finally:
        client.close()


def stream_digest(stream):
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: stream.read(65536), b""):
        digest.update(chunk)
        size += len(chunk)
    return size, digest.digest()


def publish_file(sftp, source, remote_path):
    size, digest = stream_digest(source)
    source.seek(0)
    try:
        existing = sftp.stat(remote_path)
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise
    else:
        if existing.st_size == size:
            with sftp.open(remote_path, "rb") as remote:
                if stream_digest(remote) == (size, digest):
                    return
        raise DeliveryError("The remote filename already exists with different contents; it was not overwritten.")
    staging_path = remote_path + ".part"
    sftp.putfo(source, staging_path, file_size=size, confirm=True)
    # Standard SFTP rename must not replace an existing final file.
    sftp.rename(staging_path, remote_path)
