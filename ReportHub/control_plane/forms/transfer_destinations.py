from django import forms

from control_plane.models import TransferDestination


class SecretTextarea(forms.Textarea):
    def format_value(self, value):
        return None


class TransferDestinationForm(forms.ModelForm):
    password = forms.CharField(
        required=False, max_length=1024, strip=False, widget=forms.PasswordInput,
        help_text="Password authentication only. Leave blank when editing to keep the saved password.",
    )
    private_key = forms.CharField(
        required=False, max_length=32768, strip=False,
        widget=SecretTextarea(attrs={"rows": 5, "spellcheck": "false"}),
        help_text="OpenSSH or PEM RSA, Ed25519 or NIST ECDSA key. Leave blank when editing to keep the saved key.",
    )
    private_key_passphrase = forms.CharField(
        required=False, max_length=1024, strip=False, widget=forms.PasswordInput,
        help_text="For encrypted keys only. Leave blank to keep the saved passphrase.",
    )
    clear_private_key_passphrase = forms.BooleanField(
        required=False, label="Clear the saved key passphrase",
        help_text="Select when replacing an encrypted key with an unencrypted key.",
    )

    class Meta:
        model = TransferDestination
        fields = [
            "name", "host", "port", "username", "authentication", "password",
            "private_key", "private_key_passphrase", "clear_private_key_passphrase",
            "host_key_sha256", "remote_directory", "is_active",
        ]
        widgets = {
            "port": forms.TextInput(attrs={"inputmode": "numeric", "pattern": "[0-9]+"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, field in self.fields.items():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs["class"] = "h-4 w-4 rounded border-slate-700 bg-slate-900 text-indigo-500"
            else:
                field.widget.attrs["class"] = (
                    "w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl "
                    "text-white text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 select-text"
                )
            if name in ("password", "private_key", "private_key_passphrase"):
                field.widget.attrs["autocomplete"] = "new-password"
                self.initial[name] = ""
            if name == "password":
                field.widget.attrs["data-transfer-authentication"] = "password"
            elif name in ("private_key", "private_key_passphrase", "clear_private_key_passphrase"):
                field.widget.attrs["data-transfer-authentication"] = "private_key"

    def clean(self):
        cleaned = super().clean()
        authentication = cleaned.get("authentication")
        if authentication not in TransferDestination.Authentication.values:
            return cleaned
        password_auth = authentication == TransferDestination.Authentication.PASSWORD
        active_secrets = (
            ("password",) if password_auth else ("private_key", "private_key_passphrase")
        )
        if password_auth and cleaned.get("clear_private_key_passphrase"):
            self.add_error("clear_private_key_passphrase", "Select SSH private key authentication to clear a passphrase.")
        if cleaned.get("clear_private_key_passphrase") and cleaned.get("private_key_passphrase"):
            self.add_error("private_key_passphrase", "Enter a new passphrase or clear the saved one, not both.")
        for name in ("password", "private_key", "private_key_passphrase"):
            if name not in cleaned:
                continue
            if name not in active_secrets:
                if cleaned[name]:
                    self.add_error(name, "This credential does not match the selected authentication method.")
                cleaned[name] = ""
            elif name == "private_key_passphrase" and cleaned.get("clear_private_key_passphrase"):
                cleaned[name] = ""
            elif (
                not cleaned[name]
                and self.instance.pk
                and authentication == self.instance.authentication
            ):
                cleaned[name] = getattr(self.instance, name)
        return cleaned
