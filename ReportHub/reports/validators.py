from django.core.exceptions import ValidationError
from django.core.validators import validate_email

def validate_comma_separated_emails(value):
    """
    Validates a comma-separated string of email addresses.
    Ensures every individual email address is syntactically valid.
    """
    if not value:
        return
    
    # Split by comma and strip whitespace
    emails = [email.strip() for email in value.split(',') if email.strip()]
    
    for email in emails:
        try:
            validate_email(email)
        except ValidationError:
            raise ValidationError(f"'{email}' is not a valid email address.")