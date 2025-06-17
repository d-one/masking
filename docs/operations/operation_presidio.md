# Operation `presidio`

Uses Presidio to detect sensitive ensitive entities in text (e.g., names, phone numbers, emails) and anonymizes them using a predefined strategy.


### Behavior
- Uses Presidio to detect PII entities in a string.
- Applies an anonymization operator (e.g., replace, hash, redact) to mask them.
- Can optionally take a list of entity types to target (e.g., ["PHONE_NUMBER", "EMAIL_ADDRESS"]).
