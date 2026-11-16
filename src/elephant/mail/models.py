"""Validated mailbox connection parameters, without credentials."""
from email.utils import parseaddr
from ipaddress import ip_address
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PRESETS = {
    'icloud': ('imap.mail.me.com', 'smtp.mail.me.com', 587, 'starttls'),
    'qq': ('imap.qq.com', 'smtp.qq.com', 465, 'ssl'),
    '163': ('imap.163.com', 'smtp.163.com', 465, 'ssl'),
}


class MailAccountSpec(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    provider: Literal['icloud', 'qq', '163', 'custom'] = 'custom'
    address: str = Field(min_length=3, max_length=320)
    username: str = Field(default='', max_length=320)
    imap_host: str = Field(default='', max_length=253)
    imap_port: int = Field(default=993, ge=1, le=65535)
    imap_security: Literal['ssl', 'starttls'] = 'ssl'
    smtp_host: str = Field(default='', max_length=253)
    smtp_port: int = Field(default=465, ge=1, le=65535)
    smtp_security: Literal['ssl', 'starttls'] = 'ssl'

    @model_validator(mode='before')
    @classmethod
    def defaults(cls, values):
        if not isinstance(values, dict):
            return values
        values = dict(values)
        if values.get('provider') in PRESETS:
            imap, smtp, port, security = PRESETS[values['provider']]
            for key, value in {'imap_host': imap, 'smtp_host': smtp,
                               'smtp_port': port, 'smtp_security': security}.items():
                values.setdefault(key, value)
        values.setdefault('username', values.get('address', ''))
        return values

    @field_validator('address')
    @classmethod
    def valid_address(cls, value):
        name, address = parseaddr(value, strict=True)
        if name or address != value or address.count('@') != 1 or any(char.isspace() for char in address):
            raise ValueError('a single email address is required')
        local, domain = address.rsplit('@', 1)
        if not local or not domain or any(ord(char) < 32 or ord(char) == 127 for char in address):
            raise ValueError('invalid email address')
        return address

    @field_validator('username', 'imap_host', 'smtp_host')
    @classmethod
    def valid_connection_value(cls, value, info):
        if not value.strip() or any(ord(char) < 33 or ord(char) == 127 for char in value):
            raise ValueError('invalid mail connection parameter')
        if info.field_name.endswith('_host'):
            try:
                ip_address(value)
            except ValueError:
                if any(char in value for char in '/@\\:#?[]'):
                    raise ValueError('mail server must be a hostname or IP address') from None
        return value

    @model_validator(mode='after')
    def required_hosts(self):
        if not self.imap_host or not self.smtp_host or not self.username:
            raise ValueError('mail username and both server hostnames are required')
        return self
