import pytest
from pydantic import ValidationError

from elephant.runtime.mail_filters import MailCondition, MailTrigger


def condition(field, op, value):
    return MailCondition(field=field, op=op, value=value)


def test_mail_combines_required_alternative_and_excluded_conditions():
    trigger = MailTrigger(
        account_id='demo',
        all=(condition('sender_domain', 'equals', 'example.test'), condition('subject', 'contains', '赛事')),
        any=(condition('body', 'contains', '时间'), condition('sender_address', 'equals', 'special@example.test')),
        none=(condition('subject', 'contains', '测试'),),
    )
    mail = {'sender_address': 'User@EXAMPLE.TEST', 'subject': '赛事通知', 'body': '记下时间'}
    assert trigger.folder == 'INBOX'
    assert trigger.matches(**mail)
    assert not trigger.matches(**{**mail, 'sender_address': 'user@example.test.invalid'})
    assert not trigger.matches(**{**mail, 'body': '无关'})
    assert not trigger.matches(**{**mail, 'subject': '赛事测试'})
    assert trigger.matches(**{**mail, 'sender_address': 'SPECIAL@example.test', 'body': '无关'})


@pytest.mark.parametrize('group', ['all', 'any', 'none'])
def test_incomplete_body_is_not_a_conclusive_match(group):
    trigger = MailTrigger(account_id='demo', **{group: (condition('body', 'contains', 'digest'),)})
    assert not trigger.matches(sender_address='user@example.test', subject='digest', body='', body_complete=False)


def test_independent_alternative_can_satisfy_group_with_unknown_body():
    trigger = MailTrigger(account_id='demo', any=(
        condition('body', 'equals', 'digest'), condition('subject', 'equals', 'digest'),
    ))
    assert trigger.matches(sender_address='user@example.test', subject='DIGEST', body='', body_complete=False)


def test_filter_schema_rejects_blank_values_and_unknown_expressions():
    with pytest.raises(ValidationError):
        condition('body', 'contains', ' ')
    with pytest.raises(ValidationError):
        condition('subject', 'regex', 'digest')
    with pytest.raises(ValidationError):
        MailTrigger(account_id='demo', folder='INBOX\nother')


@pytest.mark.parametrize('sender_address', ['', 'bad@@example.test', 'Sender <sender@example.test>'])
def test_unknown_sender_does_not_confirm_sender_exclusion(sender_address):
    trigger = MailTrigger(account_id='demo', none=(condition('sender_domain', 'equals', 'example.test'),))
    assert not trigger.matches(sender_address=sender_address, subject='', body='')
    assert trigger.matches(sender_address=' allowed@elsewhere.test ', subject='', body='')
