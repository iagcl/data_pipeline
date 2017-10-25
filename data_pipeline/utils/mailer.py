###############################################################################
# Module:    mailer
# Purpose:   Provides email functionality
#
# Notes:
#
###############################################################################

import logging
import smtplib
import data_pipeline.constants.const as const

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def send(from_address, to_addresses, subject, smtp_server,
         plain_text_message, html_text_message=None):
    """Send email to the list of to_addresses, with the given
    message and from_address using the smtp_server
    :param str from_address The address to use in the From
    attribute of emails
    :param list to_addresses A list of email addresses to send email to.
    :param str smtp_server The server to use for sending emails
    :param str plain_text_message The message body of the email in plain text
    :param str html_text_message The message body of the email in html text
    """

    if not to_addresses:
        logger.warn("Recipient addresses are not configured. "
                    "No email will be sent.")
        return

    orig_to_addresses = to_addresses
    to_addresses = []
    for a in orig_to_addresses:
        addresses = a.split(',')
        # Filter out blank addresses
        to_addresses += filter(lambda address: address, addresses)

    maxlen = const.MAX_SUBJECT_LENGTH
    subject = subject[:maxlen] + "..." if len(subject) > maxlen else subject

    mimemsg = MIMEMultipart("alternative")
    mimemsg['Subject'] = subject
    mimemsg['From'] = from_address
    mimemsg['To'] = const.COMMASPACE.join(to_addresses)

    logger.info("""Sending email:
    from: {from_address}
    to: {to_addresses}
    subject: {subject}
    message:\n{message}
    """.format(
        from_address=from_address,
        to_addresses=to_addresses,
        subject=subject,
        message=plain_text_message))

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    plain_mimemsg = MIMEText(plain_text_message, "plain")
    mimemsg.attach(plain_mimemsg)

    if html_text_message is not None:
        html_mimemsg = MIMEText(html_text_message, "html")
        mimemsg.attach(html_mimemsg)

    s = smtplib.SMTP(smtp_server)
    s.sendmail(from_address, to_addresses, mimemsg.as_string())
    s.quit()
