import time

from exceptions import QuakePollException, QuakePollTestException, QuakeWaitTimeoutException


def wait(max_wait_interval=64, max_wait=86400, poll_action=None,
         poll_test=None, exception_message=None, poll_error_tolerance=4):

    assert poll_action and poll_test

    wait_seconds = 1
    total_seconds_waited = 0

    # don't let a process die because of one http failure
    poll_errors = 0

    while True:

        time.sleep(wait_seconds)
        total_seconds_waited += wait_seconds
        # binary exponential backoff
        wait_seconds = min(wait_seconds * 2, max_wait_interval)

        try:
            r = poll_action()
        except Exception as e:
            poll_errors += 1
            if poll_errors > poll_error_tolerance:
                raise QuakePollException, 'Error polling Quake (%s) (%s)' % (poll_errors, str(e))

            continue

        try:
            if poll_test(r):
                return r
        except Exception as e:
            raise QuakePollTestException, 'Error with the poll test lambda function (%s)' % str(e)

        if total_seconds_waited > max_wait:
            raise QuakeWaitTimeoutException, exception_message or 'PyQuake waited too long (%s)' % total_seconds_waited
