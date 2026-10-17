"""Remote I/O releases repository locks without releasing effect ownership."""
from contextlib import closing
import multiprocessing
from pathlib import Path
import queue
import sqlite3
import tempfile
import threading
import unittest

from mas_host.bridges.remote import RemoteSnapshot, RemoteTaskService, RemoteTaskStore
from mas_host.journal import Journal
from mas_host.operations import DeploymentLedger, OperationsError, backup_state
from mas_host.provider_outcomes import ProviderOutcomeFailure
from mas_host.service_contracts import ServiceError
from test_remote_tasks import AdmittedContext, RemoteFixture


class RunContext(AdmittedContext):
    def __init__(self, root, run_id):
        self.root, self.run_id = root, run_id
        self.host = root / run_id
        self.host.mkdir(mode=0o700)
        self.ledger = DeploymentLedger(root)
        self.ledger.admit_run(run_id, self.host)
        self.journal = Journal(self.host, {
            'run_id': run_id, 'workflow_version': 'test', 'topology': {}, 'max_depth': 8})
        self.index = 0

    def invoke(self, callback, *, input=None, reservation_microusd=0):
        self.index += 1
        self.invocation_id = 'call-' + str(self.index)
        self.ledger.admit_call(self.run_id, self.invocation_id, reservation_microusd)
        with self.journal.db:
            self.journal.db.execute('INSERT INTO invocations VALUES(?,?,?,?,NULL,NULL,?)',
                                   (self.invocation_id, 'step-' + str(self.index),
                                    self.actor, 'executing', reservation_microusd))
        return callback().output


class WaitingPeer(RemoteFixture):
    def __init__(self, action):
        super().__init__()
        self.wait_action = action
        self.entered, self.release = threading.Event(), threading.Event()

    def reply(self, action):
        if action == self.wait_action:
            self.entered.set()
            if not self.release.wait(10):
                raise RuntimeError('test peer was not released')
        return super().reply(action)


def abandon_submission(root, ready):
    """Die after durable intent, before a first remote receipt is returned."""
    root = Path(root)
    context = RunContext(root, 'run-killed')
    peer = RemoteFixture()
    def send(_payload):
        ready.send('dispatched')
        # Parent kills this process while it owns the effect.
        ready.recv()
        return RemoteSnapshot('never-received', 'running')
    peer.submit = send
    remote = RemoteTaskService(RemoteTaskStore(root), peer, namespace='owner')
    remote.submit('job', {}, context=context, host_run_dir=context.host,
                  reservation_microusd=0)


class RemoteConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'state'
        DeploymentLedger(self.root)
        self.context = RunContext(self.root, 'run-main')
        self.addCleanup(self.context.journal.close)
        self.store = RemoteTaskStore(self.root)
        self.options = {'context': self.context, 'host_run_dir': self.context.host,
                        'reservation_microusd': 0}

    def start_operation(self, peer, action):
        remote = RemoteTaskService(self.store, peer, namespace='owner')
        results = queue.Queue()
        def execute():
            context = RunContext(self.root, 'run-thread')
            try:
                result = action(remote, {'context': context,
                    'host_run_dir': context.host, 'reservation_microusd': 0})
                results.put(result)
            except BaseException as error:
                results.put(error)
            finally:
                context.journal.close()
        thread = threading.Thread(target=execute)
        thread.start()
        def cleanup():
            peer.release.set()
            thread.join(10)
        self.addCleanup(cleanup)
        self.assertTrue(peer.entered.wait(5), 'peer did not enter its network call')
        return remote, thread, results

    def finish_operation(self, peer, thread, results):
        peer.release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        result = results.get_nowait()
        if isinstance(result, BaseException):
            raise result
        return result

    def test_slow_submission_allows_receipt_reads_and_unrelated_lifecycle(self):
        peer = WaitingPeer('submit')
        slow, thread, results = self.start_operation(peer,
            lambda service, opts: service.submit('slow', {}, **opts))
        self.assertEqual(slow.status('slow')['pending_action'], 'submit')
        self.assertEqual(slow.inspect('slow')['receipt']['state'], 'unknown')

        fast_peer = RemoteFixture()
        fast_peer.state = 'input_required'
        fast = RemoteTaskService(self.store, fast_peer, namespace='another-owner')
        fast.submit('fast', {}, **self.options)
        fast.update('fast', request_key='answer', payload={'answer': 7}, **self.options)
        fast.cancel('fast', **self.options)
        self.assertEqual(fast_peer.calls, ['submit', 'update', 'cancel'])
        self.assertTrue(thread.is_alive(), 'unrelated work waited for slow peer')
        with self.assertRaises(OperationsError):
            backup_state(self.root, Path(self.temp.name) / 'backup')
        self.assertEqual(self.finish_operation(peer, thread, results)['state'], 'running')

    def test_same_task_dispatch_and_resolution_cannot_race_live_observation(self):
        peer = WaitingPeer('poll')
        original = RemoteTaskService(self.store, peer, namespace='owner')
        original.submit('job', {}, **self.options)
        service, thread, results = self.start_operation(peer,
            lambda remote, opts: remote.poll('job', **opts))
        operation = service.inspect('job')['operations'][0]
        self.assertEqual(operation['state'], 'unknown')
        with self.assertRaises(ProviderOutcomeFailure) as blocked:
            service.cancel('job', **self.options)
        self.assertEqual(blocked.exception.outcome['certainty'], 'not_sent')
        self.assertEqual(blocked.exception.outcome['category'], 'operation_busy')
        with self.assertRaisesRegex(ServiceError, 'remote task busy'):
            service.resolve_operation('job', operation['operation_key'],
                decision='observation_unavailable', evidence={'reason': 'still in flight'})
        self.assertEqual(peer.calls, ['submit'])
        self.finish_operation(peer, thread, results)
        self.assertEqual(service.inspect('job')['operations'][0]['state'], 'completed')
        self.assertEqual(peer.calls, ['submit', 'poll'])

    def test_process_death_releases_ownership_without_authorising_resubmission(self):
        parent, child = multiprocessing.Pipe()
        self.addCleanup(parent.close)
        process = multiprocessing.get_context('spawn').Process(
            target=abandon_submission, args=(str(self.root), child))
        process.start()
        child.close()
        def cleanup():
            if process.is_alive():
                process.kill()
            process.join(5)
        self.addCleanup(cleanup)
        self.assertTrue(parent.poll(5))
        self.assertEqual(parent.recv(), 'dispatched')
        process.kill()
        process.join(5)
        self.assertFalse(process.is_alive())
        peer = RemoteFixture()
        service = RemoteTaskService(self.store, peer, namespace='owner')
        self.assertEqual(service.status('job')['state'], 'unknown')
        with self.assertRaisesRegex(ServiceError, 'remote outcome unknown'):
            service.submit('job', {}, **self.options)
        self.assertEqual(peer.calls, [])

    def test_received_submission_cannot_overwrite_changed_invocation_identity(self):
        peer = RemoteFixture()
        def changed_intent(_payload):
            with closing(sqlite3.connect(self.store.path)) as db, db:
                db.execute("UPDATE remote_tasks SET call_id='different-call'")
            return RemoteSnapshot('peer-task', 'running')
        peer.submit = changed_intent
        service = RemoteTaskService(self.store, peer, namespace='owner')
        with self.assertRaisesRegex(ServiceError, 'remote operation changed'):
            service.submit('job', {}, **self.options)
        result = service.inspect('job')
        self.assertEqual(result['receipt']['state'], 'unknown')
        self.assertIsNone(result['receipt']['remote_id'])
        self.assertEqual([row['event'] for row in result['audit']], ['submit_intent'])

    def test_received_lifecycle_cannot_overwrite_replaced_operation_identity(self):
        peer = RemoteFixture()
        service = RemoteTaskService(self.store, peer, namespace='owner')
        service.submit('job', {}, **self.options)
        def changed_operation(remote_id):
            with closing(sqlite3.connect(self.store.path)) as db, db:
                db.execute("UPDATE operations SET call_id='different-call'")
            return RemoteSnapshot(remote_id, 'completed', {'value': 8})
        peer.poll = changed_operation
        with self.assertRaisesRegex(ServiceError, 'remote operation changed'):
            service.poll('job', **self.options)
        result = service.inspect('job')
        self.assertEqual(result['receipt']['state'], 'running')
        self.assertEqual(result['operations'][0]['state'], 'unknown')
        self.assertEqual([row['event'] for row in result['audit']],
                         ['submit_intent', 'submit_received', 'poll_intent'])

