"""Durable prefix optimisation agrees with full replay at interruption boundaries."""
from pathlib import Path
import tempfile
import time
import unittest

from mas_host.service_contracts import ServiceError
from mas_host.workflow_evaluator import PlanEvaluator
from mas_host.workflow_store import WorkflowStore


TASK = {'kind': 'task', 'workflow': 'step'}
LIMITS = {'max_steps': 1000, 'max_parallel': 1, 'wall_seconds': 300, 'max_calls': 1000,
          'max_reserved_microusd': 0}


class StoreJournal:
    def __init__(self, root, *, checkpoints=True):
        self.store = WorkflowStore(root)
        self.visits = []
        if not checkpoints:
            self.progress = self.checkpoint = None

    def node(self, *args):
        self.visits.append(args[1])
        return self.store.node(*args)

    def attempt(self, instance, path, workflow, input, deadline):
        return self.store.attempt(instance, path, workflow, input, deadline,
                                  identity='fixture/1', wall_seconds=300)

    def __getattr__(self, name):
        return getattr(self.store, name)


def create(root, input):
    root.mkdir(parents=True, exist_ok=True)
    store = WorkflowStore(root)
    with store.transaction() as db:
        return store.create_instance(db, 'alice', 'request', 'fixture', 'plan/1', input,
                                     wall_seconds=300, maximum=1000)


def advance(journal, instance, node, input):
    ready = []
    result = PlanEvaluator({}, journal).advance(instance, 'root', node, input, LIMITS, time.time() + 300, ready)
    return result, ready


def trace(journal, instance):
    with journal.store.transaction(readonly=True) as db:
        return [tuple(row) for row in db.execute(
            'SELECT path,event,state FROM events WHERE instance=? ORDER BY id', (instance,))]


class WorkflowProgressTests(unittest.TestCase):
    def test_sequence_cursor_survives_restart_and_eliminates_completed_prefix_visits(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            node = {'kind': 'sequence', 'steps': [TASK] * 30}
            instance = create(root, {'value': 0})
            journal = StoreJournal(root)
            for index in range(20):
                _, ready = advance(journal, instance, node, {'value': 0})
                self.assertEqual(ready[0]['path'], f'root/s{index}')
                journal.store.save_attempt(ready[0], 'completed', {'value': index + 1})
            # Death before checkpointing the most recent completed child is
            # harmless: its retained completion is observed once after restart.
            restarted = StoreJournal(root)
            _, ready = advance(restarted, instance, node, {'value': 0})
            self.assertEqual(restarted.visits, ['root', 'root/s19', 'root/s20'])
            self.assertEqual(ready[0]['input'], {'value': 20})
            for _ in range(3):
                restarted.visits.clear()
                _, repeated = advance(restarted, instance, node, {'value': 0})
                self.assertEqual(restarted.visits, ['root', 'root/s20'])
                self.assertEqual(repeated[0]['run_directory'], ready[0]['run_directory'])

    def test_checkpoint_and_full_replay_have_equal_events_results_and_inputs(self):
        controls = [
            ({'kind': 'sequence', 'steps': [TASK] * 5}, {'value': 0}),
            ({'kind': 'loop', 'max_iterations': 5, 'body': TASK,
              'until': {'path': ['value'], 'equals': 3}}, {'value': 0}),
            ({'kind': 'while', 'max_iterations': 5, 'body': TASK,
              'condition': {'path': ['continue'], 'equals': True}}, {'value': 0, 'continue': True}),
            ({'kind': 'reduce', 'items': {'path': ['items'], 'type': 'array'},
              'initial': {'literal': {'value': 0}}, 'max_items': 5,
              'max_input_bytes': 300, 'body': TASK}, {'items': [1, 2, 3, 4, 5]}),
        ]
        for node, input in controls:
            with self.subTest(kind=node['kind']), tempfile.TemporaryDirectory() as folder:
                outcomes = []
                for enabled in (False, True):
                    root = Path(folder) / str(enabled)
                    instance = create(root, input)
                    journal = StoreJournal(root, checkpoints=enabled)
                    inputs = []
                    for _ in range(10):
                        result, ready = advance(journal, instance, node, input)
                        if not ready:
                            break
                        attempt = ready[0]
                        supplied = attempt['input']
                        inputs.append(supplied)
                        value = supplied.get('accumulator', supplied)['value'] + 1
                        journal.store.save_attempt(attempt, 'completed', {'value': value, 'continue': value < 3})
                        journal = StoreJournal(root, checkpoints=enabled)  # Every step restarts.
                    outcomes.append((result, inputs, trace(journal, instance)))
                self.assertEqual(outcomes[0], outcomes[1])

    def test_unknown_child_is_never_skipped_and_incomplete_checkpoint_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            node = {'kind': 'sequence', 'steps': [TASK] * 3}
            instance = create(root, {})
            journal = StoreJournal(root)
            _, ready = advance(journal, instance, node, {})
            with self.assertRaisesRegex(ServiceError, 'workflow progress unverified'):
                journal.checkpoint(instance, 'root', 'sequence', 1, {})
            journal.save_attempt(ready[0], 'completed', {'value': 1})
            _, ready = advance(journal, instance, node, {})
            journal.save_attempt(ready[0], 'unknown', error={'code': 'lost_reply'})
            result, ready = advance(StoreJournal(root), instance, node, {})
            self.assertEqual(result, ('unknown', None))
            self.assertEqual(ready, [])
            with journal.store.transaction(readonly=True) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM attempts').fetchone()[0], 2)
            with self.assertRaisesRegex(ServiceError, 'workflow progress conflict'):
                journal.checkpoint(instance, 'root', 'sequence', 3, {})

    def test_crash_after_final_loop_checkpoint_does_not_allocate_another_iteration(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            node = {'kind': 'loop', 'max_iterations': 2, 'body': TASK,
                    'until': {'path': ['done'], 'equals': True}}
            instance = create(root, {'done': False})
            journal = StoreJournal(root)
            _, ready = advance(journal, instance, node, {'done': False})
            journal.save_attempt(ready[0], 'completed', {'done': True})
            def interrupted(*args):
                journal.store.checkpoint(*args)
                raise RuntimeError('lost parent completion')
            journal.checkpoint = interrupted
            with self.assertRaisesRegex(RuntimeError, 'lost parent completion'):
                advance(journal, instance, node, {'done': False})
            result, ready = advance(StoreJournal(root), instance, node, {'done': False})
            self.assertEqual(result, ('completed', {'done': True}))
            self.assertEqual(ready, [])
            with journal.store.transaction(readonly=True) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM attempts').fetchone()[0], 1)

    def test_unchanged_poll_is_read_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            instance = create(root, {})
            journal = StoreJournal(root)
            advance(journal, instance, TASK, {})
            observations = []
            original = journal.store.transaction
            def transaction(*, readonly=False):
                observations.append(readonly)
                return original(readonly=readonly)
            journal.store.transaction = transaction
            for _ in range(10):
                advance(journal, instance, TASK, {})
            self.assertTrue(observations)
            self.assertTrue(all(observations), observations)
