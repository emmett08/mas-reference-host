"""No-network callback used only by the private reference qualification."""
import fcntl
import json
import os
from pathlib import Path
import time

from .contracts import Invocation, Outcome
from .task_service import Workflow


class ReferenceAdapter:
    def __init__(self):
        # Trusted fixture configuration. Application input cannot select paths.
        self.evidence = Path(os.environ['MAS_REFERENCE_EVIDENCE'])

    def record(self, kind, value):
        with self.evidence.open('a', encoding='utf-8') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            stream.write(json.dumps({'kind': kind, 'time_ns': time.monotonic_ns(),
                                     'principal': value['principal'], 'key': value['key']}) + '\n')
            stream.flush()

    def handle(self, context, request):
        def call():
            self.record('start', request.input)
            time.sleep(0.1)
            self.record('end', request.input)
            return Invocation({'key': request.input['key']}, {'provider': 'reference-no-network'})
        return Outcome(context.invoke(call, input=request.input, reservation_microusd=0))


def workflows():
    topology = {'agents': ['client', 'worker'], 'edges': [['client', 'worker'], ['worker', 'client']],
                'application': 'agent-task/1', 'mailbox_capacity': 10,
                'max_messages': 20, 'max_bytes': 100000, 'max_calls': 1, 'wall_seconds': 20}
    return {'reference': Workflow('reference-load/1', topology,
        {'client': ReferenceAdapter(), 'worker': ReferenceAdapter()}, 'client', 'worker', 'reference')}


class MixedReferenceAdapter(ReferenceAdapter):
    def record(self, kind, value):
        with self.evidence.open('a', encoding='utf-8') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            stream.write(json.dumps({'kind': kind, 'time_ns': time.monotonic_ns(),
                'principal': value['principal'], 'key': value['key'], 'class': value['class']}) + '\n')
            stream.flush()

    def handle(self, context, request):
        value = request.input
        if value['class'] == 'paused' and not value.get('resumed'):
            return Outcome(value | {'awaiting_input': True})

        def call():
            self.record('start', value)
            if value['class'] == 'gate':
                # Fixture-owned path, never a path supplied by application input.
                deadline = time.monotonic() + 40
                while not (self.evidence.parent / 'release-gate').exists():
                    if time.monotonic() >= deadline:
                        raise RuntimeError('reference saturation barrier timed out')
                    time.sleep(0.02)
            else:
                time.sleep({'short': 0.05, 'long': 2, 'paused': 0.05, 'history': 0}[value['class']])
            self.record('end', value)
            return Invocation({'key': value['key']}, {'provider': 'reference-no-network'})
        return Outcome(context.invoke(call, input=value, reservation_microusd=0))


def mixed_classify(outcome):
    return 'input_required' if outcome.output.get('awaiting_input') else 'completed'


def mixed_continue(previous, supplied):
    return previous | supplied


def mixed_workflows():
    topology = {'agents': ['client', 'worker'], 'edges': [['client', 'worker'], ['worker', 'client']],
                'application': 'agent-task/1', 'mailbox_capacity': 10,
                'max_messages': 20, 'max_bytes': 100000, 'max_calls': 1, 'wall_seconds': 60}
    return {'mixed': Workflow('reference-mixed/1', topology,
        {'client': MixedReferenceAdapter(), 'worker': MixedReferenceAdapter()},
        'client', 'worker', 'reference', classify=mixed_classify, continue_input=mixed_continue)}
