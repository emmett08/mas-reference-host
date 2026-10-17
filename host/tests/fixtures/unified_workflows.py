"""Portable composed bridge fixture with an approval followed by control flow."""
from mas_host import Outcome
from mas_host.task_service import Workflow


class Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


def approval(outcome):
    return 'completed' if outcome.output.get('approved') else 'input_required'


def continue_input(previous, supplied):
    return previous | supplied


def workflows():
    topology = {'agents': ['client', 'worker'], 'edges': [['client', 'worker'], ['worker', 'client']],
                'application': 'agent-task/1', 'mailbox_capacity': 10,
                'max_messages': 20, 'max_bytes': 100000, 'max_calls': 4, 'wall_seconds': 20}
    adapters = {'client': Echo(), 'worker': Echo()}
    echo = Workflow('echo/1', topology, adapters, 'client', 'worker', 'echo')
    review = Workflow('review/1', topology, adapters, 'client', 'worker', 'review',
                      classify=approval, continue_input=continue_input)
    leaf = {'kind': 'task', 'workflow': 'echo'}
    limits = {'max_steps': 40, 'max_parallel': 2, 'wall_seconds': 60,
              'max_calls': 10, 'max_reserved_microusd': 0}
    child = {'format': 'mas-workflow/1', 'name': 'child', 'version': '1', 'limits': limits,
        'root': {'kind': 'parallel', 'branches': {
            'left': {'kind': 'loop', 'body': leaf, 'until': {'path': ['approved'], 'equals': True}, 'max_iterations': 2},
            'right': {'kind': 'choose', 'condition': {'path': ['approved'], 'equals': True}, 'then': leaf, 'else': leaf}}}}
    parent = {'format': 'mas-workflow/1', 'name': 'pipeline', 'version': '1', 'limits': limits,
        'root': {'kind': 'sequence', 'steps': [{'kind': 'task', 'workflow': 'review'},
                                              {'kind': 'subworkflow', 'workflow': 'child'}]}}
    return {'workflows': {'echo': echo, 'review': review}, 'plans': {'pipeline': parent, 'child': child}}
