import sys
import spot
import buddy
import argparse
from enum import Enum
import time

class Verdict(Enum):
    ff = 0
    tt = 1
    uu = 2
    def __str__(self):
        if self.value == 0:
            return 'False'
        elif self.value == 1:
            return 'True'
        else:
            return '?'
class PredictiveMonitor:
    def __init__(self, formula, model):
        phi = spot.formula(formula)
        not_phi = spot.formula('!(' + formula + ')')
        buchi = phi.translate()
        not_buchi = not_phi.translate()
        self.__product_phi = spot.product(model, buchi)
        self.__product_not_phi = spot.product(model, not_buchi)
        self.__model = model
        self.__last_verdict = Verdict.uu
        self.__phi_states = set([self.__product_phi.get_init_state_number()])
        self.__not_phi_states = set([self.__product_not_phi.get_init_state_number()])
        self.__last_step_info = {}
        self.__model_ap_names = sorted(str(ap) for ap in model.ap())

    def __transition_count(self, automaton):
        count = 0
        for state in range(automaton.num_states()):
            for _ in automaton.out(state):
                count += 1
        return count

    def __advance_states(self, automaton, current_states, event):
        next_states = set()
        for src in current_states:
            for t in automaton.out(src):
                if (t.cond & event) != buddy.bddfalse:
                    next_states.add(t.dst)
        return next_states

    def __set_initial_states(self, automaton, states):
        if len(states) == 1:
            automaton.set_init_state(next(iter(states)))
            return
        automaton.set_univ_init_state(sorted(states))

    def __language_empty_from(self, automaton, states):
        self.__set_initial_states(automaton, states)
        return automaton.is_empty()

    def __compact_states(self, states, limit=8):
        ordered = sorted(states)
        if len(ordered) <= limit:
            return ordered
        return ordered[:limit] + ["...(+{n})".format(n=len(ordered) - limit)]

    def _set_last_step_info(self, info):
        self.__last_step_info = dict(info)

    def get_last_step_info(self):
        return dict(self.__last_step_info)

    def get_static_stats(self):
        return {
            "model_states": self.__model.num_states(),
            "model_transitions": self.__transition_count(self.__model),
            "product_phi_states": self.__product_phi.num_states(),
            "product_phi_transitions": self.__transition_count(self.__product_phi),
            "product_not_phi_states": self.__product_not_phi.num_states(),
            "product_not_phi_transitions": self.__transition_count(self.__product_not_phi),
        }

    def __encode_event_for_automaton(self, automaton, event_name):
        event_bdd = buddy.bddtrue
        for ap_name in self.__model_ap_names:
            variable = automaton.register_ap(ap_name)
            if event_name == ap_name:
                event_bdd = event_bdd & buddy.bdd_ithvar(variable)
            else:
                event_bdd = event_bdd & buddy.bdd_nithvar(variable)
        return event_bdd

    def next(self, event_tuple):
        event_name = event_tuple[0]
        event_phi = self.__encode_event_for_automaton(self.__product_phi, event_name)
        event_not_phi = self.__encode_event_for_automaton(self.__product_not_phi, event_name)
        phi_before = set(self.__phi_states)
        not_phi_before = set(self.__not_phi_states)
        next_phi_states = self.__advance_states(self.__product_phi, self.__phi_states, event_phi)
        if not next_phi_states:
            self._set_last_step_info({
                "reason": "no_matching_transition_in_phi_product",
                "phi_before_count": len(phi_before),
                "phi_after_count": 0,
                "not_phi_before_count": len(not_phi_before),
                "event": event_tuple[0],
                "phi_before_sample": self.__compact_states(phi_before),
            })
            self.__last_verdict = Verdict.ff
            return Verdict.ff

        next_not_phi_states = self.__advance_states(
            self.__product_not_phi,
            self.__not_phi_states,
            event_not_phi,
        )
        if not next_not_phi_states:
            self._set_last_step_info({
                "reason": "no_matching_transition_in_not_phi_product",
                "phi_before_count": len(phi_before),
                "phi_after_count": len(next_phi_states),
                "not_phi_before_count": len(not_phi_before),
                "not_phi_after_count": 0,
                "event": event_tuple[0],
                "phi_after_sample": self.__compact_states(next_phi_states),
                "not_phi_before_sample": self.__compact_states(not_phi_before),
            })
            self.__last_verdict = Verdict.tt
            return Verdict.tt

        self.__phi_states = next_phi_states
        self.__not_phi_states = next_not_phi_states

        if self.__language_empty_from(self.__product_phi, self.__phi_states):
            self._set_last_step_info({
                "reason": "phi_language_empty_after_event",
                "phi_before_count": len(phi_before),
                "phi_after_count": len(self.__phi_states),
                "not_phi_before_count": len(not_phi_before),
                "not_phi_after_count": len(self.__not_phi_states),
                "event": event_tuple[0],
                "phi_after_sample": self.__compact_states(self.__phi_states),
            })
            self.__last_verdict = Verdict.ff
            return Verdict.ff
        if self.__language_empty_from(self.__product_not_phi, self.__not_phi_states):
            self._set_last_step_info({
                "reason": "not_phi_language_empty_after_event",
                "phi_before_count": len(phi_before),
                "phi_after_count": len(self.__phi_states),
                "not_phi_before_count": len(not_phi_before),
                "not_phi_after_count": len(self.__not_phi_states),
                "event": event_tuple[0],
                "not_phi_after_sample": self.__compact_states(self.__not_phi_states),
            })
            self.__last_verdict = Verdict.tt
            return Verdict.tt
        self._set_last_step_info({
            "reason": "undecided",
            "phi_before_count": len(phi_before),
            "phi_after_count": len(self.__phi_states),
            "not_phi_before_count": len(not_phi_before),
            "not_phi_after_count": len(self.__not_phi_states),
            "event": event_tuple[0],
            "phi_after_sample": self.__compact_states(self.__phi_states),
            "not_phi_after_sample": self.__compact_states(self.__not_phi_states),
        })
        self.__last_verdict = Verdict.uu
        return Verdict.uu


def collect_aps(formula, model=None):
    aps = set()

    def get_aps(node):
        if node.is_literal():
            aps.add(node)
        return False

    spot.formula(formula).traverse(get_aps)
    if model is not None:
        aps.update(model.ap())
    return sorted((ap for ap in aps if not str(ap).startswith('!')), key=lambda ap: str(ap))


def encode_event(event_name, aps, system):
    # Deprecated for predictive runtime path.
    event = buddy.bddtrue
    for ap in aps:
        ap_name = str(ap)
        variable = system.register_ap(ap_name)
        if event_name == ap_name:
            event = event & buddy.bdd_ithvar(variable)
        else:
            event = event & buddy.bdd_nithvar(variable)
    return event


def verdict_label(verdict):
    if verdict == Verdict.tt:
        return 'true'
    if verdict == Verdict.ff:
        return 'false'
    return '?'


class PredictiveRuntime:
    def __init__(self, formula, model):
        # Treat the CSP model as a transition system: every infinite run is admissible.
        # Otherwise, model acceptance constraints can incorrectly force early verdicts.
        if not model.get_acceptance().is_t():
            model.set_acceptance(0, spot.acc_code.t())
        self.monitor = PredictiveMonitor(formula, model)

    def step(self, event_name):
        return self.monitor.next((event_name, None))

    def get_last_step_info(self):
        return self.monitor.get_last_step_info()

    def get_static_stats(self):
        return self.monitor.get_static_stats()


def main(argv):
    parser = argparse.ArgumentParser(
        description='Python prototype of Predictive Runtime Verification for LTL',
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('formula',
        help='LTL formula to verify',
        type=str)
    parser.add_argument('trace',
        help='the trace to analyse',
        type=str
    )
    parser.add_argument('--model',
        help='model to use for predictive monitoring (if not specified, the monitor is generated from the formula only)',
        type=str)
    args = parser.parse_args()

    start_time = time.time()

    if args.model:
        model = spot.automaton(args.model)
    else:
        model = spot.formula('true').translate()

    runtime = PredictiveRuntime(args.formula, model)

    generation_time = time.time() - start_time
    i = 0
    start_time = time.time()
    with open(args.trace) as fp:
       ev = fp.readline()
       ev = ev.replace('\n', '')
       while ev:
           i = i+1
           res = runtime.step(ev)
           if res == Verdict.tt:
               res = verdict_label(res)
               break
           if res == Verdict.ff:
               res = verdict_label(res)
               break
           res = verdict_label(res)
           ev = fp.readline().replace('\n', '')
       verification_time = time.time() - start_time
       print('RES: ' + str(res) + ';' + str(generation_time) + ';' + str(verification_time))

if __name__ == '__main__':
    main(sys.argv)
