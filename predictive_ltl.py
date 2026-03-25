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
    def next(self, event_tuple):
        # pj_event = project([event_tuple[0]], self.__model)
        # if not pj_event:
        #     return self.__last_verdict
        event = event_tuple[1]
        next = False
        l = 0
        for t in self.__product_phi.out(self.__product_phi.get_init_state_number()):
            if (t.cond & event) != buddy.bddfalse and len(spot.bdd_format_formula(self.__product_phi.get_dict(), t.cond)) > l:
                self.__product_phi.set_init_state(t.dst)
                next = True
                l = len(spot.bdd_format_formula(self.__product_phi.get_dict(), t.cond))
        if not next:
            self.__last_verdict = Verdict.ff
            return Verdict.ff
        next = False
        l = 0
        for t in self.__product_not_phi.out(self.__product_not_phi.get_init_state_number()):
            if (t.cond & event) != buddy.bddfalse and len(spot.bdd_format_formula(self.__product_not_phi.get_dict(), t.cond)) > l:
                self.__product_not_phi.set_init_state(t.dst)
                next = True
                l = len(spot.bdd_format_formula(self.__product_not_phi.get_dict(), t.cond))
        if not next:
            self.__last_verdict = Verdict.tt
            return Verdict.tt
        if self.__product_phi.is_empty():
            self.__last_verdict = Verdict.ff
            return Verdict.ff
        if self.__product_not_phi.is_empty():
            self.__last_verdict = Verdict.tt
            return Verdict.tt
        self.__last_verdict = Verdict.uu
        return Verdict.uu

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
    system = None

    if args.model:
        model = spot.automaton(args.model)
    else:
        model = spot.formula('true').translate()

    monitor = PredictiveMonitor(args.formula, model)

    generation_time = time.time() - start_time
    aps = set()
    def get_aps(f):
        if f.is_literal():
            aps.add(f)
        return False
    spot.formula(args.formula).traverse(get_aps)
    if args.model:
        aps.update(model.ap())
    system = spot.formula(args.formula).translate()
    i = 0
    start_time = time.time()
    with open(args.trace) as fp:
       ev = fp.readline()
       ev = ev.replace('\n', '')
       while ev:
           i = i+1
           event = buddy.bddtrue
           for ap in aps:
               if str(ap).startswith('!'): continue
               if ev == str(ap):
                   a = system.register_ap(str(ap))
                   bdda = buddy.bdd_ithvar(a)
                   event = event & bdda
               else:
                   a = system.register_ap(str(ap))
                   nbdda = buddy.bdd_nithvar(a)
                   event = event & nbdda
           res = monitor.next((ev, event))
           if res == Verdict.tt:
               res = 'TRUE'
               break
           if res == Verdict.ff:
               res = 'FALSE'
               break
           res = '?'
           ev = fp.readline().replace('\n', '')
       verification_time = time.time() - start_time
       print('RES: ' + str(res) + ';' + str(generation_time) + ';' + str(verification_time))

if __name__ == '__main__':
    main(sys.argv)

