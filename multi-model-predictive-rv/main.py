import sys
sys.path.insert(0,'/usr/local/lib/python3.7/site-packages/')
import spot
import buddy
import argparse
from enum import Enum

class CompositionMonitor:
    def __init__(self, op, left, right):
        self.__op = op
        self.__left = left
        self.__right = right
    def get_op(self):
        return self.__op
    def set_op(self, op):
        self.__op = op
    def get_left(self):
        return self.__left
    def set_left(self, left):
        self.__left = left
    def get_right(self):
        return self.__right
    def set_right(self, right):
        self.__right = right
    def __str__(self):
        return str(self.__left) + ' ' + self.__op + ' ' + str(self.__right)
    def __unicode__(self):
        return u(str(self.__left) + ' ' + self.__op + ' ' + str(self.__right))
    def monitorise(self, models):
        if not isinstance(self.__left, CompositionMonitor):
            combination = contextualise(self.__left, models)
            if combination:
                self.__left = PredictiveMonitor(str(self.__left), combination)
            else:
                print('Formula (or sub-formula) and Models do not share any common events')
                exit()
        else:
            self.__left.monitorise(models)
        if not isinstance(self.__right, CompositionMonitor):
            combination = contextualise(self.__right, models)
            if combination:
                self.__right = PredictiveMonitor(str(self.__right), combination)
            else:
                print('Formula (or sub-formula) and Models do not share any common events')
                exit()
        else:
            self.__right.monitorise(models)
    def next(self, event):
        l_res = self.__left.next(event)
        r_res = self.__right.next(event)
        if self.__op == '&':
            if l_res == Verdict.tt and r_res == Verdict.tt:
                return Verdict.tt
            elif l_res == Verdict.ff or r_res == Verdict.ff:
                return Verdict.ff
            else:
                return Verdict.uu
        else:
             if l_res == Verdict.tt or r_res == Verdict.tt:
                 return Verdict.tt
             elif l_res == Verdict.ff and r_res == Verdict.ff:
                 return Verdict.ff
             else:
                 return Verdict.uu
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
        not_phi = spot.formula('!' + formula)
        buchi = phi.translate()
        not_buchi = not_phi.translate()
        self.__product_phi = spot.product(model, buchi)
        self.__product_not_phi = spot.product(model, not_buchi)
        self.__model = model
        self.__last_verdict = Verdict.uu
    def next(self, event_tuple):
        pj_event = project([event_tuple[0]], self.__model)
        if not pj_event:
            return self.__last_verdict
        event = event_tuple[1]
        next = False
        l = 0
        before = self.__product_phi.get_init_state_number()
        for t in self.__product_phi.out(self.__product_phi.get_init_state_number()):
            if (t.cond & event) != buddy.bddfalse and len(spot.bdd_format_formula(self.__product_phi.get_dict(), t.cond)) > l:
                # print(str(t.cond))
                # print(str(event))
                self.__product_phi.set_init_state(t.dst)
                next = True
                l = len(spot.bdd_format_formula(self.__product_phi.get_dict(), t.cond))
        print(str(before) + ' -> ' + str(self.__product_phi.get_init_state_number()))
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
def project(trace, model):
    pj_trace = []
    aps = []
    for ap in model.ap():
        aps.append(str(ap))
    for ev in trace:
        if ev in aps:
            pj_trace.append(ev)
    return pj_trace
def contextualise(property, models):
    def get_aps(f):
        if f.is_literal():
            aps.add(f)
        return False
    models_ap = []
    for m in models:
        models_ap.append((m, m.ap()))
    aps = set()
    property.traverse(get_aps)
    models_of_interest = []
    for (m, m_ap) in models_ap:
        if not aps.isdisjoint(m_ap):
            models_of_interest.append(m)
    combination = None
    for m in models_of_interest:
        if combination is None:
            combination = m
        else:
            combination = parallel(combination, m)
    return combination

composition_monitors = []
def decompose(f):
    if f._is(spot.op_And):
        aux = CompositionMonitor('&', f[0], f[1])
        composition_monitors.append(aux)
        for i in range(2, len(f)):
            aux = CompositionMonitor('&', aux, f[i])
            composition_monitors.append(aux)
        return False
    if f._is(spot.op_Or):
        aux = CompositionMonitor('|', f[0], f[1])
        composition_monitors.append(aux)
        for i in range(2, len(f)):
            aux = CompositionMonitor('|', aux, f[i])
            composition_monitors.append(aux)
        return False
    return True
def extract_root_composition_monitor():
    global composition_monitors
    to_remove = set()
    for f1 in composition_monitors:
        for f2 in composition_monitors:
            if str(f1) == str(f2.get_left()):
                f2.set_left(f1)
                to_remove.add(f1)
            if str(f1) == str(f2.get_right()):
                f2.set_right(f1)
                to_remove.add(f1)
    composition_monitors = [x for x in composition_monitors if x not in to_remove]

# f.traverse(countg)

# def m():
# bdict = spot.make_bdd_dict()
# aut = spot.make_twa_graph(bdict)



def parallel(left, right):
    bdict = left.get_dict()
    if right.get_dict() != bdict:
        raise RuntimeError("automata should share their dictionary")

    result = spot.make_twa_graph(bdict)
    # Copy the atomic propositions of the two input automata
    result.copy_ap_of(left)
    result.copy_ap_of(right)

    sdict = {}
    todo = []
    def dst(ls, rs):
        pair = (ls, rs)
        p = sdict.get(pair)
        if p is None:
            p = result.new_state()
            sdict[pair] = p
            todo.append((ls, rs, p))
        return p

    result.set_init_state(dst(left.get_init_state_number(),
                              right.get_init_state_number()))

    # The acceptance sets of the right automaton will be shifted by this amount
    shift = left.num_sets()
    result.set_acceptance(shift + right.num_sets(),
                          left.get_acceptance() & (right.get_acceptance() << shift))

    while todo:
        lsrc, rsrc, osrc = todo.pop()
        for lt in left.out(lsrc):
            for rt in right.out(rsrc):
                cond = lt.cond
                if cond != buddy.bddfalse:
                    # membership of this transitions to the new acceptance sets
                    acc = lt.acc | (rt.acc << shift)
                    result.new_edge(osrc, dst(lt.dst, rt.src), cond, acc)
                cond = rt.cond
                if cond != buddy.bddfalse:
                    # membership of this transitions to the new acceptance sets
                    acc = lt.acc | (rt.acc << shift)
                    result.new_edge(osrc, dst(lt.src, rt.dst), cond, acc)
                cond = lt.cond & rt.cond
                if cond != buddy.bddfalse:
                    # membership of this transitions to the new acceptance sets
                    acc = lt.acc | (rt.acc << shift)
                    result.new_edge(osrc, dst(lt.dst, rt.dst), cond, acc)
    result.merge_edges()
    return result


# formula = 'Ga | Gb'
# # system = spot.formula('G(a & !b) | G(!a & b)').translate()
# models = []
# models.append(spot.automaton('model0.hoa'))
# models.append(spot.automaton('model1.hoa'))
# models.append(spot.automaton('model2.hoa'))
# system = parallel(models[0], models[1])
# # system = parallel(system, models[2])
# predictive_monitor = PredictiveMonitor(formula, system)
# monitor = spot.translate(formula, 'monitor', 'det')

def main(argv):
    parser = argparse.ArgumentParser(
        description='Python prototype of Multi-Model Predictive Runtime Verification',
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('formula',
        help='LTL formula to verify',
        type=str)
    parser.add_argument('trace',
        help='the trace to analyse',
        nargs='+',
        type=str
    )
    parser.add_argument('--models',
        help='LTL formula to verify',
        nargs='+',
        type=str)
    parser.add_argument('--single', action='store_true')
    parser.add_argument('--multi', action='store_true')
    args = parser.parse_args() # maybe in the future we will need more arguments, for now it's just one

    if not args.models and (args.multi or args.single):
        print('When multi-model predictive is selected, you have to pass the list of models as well (--models)')
        return
    models = []
    if args.models:
        for m in args.models:
            models.append(spot.automaton(m))
    system = None
    for m in models:
        if not system:
            system = m
        else:
            system = parallel(system, m)
    if args.single:
        contextualised_model = contextualise(spot.formula(args.formula), models)
        if contextualised_model:
            monitor = PredictiveMonitor(args.formula, contextualised_model)
        else:
            print('Formula (or sub-formula) and Models do not share any common events')
            return
    elif args.multi:
        spot.formula(args.formula).traverse(decompose)
        extract_root_composition_monitor()
        if not composition_monitors:
            contextualised_model = contextualise(spot.formula(args.formula), models)
            if contextualised_model:
                monitor = PredictiveMonitor(args.formula, contextualised_model)
            else:
                print('Formula (or sub-formula) and Models do not share any common events')
                return
        else:
            composition_monitors[0].monitorise(models)
            monitor = composition_monitors[0]
    else:
        monitor = spot.translate(args.formula, 'monitor', 'det')
    aps = set()
    def get_aps(f):
        if f.is_literal():
            aps.add(f)
        return False
    spot.formula(args.formula).traverse(get_aps)
    for m in models:
        aps.update(m.ap())
    if not system:
        system = spot.formula(args.formula).translate()
    i = 0
    for ev in args.trace:
        print('event#' + str(i) + ': ' + ev)
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
        next = False
        l = 0
        if args.single or args.multi:
            res = monitor.next((ev, event))
            print('res: ' + str(res))
            if res == Verdict.tt or res == Verdict.ff:
                break
        else:
            for t in monitor.out(monitor.get_init_state_number()):
                if (t.cond & event) != buddy.bddfalse and len(spot.bdd_format_formula(monitor.get_dict(), t.cond)) > l:
                    monitor.set_init_state(t.dst)
                    next = True
                    l = len(spot.bdd_format_formula(monitor.get_dict(), t.cond))
            if not next:
                print('res: FALSE')
            else:
                print('res: ?')

if __name__ == '__main__':
    main(sys.argv)

#
#
#
#
#
# # events
# a = system.register_ap('a')
# bdda = buddy.bdd_ithvar(a)
# nbdda = buddy.bdd_nithvar(a)
# b = system.register_ap('b')
# bddb = buddy.bdd_ithvar(b)
# nbddb = buddy.bdd_nithvar(b)
# c = system.register_ap('c')
# bddc = buddy.bdd_ithvar(c)
# nbddc = buddy.bdd_nithvar(c)
# d = system.register_ap('d')
# bddd = buddy.bdd_ithvar(d)
# nbddd = buddy.bdd_nithvar(d)
# e = system.register_ap('e')
# bdde = buddy.bdd_ithvar(e)
# nbdde = buddy.bdd_nithvar(e)
# f = system.register_ap('f')
# bddf = buddy.bdd_ithvar(f)
# nbddf = buddy.bdd_nithvar(f)
# g = system.register_ap('g')
# bddg = buddy.bdd_ithvar(g)
# nbddg = buddy.bdd_nithvar(g)
# h = system.register_ap('h')
# bddh = buddy.bdd_ithvar(h)
# nbddh = buddy.bdd_nithvar(h)
# events = [
#     nbdda & bddb & nbddc & nbddd & nbdde & nbddf & nbddg & nbddh,
#     nbdda & bddb & nbddc & nbddd & nbdde & nbddf & nbddg & nbddh,
#     nbdda & bddb & nbddc & nbddd & nbdde & nbddf & nbddg & nbddh
# ]
# i = 0
# while events:
#     print('event#' + str(i))
#     i = i+1
#     event = events.pop(0)
#     next = False
#     l = 0
#     for t in monitor.out(monitor.get_init_state_number()):
#         if (t.cond & event) != buddy.bddfalse and len(spot.bdd_format_formula(monitor.get_dict(), t.cond)) > l:
#             monitor.set_init_state(t.dst)
#             next = True
#             l = len(spot.bdd_format_formula(monitor.get_dict(), t.cond))
#     if not next:
#         print('standard: FALSE')
#     else:
#         print('standard: ?')
#     res = predictive_monitor.next(event)
#     print('predictive: ' + str(res))
#     if res == Verdict.tt or res == Verdict.ff:
#         break
