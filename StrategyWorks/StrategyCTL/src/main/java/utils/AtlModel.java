package utils;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.checkerframework.checker.units.qual.A;

import java.util.*;
import java.util.stream.Collectors;

public class AtlModel extends JsonObject implements Cloneable {

	@SerializedName("states")
	@Expose
	private List<State> states = null;
	@SerializedName("agents")
	@Expose
	private List<Agent> agents = new ArrayList<>();
	@SerializedName("transitions")
	@Expose
	private List<Transition> transitions = new ArrayList<>();
	@SerializedName("group")
	@Expose
	private Group group;
	@SerializedName("formula")
	@Expose
	private Formula formula;
	
	private transient Map<String, State> stateMap;
	
	private transient Map<String, Agent> agentMap;
	
	private transient Map<String, List<Transition>> transitionMap;

	private transient MultiKeyMap<String, List<List<AgentAction>>> agentActionsByStates;

	public List<? extends State> getStates() {
		return states;
	}

	public void setStates(List<State> states) {
		this.states = states;
	}

	public List<Agent> getAgents() {
		return agents;
	}

	public void setAgents(List<Agent> agents) {
		this.agents = agents;
	}

	public List<Transition> getTransitions() {
		return transitions;
	}

	public void setTransitions(List<Transition> transitions) {
		this.transitions = transitions;
	}

	public Group getGroup() {
		return group;
	}

	public void setGroup(Group group) {
		this.group = group;
	}

	public Formula getFormula() {
		return formula;
	}

	public void setFormula(Formula formula) {
		this.formula = formula;
	}
	
	public Map<String, State> getStateMap() {
		if (stateMap == null) {
			stateMap = new HashMap<>();
			for (State state : getStates()) {
				stateMap.put(state.getName(), state);
			}
		}
		return stateMap;
	}

	public boolean hasState(String stateName) {
		return getStateMap().containsKey(stateName);
	}

	public State getState(String stateName) {
		return getStateMap().get(stateName);
	}
	
	public Map<String, Agent> getAgentMap() {
		if (agentMap == null) {
			agentMap = new HashMap<>();
			for (Agent agent : getAgents()) {
				agentMap.put(agent.getName(), agent);
			}
		}
		return agentMap;
	}

	public void setStateMap(Map<String, State> map) {
		this.stateMap = map;
	}
	
	public MultiKeyMap<String, List<List<AgentAction>>> getAgentActionsByStates() {
		if (agentActionsByStates == null) {
			agentActionsByStates = new MultiKeyMap<>();
			for (Transition transition : getTransitions()) {
				if (!agentActionsByStates.containsKey(transition.getFromState(), transition.getToState())) {
					agentActionsByStates.put(transition.getFromState(), transition.getToState(), new ArrayList<>());
				}
				agentActionsByStates.get(transition.getFromState(), transition.getToState()).addAll(transition.getAgentActions());
			}
		}
		
		return agentActionsByStates;
	}
	
	public Map<String, List<Transition>> getTransitionMap() {
		if (MapUtils.isEmpty(transitionMap)) {
			transitionMap = new HashMap<>();
			for (Transition transition : getTransitions()) {
				if (!transitionMap.containsKey(transition.getFromState())) {
					transitionMap.put(transition.getFromState(), new ArrayList<>());
				}
				transitionMap.get(transition.getFromState()).add(transition);
			}
		}
		
		return transitionMap;
	}
	
	public void updateModel(String atom) {
		for(State state : states) {
			if(state.isInitial()) {
				state.getLabels().add(atom);
			}
		}
	}

	public void removeState(State state) {
		if(states.remove(state)) {
			List<Transition> list = new ArrayList<>();
			if(!hasState("sink")){
				State sinkState = new State();
				sinkState.setName("sink");
				states.add(sinkState);
			}
			for (Transition t : transitions) {
				if (!t.getFromState().equals(state.getName())) {
					if(!t.getToState().equals(state.getName())) {
						list.add(t);
					} else {
						Transition trSink = new Transition();
						trSink.setFromState(t.getFromState());
						trSink.setToState("sink");
						trSink.setAgentActions(t.copyAgentActions());
						trSink.setMultipleAgentActions(t.copyMultiAgentActions());
						list.add(trSink);
					}
				}
			}
			transitions = list;
			transitionMap = null;
			stateMap = null;
			for(Agent agent : agents) {
				for(List<String> ind : agent.getIndistinguishableStates()) {
					ind.remove(state.getName());
				}
				agent.setIndistinguishableStates(agent.getIndistinguishableStates().stream().filter(ind -> ind.size() > 1).collect(Collectors.toList()));
			}
		}
	}

	@Override
	public AtlModel clone() {
		AtlModel clone;
		try {
			clone = (AtlModel) super.clone();
		}
		catch (CloneNotSupportedException ex) {
			throw new RuntimeException("Superclass messed up", ex);
		}

		List<State> statesAuxList = new ArrayList<>();
		for (State state : states) {
			State newState = new State(state.getName(), state.isInitial());
			newState.setLabels(new ArrayList<>(state.getLabels()));
			newState.setFalseLabels(new ArrayList<>(state.getFalseLabels()));
			statesAuxList.add(newState);
		}
		clone.states = statesAuxList;
		List<Agent> agentsAuxList = new ArrayList<>();
		for(Agent agent : agents) {
			Agent newAgent = new Agent();
			newAgent.setName(agent.getName());
			newAgent.setActions(new ArrayList<>(agent.getActions()));
			newAgent.setIndistinguishableStates(new ArrayList<>());
			for(List<String> indS : agent.getIndistinguishableStates()) {
				newAgent.getIndistinguishableStates().add(new ArrayList<>(indS));
			}
			agentsAuxList.add(newAgent);
		}
		clone.agents = agentsAuxList;
		List<Transition> transitionsAuxList = new ArrayList<>();
		for(Transition tr : transitions) {
			Transition newTransition = new Transition();
			newTransition.setFromState(tr.getFromState());
			newTransition.setToState(tr.getToState());
			newTransition.setAgentActions(new ArrayList<>());
			for(List<AgentAction> aal : tr.getAgentActions()) {
				List<AgentAction> aalAux = new ArrayList<>();
				for(AgentAction aa : aal) {
					AgentAction newAa = new AgentAction();
					newAa.setAgent(aa.getAgent());
					newAa.setAction(aa.getAction());
					aalAux.add(newAa);
				}
				newTransition.getAgentActions().add(aalAux);
			}
			List<MultipleAgentAction> maalAux = new ArrayList<>();
			for(MultipleAgentAction maa : tr.getMultipleAgentActions()) {
				MultipleAgentAction newMaa = new MultipleAgentAction();
				newMaa.setAgent(maa.getAgent());
				newMaa.setActions(new ArrayList<>(maa.getActions()));
				maalAux.add(newMaa);
			}
			newTransition.setMultipleAgentActions(maalAux);
			newTransition.setDefaultTransition(tr.isDefaultTransition());
			transitionsAuxList.add(newTransition);
		}
		clone.transitions = transitionsAuxList;
		clone.group = new Group();
		clone.group.setName(group.getName());
		clone.group.setAgents(new ArrayList<>(group.getAgents()));
		clone.formula = formula.clone();
		clone.agentMap = null;
		clone.stateMap = null;
		return clone;
	}

	public boolean isConnected() {
		State initialState = states.stream().filter(State::isInitial).findFirst().orElseGet(null);
		if(initialState == null) {
			return false;
		} else {
			return reachableStates(initialState.getName(), new HashSet<>()).size() == states.size();
		}
	}

	private Set<String> reachableStates(String state, Set<String> visitedStates) {
		visitedStates.add(state);
		List<String> nextStates = transitions
				.stream()
				.filter(t -> t.getFromState().equals(state) && !visitedStates.contains(t.getToState()))
				.map(Transition::getToState)
				.collect(Collectors.toList());
		if (!nextStates.isEmpty()) {
			for (String s : nextStates) {
				visitedStates.addAll(reachableStates(s, visitedStates));
			}
		}
		return visitedStates;
	}

	public void removeSubModel(AtlModel subModel, String atom) {
		State metaState = new State();
		metaState.setName("metastate");
		metaState.getLabels().add(atom);
		states.add(metaState);
		stateMap = null;
		for(State state : subModel.getStates()) {
			if(this.getState(state.getName()) != null && this.getState(state.getName()).isInitial()) {
				metaState.setInitial(true);
			}
			this.states.remove(state);
			if (state.isInitial()) {
				List<Transition> list = new ArrayList<>();
				for (Transition t : transitions) {
					if (!t.getFromState().equals(state.getName()) && t.getToState().equals(state.getName())) {
						Transition trMeta = new Transition();
						trMeta.setFromState(t.getFromState());
						trMeta.setToState("metastate");
						trMeta.setAgentActions(t.copyAgentActions());
						trMeta.setMultipleAgentActions(t.copyMultiAgentActions());
						list.add(trMeta);
					}
				}
				transitions.addAll(list);
			}
			transitions.removeIf(t -> t.getFromState().equals(state.getName()) || t.getToState().equals(state.getName()));
		}
	}

}
