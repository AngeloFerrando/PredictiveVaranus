package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import javax.swing.*;

public class Transition extends JsonObject {

	public Transition() {
	}

	public Transition(String fromState, String toState, List<List<AgentAction>> agentActions) {
		this.fromState = fromState;
		this.toState = toState;
		this.agentActions = agentActions;
	}

	@SerializedName("fromState")
	@Expose
	private String fromState;
	
	@SerializedName("toState")
	@Expose
	private String toState;
	
	@SerializedName("agentActions")
	@Expose
	private List<List<AgentAction>> agentActions = new ArrayList<>();

	@SerializedName("multipleAgentActions")
	@Expose
	private List<MultipleAgentAction> multipleAgentActions = new ArrayList<>();

	@SerializedName("defaultTransition")
	@Expose
	private boolean defaultTransition;

	public String getFromState() {
		return fromState;
	}

	public void setFromState(String fromState) {
		this.fromState = fromState;
	}
	
	public String getToState() {
		return toState;
	}

	public void setToState(String toState) {
		this.toState = toState;
	}

	public List<List<AgentAction>> getAgentActions() {
		return agentActions;
	}

	public void setAgentActions(List<List<AgentAction>> agentActions) {
		for (List<AgentAction> agentActionList : agentActions) {
			Collections.sort(agentActionList);
		}
		
		this.agentActions = agentActions;
	}

	public boolean isDefaultTransition() {
		return defaultTransition;
	}

	public void setDefaultTransition(boolean defaultTransition) {
		this.defaultTransition = defaultTransition;
	}

	public List<MultipleAgentAction> getMultipleAgentActions() {
		return multipleAgentActions;
	}

	public void setMultipleAgentActions(List<MultipleAgentAction> multipleAgentActions) {
		this.multipleAgentActions = multipleAgentActions;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Transition)) {
			return false;
		}
		Transition tr = (Transition) obj;
		if(this.agentActions.size() != tr.agentActions.size()) {
			return false;
		}
		for(List<AgentAction> acts : this.agentActions) {
			boolean found = false;
			for(List<AgentAction> acts1 : tr.getAgentActions()) {
				if(new HashSet<>(acts).equals(new HashSet<>(acts1))) {
					found = true;
					break;
				}
			}
			if(!found){
				return false;
			}
		}
		if(this.multipleAgentActions.size() != tr.multipleAgentActions.size()) {
			return false;
		}
		for(MultipleAgentAction act : this.multipleAgentActions) {
			boolean found = false;
			for(MultipleAgentAction act1 : tr.multipleAgentActions) {
				if(act.getAgent().equals(act1.getAgent()) && new HashSet<>(act.getActions()).equals(new HashSet<>(act1.getActions()))) {
					found = true;
					break;
				}
			}
			if(!found){
				return false;
			}
		}
		return this.fromState.equals(tr.fromState) && this.toState.equals(tr.toState);
	}

	public List<List<AgentAction>> copyAgentActions() {
		List<List<AgentAction>> copy = new ArrayList<>();
		for(List<AgentAction> aal : agentActions) {
			List<AgentAction> aalAux = new ArrayList<>();
			for(AgentAction aa : aal) {
				AgentAction newAa = new AgentAction();
				newAa.setAgent(aa.getAgent());
				newAa.setAction(aa.getAction());
				aalAux.add(newAa);
			}
			copy.add(aalAux);
		}
		return copy;
	}

	public List<MultipleAgentAction> copyMultiAgentActions() {
		List<MultipleAgentAction> copy = new ArrayList<>();
		for(MultipleAgentAction maa : multipleAgentActions) {
			MultipleAgentAction newMaa = new MultipleAgentAction();
			newMaa.setAgent(maa.getAgent());
			newMaa.setActions(new ArrayList<>(maa.getActions()));
			copy.add(newMaa);
		}
		return copy;
	}
}
