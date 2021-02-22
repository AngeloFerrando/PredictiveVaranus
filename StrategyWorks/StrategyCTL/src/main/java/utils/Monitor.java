package utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

public class Monitor {
    private String[] alphabet;
    private HashMap<String, State> states = new HashMap<>();
    private State currentState;
    private Verdict currentVerdict = Verdict.Unknown;
    private String ltl;
    private String atl;
    public static String rv;

    private static class State {
        private String name;
        private HashMap<String, State> transitions = new HashMap<>();
        private Verdict output;
        private State(String name, Verdict output) {
            this.name = name;
            this.output = output;
        }
    }
    public static enum Verdict { True, False, Unknown};

    public String getLtl() {
        return ltl;
    }
    public String getAtl() {
        return atl;
    }
    public Verdict getCurrentVerdict() {
        return this.currentVerdict;
    }

    public Monitor(String ltl, String atl, String[] ltlAlphabet) throws IOException {
        this.ltl = "LTL=" + ltl.replace("and", "AND").replace("or", "OR").replace(" ", "");
        this.atl = atl;
        StringBuilder ltlAlphabetCommand = new StringBuilder();
        ltlAlphabetCommand.append(",ALPHABET=[");
        for(int i = 0; i < ltlAlphabet.length; i++) {
            ltlAlphabetCommand.append(ltlAlphabet[i].toLowerCase());
            if(i < ltlAlphabet.length-1) {
                ltlAlphabetCommand.append(",");
            }
        }
        ltlAlphabetCommand.append("]");
        String command = "java -jar " + rv + "/rltlconv.jar " + this.ltl + ltlAlphabetCommand + " --formula --nbas --min --nfas --dfas --min --moore";

        try(Scanner scanner = new Scanner(Runtime.getRuntime().exec(command).getInputStream()).useDelimiter("\n")) {
            while(scanner.hasNext()) {
                String mooreString = scanner.next();
                if(mooreString.contains("ALPHABET")) {
                    String[] alphabet = mooreString.split("=")[1].trim().replace("[", "").replace("]", "").split(",");
                    this.alphabet = new String[alphabet.length];
                    for(int i = 0; i < alphabet.length; i++) {
                        this.alphabet[i] = alphabet[i].replace("\"", "");
                    }
                } else if(mooreString.contains("STATES")) {
                    for(String state : mooreString.split("=")[1].split(",")) {
                        state = state.trim().replace("[", "").replace("]", "");
                        String name = state.split(":")[0];
                        String verdictStr = state.split(":")[1];
                        Verdict output = verdictStr.equals("true") ? Verdict.True : (verdictStr.equals("false") ? Verdict.False : Verdict.Unknown);
                        states.put(name, new State(name, output));
                    }
                } else if(mooreString.contains("START")) {
                    currentState = states.get(mooreString.split("=")[1].trim());
                } else if(mooreString.contains("DELTA")) {
                    String[] args = mooreString.substring(mooreString.indexOf("(")+1, mooreString.indexOf(")")).split(",");
                    states.get(args[0].trim()).transitions.put(args[1].trim().replace("\"", ""), states.get(mooreString.split("=")[1].trim()));
                }
            }
        }
    }

    public Verdict next(String event) {
        event = event.toLowerCase();
        if(currentState.transitions.containsKey(event)) {
            currentState = currentState.transitions.get(event);
            currentVerdict = currentState.output;
            return currentVerdict;
        } else if(currentState.transitions.containsKey("?")) {
            currentState = currentState.transitions.get("?");
            currentVerdict = currentState.output;
            return currentVerdict;
        }
        return currentVerdict;
        //throw new IllegalArgumentException("event does not belong to the alphabet");
    }
}