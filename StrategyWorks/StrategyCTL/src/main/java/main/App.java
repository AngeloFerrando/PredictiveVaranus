package main;

import com.google.common.base.Stopwatch;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import utils.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Main class
 */
public class App 
{
    public static void main( String[] args ) throws Exception {

        Options options = new Options();

        Option impRecall = new Option("r", "imperfect-recall", false, "find sub-models with imperfect recall");
        impRecall.setRequired(false);
        options.addOption(impRecall);
        Option perfectInfo = new Option("I", "perfect-information", false, "find sub-models with perfect information");
        perfectInfo.setRequired(false);
        options.addOption(perfectInfo);
        Option modelOpt = new Option("m", "model", true, "the ATL model");
        modelOpt.setRequired(true);
        options.addOption(modelOpt);
        Option outputFolder = new Option("o", "output", true, "folder where sub-models will be saved");
        outputFolder.setRequired(false);
        options.addOption(outputFolder);
        Option verbose = new Option("s", "silent", false, "disable prints");
        verbose.setRequired(false);
        options.addOption(verbose);
        Option mcmas = new Option("mcmas", "mcmas", true, "installation folder of mcmas");
        mcmas.setRequired(true);
        options.addOption(mcmas);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            File file = new File("./tmp");
            if(!file.exists() && !file.mkdir()) {
                throw new FileSystemException("./tmp folder could not be created");
            }
            cmd = parser.parse(options, args);
            AbstractionUtils.mcmas = cmd.getOptionValue("mcmas");
            Monitor.rv = cmd.getOptionValue("rv");
            System.out.println("Parsing the model..");
            // read json file
            String jsonModel = Files.readString(Paths.get(cmd.getOptionValue("model")), StandardCharsets.UTF_8);
            // load json file to ATL Model Java representation
            AtlModel atlModel = JsonObject.load(jsonModel, AtlModel.class);
            // validate the model
            AbstractionUtils.validateAtlModel(atlModel);
            // add default transitions to the model
            AbstractionUtils.processDefaultTransitions(atlModel);
            System.out.println("Model successfully parsed");
            boolean silent = cmd.hasOption("silent");

            // String s = AbstractionUtils.modelCheck_IR("./tmp/MyModel.ispl");

            if(cmd.hasOption("imperfect-recall") && cmd.hasOption("perfect-information")) {
                System.out.println("Start extracting sub-models with imperfect recall or perfect information..");
                Stopwatch stopwatch = Stopwatch.createStarted();
                Boolean res = isSatisfiedWithImperfectRecallOrPerfectInformation(atlModel, silent);
                System.out.println("Is satisfied with imperfect recall: " + (res == null ? "?" : (res ? "True" : "False")));
                stopwatch.stop();
                System.out.println("Time: " + stopwatch.elapsed().toMillis() + " [ms]");
            } else if(cmd.hasOption("imperfect-recall")) {
                System.out.println("Start extracting sub-models with imperfect recall..");
                Stopwatch stopwatch = Stopwatch.createStarted();
                Boolean res = isSatisfiedWithImperfectRecall(atlModel, silent);
                System.out.println("Is satisfied with imperfect recall: " + (res == null ? "?" : (res ? "True" : "False")));
                stopwatch.stop();
                System.out.println("Time: " + stopwatch.elapsed().toMillis() + " [ms]");
            } else if(cmd.hasOption("perfect-information")) {
                System.out.println("Start extracting sub-models with perfect information..");
                Stopwatch stopwatch = Stopwatch.createStarted();
                Boolean res = isSatisfiedWithPerfectInformation(atlModel, silent);
                System.out.println("Is satisfied with perfect information: " + (res == null ? "?" : (res ? "True" : "False")));
                stopwatch.stop();
                System.out.println("Time: " + stopwatch.elapsed().toMillis() + " [ms]");
            } else {
                System.out.println("You have to specify -r, or -I (or both)");
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Strategy RV", options);
            System.exit(1);
        }



        // String s = AbstractionUtils.modelCheck("./tmp/subModel.ispl");




//        String jsonSubModel = Files.readString(Paths.get("./tmp/ir/subModel0.json"), StandardCharsets.UTF_8);
//        AtlModel atlSubModel = JsonObject.load(jsonSubModel, AtlModel.class);
//        AbstractionUtils.validateAtlModel(atlSubModel);
//        AbstractionUtils.processDefaultTransitions(atlSubModel);
//
//        createMonitor(atlModel, atlSubModel);

//        List<Pair<AtlModel,Monitor>> subModelsIR = allSubICGSWithPerfectInformation(atlModel);
//        System.out.println("PerfectInformationSubModels: " + subModelsIR.size() + "\n\n");
//        FileUtils.cleanDirectory(new File("./tmp/IR/"));
//        int i = 0;
//        for(Pair<AtlModel,Monitor> m : subModelsIR) {
//            FileWriter writer = new FileWriter("./tmp/IR/subModel" + i++ + ".json");
//            writer.append(m.getLeft().toString()).append("\n\n");
//            writer.close();
//        }
//
//
//        Set<Monitor> monitors = new HashSet<>();
//        monitors.addAll(subModelsIR.stream().map(Pair::getRight).collect(Collectors.toList()));
////        monitors.addAll(subModelsir.stream().map(Pair::getRight).collect(Collectors.toList()));
//        List<String> trace = new ArrayList<>();
//        trace.add("s0");
//        trace.add("s2");
//        trace.add("o");
//        trace.add("s0");
//        execRV(monitors, trace);
    }

    public static Boolean isSatisfiedWithImperfectRecallOrPerfectInformation(AtlModel model, boolean silent) throws Exception {
        System.out.println("Generating sub-models..");
        return AbstractionUtils.verify(model, AbstractionUtils.validateSubModels(
                StreamSupport.stream(AbstractionUtils.allModels(model).spliterator(), false).collect(Collectors.toList()),
                model.getFormula(),
                AbstractionUtils.Verification.Both), silent);
        //return AbstractionUtils.validateSubModels(model, AbstractionUtils.allModels(model), true, silent);
    }

    public static Boolean isSatisfiedWithImperfectRecall(AtlModel model, boolean silent) throws Exception {
        System.out.println("Generating sub-models..");
        return AbstractionUtils.verify(model, AbstractionUtils.validateSubModels(
                StreamSupport.stream(AbstractionUtils.allModels(model).spliterator(), false).collect(Collectors.toList()),
                model.getFormula(),
                AbstractionUtils.Verification.ImperfectRecall), silent);
        //return AbstractionUtils.validateSubModels(model, AbstractionUtils.allModels(model), true, silent);
    }

    public static Boolean isSatisfiedWithPerfectInformation(AtlModel model, boolean silent) throws Exception {
        System.out.println("Generating sub-models..");
        List<AtlModel> candidates = new LinkedList<>();
        candidates.add(model);
        List<AtlModel> candidatesPP = new LinkedList<>();
        while(!candidates.isEmpty()) {
            AtlModel candidate = candidates.remove(0);
            boolean valid = true;
            for(Agent agent : candidate.getAgents()){
                if(!agent.getIndistinguishableStates().isEmpty()) {
                    for(List<String> indistinguishableStates : agent.getIndistinguishableStates()) {
                        for (String ind : indistinguishableStates) {
                            AtlModel aux = candidate.clone();
                            State s = new State();
                            s.setName(ind);
                            aux.removeState(s);
                            candidates.add(aux);
                        }
                    }
                    valid = false;
                    break;
                }
            }
            if(valid) {
                if(candidatesPP.stream().noneMatch((m) -> new HashSet<>(m.getStates()).equals(new HashSet<>(candidate.getStates())))) {
                    candidatesPP.add(candidate);
                }
            }
        }
        System.out.println("Sub-models generated: " + candidatesPP.size());
        return AbstractionUtils.verify(model, AbstractionUtils.validateSubModels(candidatesPP, model.getFormula(), AbstractionUtils.Verification.PerfectInformation), silent);
        //return AbstractionUtils.validateSubModels(model, candidatesPP, false, silent);
    }

    public static void execRV(Set<Pair<Monitor, Monitor>> monitors, Collection<String> trace) throws IOException {
        for(String event : trace) {
            System.out.println("Analyse event: " + event);
            Set<Pair<Monitor,Monitor>> monitorsAux = new HashSet<>();
            Set<Pair<String, String>> satisfiedFormulas = new HashSet<>();
            for(Pair<Monitor,Monitor> monitor : monitors) {
                Monitor.Verdict output;
                if(monitor.getLeft().getCurrentVerdict() == Monitor.Verdict.True) {
                    output = monitor.getRight().next(event);
                    if(output == Monitor.Verdict.True) {
                        satisfiedFormulas.add(new ImmutablePair<>(monitor.getLeft().getLtl(), monitor.getLeft().getAtl()));
                    } else if(output == Monitor.Verdict.Unknown) {
                        monitorsAux.add(monitor);
                    }
                } else {
                    output = monitor.getLeft().next(event);
                    if(output != Monitor.Verdict.False) {
                        monitorsAux.add(monitor);
                    }
                }
            }
            monitors = monitorsAux;
            for(Pair<String, String> p : satisfiedFormulas) {
                System.out.println("- Monitor concluded satisfaction of LTL property: " + p.getLeft());
                System.out.println("and reached a sub-model where the ATL property: " + p.getRight() + " is satisfied.");
            }
            if(!satisfiedFormulas.isEmpty()) {
                System.out.println("Do you want to continue monitoring the system? [y/n]");
                Scanner scanner = new Scanner(System.in);
                String choice = scanner.next();
                if(choice.equals("n")) {
                    return;
                }
            }
        }
    }
}
