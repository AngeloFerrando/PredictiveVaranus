# Decidability of ATL Model Checking under Imperfect Information and Perfect Recall via Runtime Verification


## What you need to install

- MCMAS: https://vas.doc.ic.ac.uk/software/mcmas/
- LamaConv: https://www.isp.uni-luebeck.de/lamaconv

In the following, <path-to-mcmas> is the path to MCMAS main folder (where mcmas executable is),
and <path-to-lamaconv> is the path to LamaConv Java library (Jar).

## How to generate sub-models with perfect information and perfect recall

To generate sub-models for the model obtained combining Figure 1 and Figure 2.
```shell script
-$ java -jar StrategyRV-1.0-SNAPSHOT-jar-with-dependencies.jar -mcmas <path-to-mcmas> -rv <path-to-lamaconv> -m ./roverEx1.json -I
```
This will generate 6 sub-models inside folder ./tmp/IR/.

To generate sub-models for the model obtained combining Figure 1 and Figure 3.
```shell script
-$ java -jar StrategyRV-1.0-SNAPSHOT-jar-with-dependencies.jar -mcmas <path-to-mcmas> -rv <path-to-lamaconv> -m ./roverEx1.json -I
```
This will generate 0 sub-models inside folder ./tmp/IR/.

[To change folder where the sub-models are going to be saved, it is enough to also pass parameter -o <path-to-folder>]

## How to generate sub-models with imperfect information and imperfect recall

To generate sub-models for the model obtained combining Figure 1 and Figure 2.
```shell script
-$ java -jar StrategyRV-1.0-SNAPSHOT-jar-with-dependencies.jar -mcmas <path-to-mcmas> -rv <path-to-lamaconv> -m ./roverEx1.json -r
```
This will generate 452 sub-models inside folder ./tmp/ir/.

To generate sub-models for the model obtained combining Figure 1 and Figure 3.
```shell script
-$ java -jar StrategyRV-1.0-SNAPSHOT-jar-with-dependencies.jar -mcmas <path-to-mcmas> -rv <path-to-lamaconv> -m ./roverEx1.json -r
```
This will generate 651 sub-models inside folder ./tmp/ir/.

## How to generate and run monitors

Monitors for Figure 1 and Figure 2
```shell script
-$ java -jar StrategyRV-1.0-SNAPSHOT-jar-with-dependencies.jar -mcmas <path-to-mcmas> -rv <path-to-lamaconv> -m ./roverEx1.json -sb <path-to-folder> -t s01,s02,s03,o,s0
```
Where <path-to-folder> is where the sub-models generated at the previous step have been saved.

If this was left unchanged, the sub-models with imperfect information and imperfect recall are saved into ./tmp/ir, while the sub-models
with perfect information and perfect recall are saved into ./tmp/IR.

-t contains the trace the monitors have to analyse (each state separated by a comma).

The model passed for generating the monitors has to be same used to generate the sub-models. Thus, the proper flow of actions is to create
the sub-models using a model, and then generate the monitors using the same model.

Monitors for Figure 1 and Figure 3
```shell script
-$ java -jar StrategyRV-1.0-SNAPSHOT-jar-with-dependencies.jar -mcmas <path-to-mcmas> -rv <path-to-lamaconv> -m ./roverEx2.json -sb <path-to-folder> -t s01,s02,s03,o,s2 
```

[In the current version, the monitors generation and their execution is not separated. In the next version we will decouple
the generation from the execution for having an improved performance for RV.]
