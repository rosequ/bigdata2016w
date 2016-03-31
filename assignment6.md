####Question 1: 
For each individual classifiers trained on group_x, group_y, and britney, what are the 1-ROCA% scores? You should be able to replicate our results on group_x, group_y, but there may be some non-determinism for britney, which is why we want you to report the figures.

group_x: 17.25

group_y: 12.82

britney: 16.82 (linux) 14.61 (Altiscale)

####Question 2: 
What is the 1-ROCA% score of the score averaging technique in the 3-classifier ensemble?

average: 11.84 (linux) 11.81 (Altiscale)


####Question 3: 
What is the 1-ROCA% score of the voting technique in the 3-classifier ensemble?

voting: 14.78 (linux) 14.60 (Altiscale)


####Question 4: 
What is the 1-ROCA% score of a single classifier trained on all available training data concatenated together?

all: 16.29 (linux) 20.01 (Altiscale)

####Question 5: 
Run the shuffle trainer 10 times on the britney dataset, predict and evaluate the classifier on the test data each time. Report the 1-ROCA% score in each of the ten trials and compute the overall average.

(linux) each of the 10 times: 16.98 17.61 14.58 17.68 19.42 16.55 16.37 19.84. 19.44 17.02

overall average 17.54

(Altiscale) each of the 10 times: 13.83 16.05 16.00 17.02 17.20 15.29 17.14 17.99 14.98 16.18

overall average 16.17

Marks:
Compilation: 4/4
TrainSpamClassifier: 14/15
ApplySpamClassifier: 5/5
ApplyEnsembleClassifier: 6/6
Shuffle implementation: 5/5
Question Answers: 13.5/15
Runs on Altiscale: 10/10
Total: 58.5/60

- Q4 average is higher than expected (14% +- 3%) but your non-determinism wrt to linux and altiscale is within that range so you'll get some marks
- TrainSpamClassifier has lots of duplicated code due to shuffle that is not necessary
- The model does not need to be global for training and would potentially raise issues in a more complex application
